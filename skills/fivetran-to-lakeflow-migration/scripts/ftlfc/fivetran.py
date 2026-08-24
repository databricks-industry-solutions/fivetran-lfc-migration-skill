"""Fivetran REST API client.

Read-only by design: every method issues GET requests. Nothing in this module can
mutate a customer's Fivetran account. In particular it never calls
``POST /schemas/reload``, which despite being a discovery-shaped endpoint can
change which tables are selected.

Auth uses HTTP Basic with the API key as username and API secret as password,
read from FIVETRAN_API_KEY / FIVETRAN_API_SECRET.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import random
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger(__name__)

BASE_URL = "https://api.fivetran.com"

# Fivetran versions its API per-endpoint through the Accept header, and returns
# 406 rather than falling back when the value is wrong. /connections and
# /destinations/{id} default to v2; everything else rejects it.
ACCEPT_V1 = "application/json"
ACCEPT_V2 = "application/json;version=2"

_V2_PATHS = (
    re.compile(r"^/v1/connections/?$"),
    re.compile(r"^/v1/connectors/?$"),
    re.compile(r"^/v1/destinations/[^/]+/?$"),
    re.compile(r"^/v1/groups/[^/]+/conn(ections|ectors)/?$"),
)

PAGE_LIMIT = 1000
MAX_ATTEMPTS = 5
RETRY_STATUS = frozenset({429, 500, 502, 503, 504})

# Fivetran's documented Retry-After on a 429 can be a full hour. Waiting that
# long inside a discovery run is worse than failing with a clear message.
MAX_RETRY_AFTER_SECONDS = 120

# Throttle pre-emptively once the remaining hourly budget gets this low, rather
# than driving into a 429 and being locked out for an hour.
RATE_LIMIT_FLOOR = 50

# Config keys whose values must never be written to an inventory file. Fivetran
# already masks most secrets server-side, but connectors differ in what they
# return and inventory files get shared over Slack and attached to tickets.
SECRET_KEY_HINTS = (
    "password",
    "secret",
    "token",
    "key",
    "credential",
    "passphrase",
    "certificate",
    "private",
    "auth",
    "sasl",
    "pem",
)

# Keys that contain one of the SECRET_KEY_HINTS substrings but are structural
# rather than sensitive, and are needed to understand the source setup.
SECRET_KEY_ALLOWLIST = frozenset(
    {
        "public_key",
        "is_keypair",
        "auth_mode",
        "auth_type",
        "authorization_method",
        "is_secure",
        "primary_keys",
        "replication_key",
        "partition_key",
        "sort_key",
        "key_columns",
    }
)


class FivetranError(RuntimeError):
    """A Fivetran API call failed in a way that is not worth retrying."""


class FivetranAuthError(FivetranError):
    """Credentials are missing, malformed, or rejected."""


class FivetranRateLimitError(FivetranError):
    """Rate limited with a Retry-After longer than this run is willing to wait."""


def _accept_for(path: str) -> str:
    return ACCEPT_V2 if any(p.match(path) for p in _V2_PATHS) else ACCEPT_V1


@dataclass
class FivetranClient:
    api_key: str
    api_secret: str
    base_url: str = BASE_URL
    timeout: float = 60.0
    max_attempts: int = MAX_ATTEMPTS
    #: Human-readable strings for calls that failed but were non-fatal, so
    #: discovery reports partial results instead of aborting.
    warnings: list[str] = field(default_factory=list)
    #: Last observed value of the X-Rate-Limit-Remaining header, or None.
    rate_limit_remaining: int | None = None

    @classmethod
    def from_env(cls, **kwargs: Any) -> FivetranClient:
        key = os.environ.get("FIVETRAN_API_KEY", "").strip()
        secret = os.environ.get("FIVETRAN_API_SECRET", "").strip()
        if not key or not secret:
            raise FivetranAuthError(
                "Set FIVETRAN_API_KEY and FIVETRAN_API_SECRET. Create a key in the "
                "Fivetran dashboard under Account Settings > API Config."
            )
        return cls(api_key=key, api_secret=secret, **kwargs)

    @property
    def _auth_header(self) -> str:
        raw = f"{self.api_key}:{self.api_secret}".encode()
        return "Basic " + base64.b64encode(raw).decode()

    def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """GET a Fivetran path and return the unwrapped ``data`` object."""
        url = f"{self.base_url}{path}"
        if params:
            clean = {k: v for k, v in params.items() if v is not None}
            if clean:
                url = f"{url}?{urllib.parse.urlencode(clean)}"

        body = self._request_with_retry(url, _accept_for(path))
        # Fivetran wraps successful payloads as {"code": "Success", "data": {...}}.
        # A few endpoints return the object directly; tolerate both.
        if isinstance(body, dict) and "data" in body:
            data = body["data"]
            return data if isinstance(data, dict) else {"items": data}
        return body if isinstance(body, dict) else {"items": body}

    def _request_with_retry(self, url: str, accept: str) -> Any:
        request = urllib.request.Request(
            url,
            method="GET",
            headers={
                "Authorization": self._auth_header,
                "Accept": accept,
                "User-Agent": "fivetran-lfc-migration-skill/0.1",
            },
        )

        last_error: Exception | None = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    self._note_rate_limit(response.headers)
                    return json.loads(response.read().decode("utf-8") or "{}")
            except urllib.error.HTTPError as exc:
                self._note_rate_limit(exc.headers)
                detail = _read_error_body(exc)
                if exc.code in (401, 403):
                    raise FivetranAuthError(
                        f"Fivetran rejected the credentials ({exc.code}) for {url}: {detail}"
                    ) from exc
                if exc.code == 406:
                    raise FivetranError(
                        f"GET {url} rejected the Accept header {accept!r}. Fivetran versions "
                        f"its API per-endpoint; see ftlfc.fivetran._V2_PATHS. Detail: {detail}"
                    ) from exc
                if exc.code == 429:
                    self._sleep_for_retry_after(exc.headers, url)
                    last_error = exc
                    continue
                if exc.code not in RETRY_STATUS or attempt == self.max_attempts:
                    raise FivetranError(f"GET {url} failed with HTTP {exc.code}: {detail}") from exc
                last_error = exc
            except urllib.error.URLError as exc:
                if attempt == self.max_attempts:
                    raise FivetranError(f"GET {url} failed: {exc.reason}") from exc
                last_error = exc

            backoff = min(2 ** (attempt - 1), 16) + random.random()
            log.warning("Retrying %s in %.1fs (%s)", url, backoff, last_error)
            time.sleep(backoff)

        raise FivetranError(f"GET {url} exhausted {self.max_attempts} attempts")

    def _note_rate_limit(self, headers: Any) -> None:
        raw = headers.get("X-Rate-Limit-Remaining") if headers else None
        if raw is None:
            return
        try:
            self.rate_limit_remaining = int(raw)
        except (TypeError, ValueError):
            return
        if self.rate_limit_remaining < RATE_LIMIT_FLOOR:
            log.warning(
                "Fivetran rate limit nearly exhausted (%s remaining this hour)",
                self.rate_limit_remaining,
            )

    def _sleep_for_retry_after(self, headers: Any, url: str) -> None:
        """Honour Retry-After on a 429; Fivetran documents values up to an hour."""
        raw = headers.get("Retry-After") if headers else None
        try:
            wait = int(raw) if raw is not None else 30
        except (TypeError, ValueError):
            wait = 30
        if wait > MAX_RETRY_AFTER_SECONDS:
            raise FivetranRateLimitError(
                f"Rate limited on {url} with Retry-After={wait}s, beyond the "
                f"{MAX_RETRY_AFTER_SECONDS}s this run will wait. Re-run later, narrow the "
                "scope with --group, or pass --no-columns to skip the per-table column crawl."
            )
        log.warning("Rate limited on %s; sleeping %ss per Retry-After", url, wait)
        time.sleep(wait)

    def paginate(self, path: str, params: dict[str, Any] | None = None) -> Iterator[dict[str, Any]]:
        """Yield every item across a cursor-paginated collection endpoint.

        Fivetran signals the last page by *omitting* next_cursor rather than
        setting it null, so termination keys off absence.
        """
        cursor: str | None = None
        seen_cursors: set[str] = set()
        while True:
            page_params = dict(params or {})
            page_params["limit"] = PAGE_LIMIT
            if cursor:
                page_params["cursor"] = cursor
            data = self.get(path, page_params)
            yield from data.get("items", []) or []
            cursor = data.get("next_cursor")
            if not cursor or cursor in seen_cursors:
                return
            seen_cursors.add(cursor)

    # -- Collection helpers -------------------------------------------------

    def get_account_info(self) -> dict[str, Any]:
        """Validate credentials and identify the key type.

        A scoped key only sees what its owning user can see, so an estate
        discovered with one may be silently incomplete.
        """
        return self.get("/v1/account/info")

    def list_groups(self) -> list[dict[str, Any]]:
        return list(self.paginate("/v1/groups"))

    def get_destination(self, group_id: str) -> dict[str, Any]:
        """Fetch the destination for a group.

        The destination id equals the group id, and a group may legitimately have
        no destination configured yet, which surfaces as a 404.
        """
        try:
            return self.get(f"/v1/destinations/{group_id}")
        except FivetranError as exc:
            self.warnings.append(f"destination {group_id}: {exc}")
            return {}

    def list_connections(self, group_id: str | None = None) -> list[dict[str, Any]]:
        """List connections, preferring the current ``/connections`` collection.

        ``/v1/connectors`` remains a live alias with no announced sunset, so fall
        back to it transparently for older accounts.
        """
        primary = "/v1/connections"
        fallback = f"/v1/groups/{group_id}/connectors" if group_id else "/v1/connectors"
        params = {"group_id": group_id} if group_id else None
        try:
            return list(self.paginate(primary, params))
        except FivetranAuthError:
            raise
        except FivetranError as exc:
            log.warning("%s unavailable (%s); falling back to %s", primary, exc, fallback)
            self.warnings.append(f"{primary} unavailable, used {fallback}: {exc}")
            return list(self.paginate(fallback))

    def get_connection_schemas(self, connection_id: str) -> dict[str, Any]:
        """Fetch the schema/table selection tree for a connection.

        Returns an empty dict when the connector does not expose a schema config
        (event and Magic Folder style connectors) or has not completed setup.
        """
        last: Exception | None = None
        for path in (
            f"/v1/connections/{connection_id}/schemas",
            f"/v1/connectors/{connection_id}/schemas",
        ):
            try:
                return self.get(path)
            except FivetranAuthError:
                raise
            except FivetranError as exc:
                last = exc
        self.warnings.append(f"schemas for {connection_id} unavailable: {last}")
        return {}

    def get_table_columns(self, connection_id: str, schema: str, table: str) -> dict[str, Any]:
        """Fetch the exhaustive column list for one table.

        The schemas endpoint returns only columns that were explicitly
        overridden, so this is the only way to see the true column set. It
        queries the source live, counts against the tight source-interaction
        rate limit, and requires the connection to be Connected. Path segments
        are case-sensitive.
        """
        quoted_schema = urllib.parse.quote(schema, safe="")
        quoted_table = urllib.parse.quote(table, safe="")
        path = (
            f"/v1/connections/{connection_id}/schemas/{quoted_schema}/tables/{quoted_table}/columns"
        )
        try:
            return self.get(path)
        except FivetranRateLimitError:
            raise
        except FivetranAuthError:
            raise
        except FivetranError as exc:
            self.warnings.append(f"columns for {connection_id}.{schema}.{table}: {exc}")
            return {}

    def list_connector_types(self) -> list[dict[str, Any]]:
        """List Fivetran's connector-type catalog.

        ``/v1/metadata/connectors`` is a live legacy alias for this path and,
        contrary to Fivetran's docs, also requires authentication.
        """
        try:
            return list(self.paginate("/v1/metadata/connector-types"))
        except FivetranError as exc:
            self.warnings.append(f"connector metadata unavailable: {exc}")
            return []

    def list_transformations(self) -> list[dict[str, Any]]:
        """List dbt Core and Quickstart transformations.

        ``schedule.connection_ids[]`` gives the dependency edge back to the
        connections feeding each transformation, which orders the cutover.
        """
        try:
            return list(self.paginate("/v1/transformations"))
        except FivetranError as exc:
            self.warnings.append(f"transformations unavailable: {exc}")
            return []


def _read_error_body(exc: urllib.error.HTTPError) -> str:
    try:
        payload = exc.read().decode("utf-8")
    except Exception:
        return exc.reason or ""
    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError:
        return payload[:500]
    return str(parsed.get("message") or parsed.get("code") or parsed)[:500]


def redact(value: Any, _key: str = "") -> Any:
    """Recursively strip secret-looking values from a connector config."""
    if isinstance(value, dict):
        return {k: redact(v, k) for k, v in value.items()}
    if isinstance(value, list):
        return [redact(v, _key) for v in value]

    key = _key.lower()
    is_secret = (
        key and key not in SECRET_KEY_ALLOWLIST and any(hint in key for hint in SECRET_KEY_HINTS)
    )
    if is_secret:
        return None if value is None else "***REDACTED***"
    return value
