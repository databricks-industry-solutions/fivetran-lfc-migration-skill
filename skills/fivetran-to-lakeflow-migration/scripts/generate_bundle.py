#!/usr/bin/env python3
"""Stage 5 - generate a Databricks Asset Bundle from the migration plan.

    python3 generate_bundle.py -p out/plan.json -o out/bundle --name acme-lfc

Emits reviewable YAML the customer keeps, plus a pre-deploy script for the Unity
Catalog connections, which bundles cannot express.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import stat
import sys
from collections.abc import Callable
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ftlfc.bundle import (
    DEV_SCHEMA_SUFFIX,
    build_bundle,
    dev_catalog_for,
    is_pipeline_name_for,
    migratable_keys,
    rename_pipeline_keys,
)
from ftlfc.workspace import (
    WorkspaceError,
    cli_catalog_lookup,
    cli_current_user,
    cli_pipeline_lookup,
    cli_pipeline_search,
    cli_table_list,
    cli_workspace_path_lookup,
)


def existing_bundle_files(output: Path) -> list[str]:
    """The databricks.yml and resource files already in ``output``, relative."""
    if not output.is_dir():
        return []
    found = [output / "databricks.yml"] if (output / "databricks.yml").is_file() else []
    resources = output / "resources"
    if resources.is_dir():
        found += sorted(p for p in resources.rglob("*") if p.suffix in (".yml", ".yaml"))
    return [p.relative_to(output).as_posix() for p in found]


def find_name_clashes(
    keys: list[str],
    bundle_name: str,
    search: Callable[[str], list[dict]],
    get: Callable[[str], dict | None],
) -> dict[str, str]:
    """Map each key whose pipeline name another bundle or person already uses to a
    suffixed key nobody uses. A pipeline deployed from a bundle of this name is
    this bundle's own and not a clash."""

    def clashes(key: str) -> bool:
        return any(
            is_pipeline_name_for(pipeline.get("name", ""), key)
            and _owner_bundle(get(pipeline["pipeline_id"])) != bundle_name
            for pipeline in search(f"lfc-{key}-")
        )

    taken = set(keys)
    renames: dict[str, str] = {}
    for key in keys:
        candidate, n = key, 1
        while clashes(candidate):
            n += 1
            candidate = f"{key}_{n}"
            while candidate in taken:
                n += 1
                candidate = f"{key}_{n}"
        if candidate != key:
            taken.add(candidate)
            renames[key] = candidate
    return renames


def _owner_bundle(pipeline: dict | None) -> str | None:
    """The bundle a pipeline was deployed from, from its deployment metadata path."""
    spec = (pipeline or {}).get("spec") or {}
    path = (spec.get("deployment") or {}).get("metadata_file_path", "")
    match = re.search(r"/\.bundle/([^/]+)/", path)
    return match.group(1) if match else None


def find_table_conflicts(
    plan: dict,
    dev_catalog: str,
    bundle_name: str,
    list_tables: Callable[[str, str], list[dict]],
    get: Callable[[str], dict | None],
) -> list[dict[str, str]]:
    """Destination tables that already exist, per target, and do not belong to a
    pipeline deployed from this bundle."""
    targets = [
        ("dev", dev_catalog, DEV_SCHEMA_SUFFIX),
        ("prod", plan["target_catalog"], ""),
    ]
    listed: dict[tuple[str, str], dict[str, dict]] = {}
    conflicts: list[dict[str, str]] = []
    for item in plan["items"]:
        if item["blockers"]:
            continue
        for target, catalog, suffix in targets:
            for obj in item["objects"]:
                schema = f"{obj['destination_schema']}{suffix}"
                if (catalog, schema) not in listed:
                    listed[(catalog, schema)] = {t["name"]: t for t in list_tables(catalog, schema)}
                table = listed[(catalog, schema)].get(obj["destination_table"])
                if table is None:
                    continue
                owner_id = table.get("pipeline_id")
                pipeline = get(owner_id) if owner_id else None
                bundle = _owner_bundle(pipeline)
                if bundle == bundle_name:
                    continue
                if owner_id:
                    name = (pipeline or {}).get("name", "name unknown")
                    owner = f"pipeline `{owner_id}` ({name})"
                    owner += f", bundle `{bundle}`" if bundle else ", not from a bundle"
                else:
                    owner = f"{table.get('table_type', 'table')} not owned by a pipeline"
                full_name = f"{catalog}.{schema}.{obj['destination_table']}"
                conflicts.append({"target": target, "table": full_name, "owner": owner})
    return conflicts


def check_tables(plan: dict, dev_catalog: str, bundle_name: str, profile: str) -> list[dict]:
    """Warn about destination tables something else owns; never fails."""
    try:
        conflicts = find_table_conflicts(
            plan, dev_catalog, bundle_name, cli_table_list(profile), cli_pipeline_lookup(profile)
        )
    except WorkspaceError as exc:
        print(f"  warning: could not check destination tables: {exc}", file=sys.stderr)
        return []
    for c in conflicts:
        print(
            f"  warning: {c['table']} ({c['target']}) already exists, owned by {c['owner']}; "
            "see 'Table conflicts' in README.md",
            file=sys.stderr,
        )
    return conflicts


def check_bundle_state(bundle_name: str, profile: str) -> None:
    """Warn when the profile's user already deployed a bundle with this name:
    bundles with the same name share deployment state, so a deploy replaces that
    bundle's resources with this one's."""
    try:
        user = cli_current_user(profile)
        path = f"/Workspace/Users/{user}/.bundle/{bundle_name}"
        exists = cli_workspace_path_lookup(profile)(path) is not None
    except WorkspaceError as exc:
        print(f"  warning: could not check for an existing bundle state: {exc}", file=sys.stderr)
        return
    if exists:
        print(
            f"  warning: {path} exists, so a bundle named '{bundle_name}' was already "
            "deployed. Deploying this one replaces that bundle's resources; if it is a "
            "different bundle, pass a different --name.",
            file=sys.stderr,
        )


def check_pipeline_names(plan: dict, bundle_name: str, profile: str) -> dict[str, str]:
    """Suffix keys whose pipeline names are already taken; warns and renames
    nothing when the workspace cannot be queried."""
    try:
        renames = find_name_clashes(
            migratable_keys(plan),
            bundle_name,
            cli_pipeline_search(profile),
            cli_pipeline_lookup(profile),
        )
    except WorkspaceError as exc:
        print(f"  warning: could not check pipeline names: {exc}", file=sys.stderr)
        return {}
    for old, new in renames.items():
        print(
            f"  note: a pipeline named like 'lfc-{old}-<target>' already exists outside this "
            f"bundle; using key '{new}' instead",
            file=sys.stderr,
        )
    return renames


def check_catalogs(
    names: list[str], lookup: Callable[[str], dict | None]
) -> dict[str, bool | None]:
    """Report which catalogs exist. Warns rather than fails: the operator may be
    about to create them, and nothing is deployed from here."""
    found: dict[str, bool | None] = {}
    for name in dict.fromkeys(names):
        try:
            found[name] = lookup(name) is not None
        except WorkspaceError as exc:
            found[name] = None
            print(f"  warning: could not check catalog '{name}': {exc}", file=sys.stderr)
            continue
        if not found[name]:
            print(
                f"  warning: catalog '{name}' does not exist in the workspace; create it "
                "before deploying, or pass --dev-catalog for the dev target",
                file=sys.stderr,
            )
    return found


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-p", "--plan", type=Path, default=Path("out/plan.json"))
    parser.add_argument("-o", "--output", type=Path, default=Path("out/bundle"))
    parser.add_argument("--name", default="lakeflow-ingestion", help="bundle name")
    parser.add_argument("--host", help="Databricks workspace URL for both targets")
    parser.add_argument("--email", help="address for pipeline and job failure notifications")
    parser.add_argument(
        "--dev-catalog",
        help="catalog the dev target writes to (default: <plan target catalog>_dev)",
    )
    parser.add_argument(
        "--databricks-profile",
        help="CLI profile of the target workspace, to confirm (read-only) that the dev and "
        "prod catalogs exist; warns if they do not",
    )
    parser.add_argument(
        "--force", action="store_true", help="overwrite an existing output directory"
    )
    parser.add_argument(
        "--keep-existing-resources",
        action="store_true",
        help="with --force, keep resource files already in the output directory that this "
        "plan does not generate; they deploy with the bundle",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
        stream=sys.stderr,
    )

    if not args.plan.exists():
        print(f"error: {args.plan} not found. Run plan_migration.py first.", file=sys.stderr)
        return 1

    plan = json.loads(args.plan.read_text())
    existing = existing_bundle_files(args.output)
    if args.output.exists() and any(args.output.iterdir()) and not args.force:
        print(f"error: {args.output} is not empty.", file=sys.stderr)
        if existing:
            print(f"  It already holds a bundle: {', '.join(existing)}", file=sys.stderr)
        print(
            "  Write to a new -o folder, or pass --force to overwrite this one.",
            file=sys.stderr,
        )
        return 1

    dev_catalog = dev_catalog_for(plan, args.dev_catalog)
    catalog_exists: dict[str, bool | None] = {}
    renamed_keys: dict[str, str] = {}
    table_conflicts: list[dict[str, str]] = []
    if args.databricks_profile:
        catalog_exists = check_catalogs(
            [dev_catalog, plan["target_catalog"]], cli_catalog_lookup(args.databricks_profile)
        )
        renamed_keys = check_pipeline_names(plan, args.name, args.databricks_profile)
        plan = rename_pipeline_keys(plan, renamed_keys)
        check_bundle_state(args.name, args.databricks_profile)
        table_conflicts = check_tables(plan, dev_catalog, args.name, args.databricks_profile)

    def build(kept: list[str]) -> dict[str, str]:
        return build_bundle(
            plan,
            bundle_name=args.name,
            host=args.host,
            notification_email=args.email,
            dev_catalog=dev_catalog,
            catalog_exists=catalog_exists,
            renamed_keys=renamed_keys,
            kept_resources=kept,
            table_conflicts=table_conflicts,
        )

    files = build([])
    leftover = [p for p in existing if p != "databricks.yml" and p not in files]
    if leftover and not args.keep_existing_resources:
        print(
            f"error: {args.output} holds resource files this plan does not generate, and "
            f"`include: resources/*.yml` would deploy them: {', '.join(leftover)}",
            file=sys.stderr,
        )
        print(
            "  Write to a new -o folder (and --name), or pass --keep-existing-resources "
            "to deploy them with this bundle.",
            file=sys.stderr,
        )
        return 1
    if leftover:
        files = build(leftover)

    for relative, contents in sorted(files.items()):
        path = args.output / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents)
        if path.suffix == ".sh":
            path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP)

    _report(plan, args.output, files)
    return 0


def _report(plan: dict, output: Path, files: dict) -> None:
    summary = plan["summary"]
    gateways = sum(1 for f in files if f.endswith("_gateway.pipeline.yml"))
    pipelines = sum(1 for f in files if f.endswith(".pipeline.yml")) - gateways

    print(f"Wrote {len(files)} files to {output}")
    print(f"  {pipelines} ingestion pipeline(s) covering {summary['tables_total']} table(s)")
    print(f"  {gateways} ingestion gateway(s)")
    print(f"  {sum(1 for f in files if f.endswith('.job.yml'))} companion job(s)")
    print()
    print("  Next:")
    step = 1
    if "databricks connections create" in files.get("scripts/create_connections.sh", ""):
        print("    1. Fill in the REPLACE_ME values in scripts/create_connections.sh")
        print(f"    2. cd {output} && ./scripts/create_connections.sh <profile>")
        step = 3
    manual = summary.get("connections_manual_sign_in", 0)
    if manual:
        print(
            f"    {step}. Create the {manual} browser-OAuth connection(s) listed under "
            "'Manual connections' in README.md"
        )
        step += 1
    print(f"    {step}. databricks bundle validate --strict -t dev")
    print(f"    {step + 1}. databricks bundle deploy -t dev")

    not_migratable = summary["connections_not_migratable"]
    if not_migratable:
        print(
            f"\n  {not_migratable} connection(s) are absent from the bundle; see "
            f"'Not included' in {output}/README.md for why and for other paths.",
            file=sys.stderr,
        )


if __name__ == "__main__":
    raise SystemExit(main())
