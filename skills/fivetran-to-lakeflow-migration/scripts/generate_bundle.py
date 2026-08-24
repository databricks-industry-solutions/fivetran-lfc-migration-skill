#!/usr/bin/env python3
"""Stage 5 - generate a Databricks Asset Bundle from a migration plan.

    python3 generate_bundle.py --plan out/plan.json --output out/bundle \
        --bundle-name acme_fivetran_migration --host https://acme.cloud.databricks.com

Writes nothing to Databricks. Review the bundle, then deploy it with stage 6.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ftlfc.bundle import write_bundle  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-p", "--plan", required=True, help="migration plan JSON from stage 3")
    parser.add_argument("-o", "--output", default="out/bundle", help="bundle directory to create")
    parser.add_argument(
        "--bundle-name", default="fivetran_lfc_migration", help="Asset Bundle name"
    )
    parser.add_argument("--host", default=None, help="Databricks workspace URL for the targets")
    parser.add_argument(
        "--catalog",
        default=None,
        help="override the catalog variable default (defaults to the plan's target catalog)",
    )
    args = parser.parse_args(argv)

    plan = json.loads(Path(args.plan).read_text())

    result = write_bundle(
        plan,
        output_dir=Path(args.output),
        bundle_name=args.bundle_name,
        host=args.host,
        default_catalog=args.catalog,
    )

    print(f"Wrote bundle to {result['output_dir']}")
    for path in result["files"]:
        print(f"  {path}")
    print(
        f"  {result['connections_emitted']} connection(s) emitted as "
        f"{result['pipelines']} pipeline(s); {result['connections_skipped']} skipped"
    )
    print()
    print("Next:")
    print(f"  1. Read {Path(result['output_dir']) / 'MIGRATION_NOTES.md'}")
    print("  2. Create the UC connections in connections/create_connections.sql")
    print(f"  3. cd {result['output_dir']} && databricks bundle validate")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
