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
import stat
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ftlfc.bundle import build_bundle


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-p", "--plan", type=Path, default=Path("out/plan.json"))
    parser.add_argument("-o", "--output", type=Path, default=Path("out/bundle"))
    parser.add_argument("--name", default="lakeflow-ingestion", help="bundle name")
    parser.add_argument("--host", help="Databricks workspace URL for both targets")
    parser.add_argument("--email", help="address for pipeline and job failure notifications")
    parser.add_argument(
        "--force", action="store_true", help="overwrite an existing output directory"
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
    if args.output.exists() and any(args.output.iterdir()) and not args.force:
        print(
            f"error: {args.output} is not empty. Pass --force to overwrite.",
            file=sys.stderr,
        )
        return 1

    files = build_bundle(plan, bundle_name=args.name, host=args.host, notification_email=args.email)

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
    print("    1. Fill in the REPLACE_ME values in scripts/create_connections.sh")
    print(f"    2. cd {output} && ./scripts/create_connections.sh <profile>")
    print("    3. databricks bundle validate --strict -t dev")
    print("    4. databricks bundle deploy -t dev")

    blocked = summary["connections_blocked"]
    if blocked:
        print(
            f"\n  {blocked} connection(s) are absent from the bundle because they have no "
            f"automatable path. See {output}/README.md.",
            file=sys.stderr,
        )


if __name__ == "__main__":
    raise SystemExit(main())
