#!/usr/bin/env python3
"""
Port Operations Analytics Warehouse — Schema Applicator
========================================================
Applies all SQL schema files to the Postgres database in the
correct dependency order:

  01_dimensions.sql  →  dimension tables
  02_facts.sql       →  fact tables (FK to dimensions)
  03_indexes.sql     →  performance indexes
  04_views.sql       →  mart views (depend on facts + dims)
  05_operations.sql  →  TOS ops_* tables + dim_date enhancement

All DDL is idempotent (CREATE IF NOT EXISTS / CREATE OR REPLACE /
ALTER TABLE … ADD COLUMN IF NOT EXISTS), so this script is safe
to re-run at any time — e.g. after pulling updated schema files.

Usage
-----
  python scripts/apply_schema.py                  # default: schema/ dir
  python scripts/apply_schema.py --schema-dir path/to/schema
  python scripts/apply_schema.py --dry-run        # print SQL, don't execute

Prerequisites
-------------
  1. docker compose up -d postgres   (or any reachable Postgres 16 instance)
  2. pip install -r requirements.txt

Environment variables (loaded from .env if present):
  POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
"""

import os
import sys
import time
import argparse
from pathlib import Path

import psycopg2
from psycopg2 import sql

# ── Optional .env loading ──────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv not installed; rely on shell environment


# ─────────────────────────────────────────────────────────────
# Schema files — applied in this exact order every time.
# DO NOT reorder: facts depend on dimensions, views depend on both.
# ─────────────────────────────────────────────────────────────
SCHEMA_FILES = [
    "01_dimensions.sql",   # dim_date, dim_port, dim_terminal, …
    "02_facts.sql",        # fact_port_calls, fact_cargo_movements
    "03_indexes.sql",      # all performance indexes
    "04_views.sql",        # mart views (presentation layer)
    "05_operations.sql",   # TOS tables + dim_date holiday/season cols
]


def get_conn_params() -> dict:
    """Read connection parameters from environment variables."""
    return {
        "host":     os.getenv("POSTGRES_HOST",     "localhost"),
        "port":     int(os.getenv("POSTGRES_PORT", "5432")),
        "dbname":   os.getenv("POSTGRES_DB",       "port_analytics"),
        "user":     os.getenv("POSTGRES_USER",     "portadmin"),
        "password": os.getenv("POSTGRES_PASSWORD", "portpass123"),
    }


def wait_for_postgres(conn_params: dict, retries: int = 20, delay: int = 3) -> None:
    print("Waiting for Postgres", end="", flush=True)
    # Use 127.0.0.1 explicitly — avoids IPv6/IPv4 ambiguity on Windows
    # where 'localhost' may resolve to ::1 but Docker binds on 0.0.0.0
    params = {**conn_params, "host": "127.0.0.1"}
    for attempt in range(retries):
        try:
            conn = psycopg2.connect(**params, connect_timeout=3)
            conn.close()
            print(" ready.\n")
            return
        except psycopg2.OperationalError as e:
            msg = str(e).lower()
            # Auth errors mean Postgres IS running — no point retrying
            if "password" in msg or "role" in msg or "database" in msg:
                print()
                sys.exit(
                    f"\nERROR: Postgres is running but rejected the connection:\n"
                    f"  {e.args[0].strip()}\n\n"
                    f"This means your .env credentials don't match what the\n"
                    f"Postgres volume was initialised with. Fix:\n\n"
                    f"  docker compose down -v     ← wipes the old volume\n"
                    f"  docker compose up -d postgres\n"
                    f"  python scripts/apply_schema.py\n"
                )
            print(".", end="", flush=True)
            time.sleep(delay)
    print()
    sys.exit(
        "\nERROR: Could not reach Postgres after 60 seconds.\n"
        "  • Is Docker running?       docker compose up -d postgres\n"
        "  • Is the port occupied?    netstat -ano | findstr :5432\n"
        "  • Test the port:           Test-NetConnection localhost 5432\n"
    )


def apply_file(cur, path: Path, dry_run: bool) -> None:
    """
    Read a SQL file and execute it as a single statement block.
    psycopg2 execute() handles multi-statement scripts when
    autocommit is ON (each DDL statement is its own transaction).
    """
    sql_text = path.read_text(encoding="utf-8")

    if dry_run:
        print(f"\n{'─' * 60}")
        print(f"  [DRY RUN] Would execute: {path.name}")
        print(f"{'─' * 60}")
        # Print first 400 chars as a preview
        preview = sql_text[:400].strip()
        print(preview)
        if len(sql_text) > 400:
            print(f"  … ({len(sql_text):,} chars total)")
        return

    cur.execute(sql_text)


def main(schema_dir: Path, dry_run: bool) -> None:
    conn_params = get_conn_params()

    print("=" * 60)
    print("Port Analytics — Schema Applicator")
    print("=" * 60)
    print(f"\nTarget database : {conn_params['dbname']}")
    print(f"Host            : {conn_params['host']}:{conn_params['port']}")
    print(f"Schema dir      : {schema_dir.resolve()}")
    if dry_run:
        print("\n⚠  DRY RUN — no changes will be made to the database.\n")

    # ── Validate all files exist before connecting ─────────────
    missing = []
    for filename in SCHEMA_FILES:
        p = schema_dir / filename
        if not p.exists():
            missing.append(str(p))

    if missing:
        sys.exit(
            "\nERROR: The following schema files were not found:\n"
            + "\n".join(f"  {m}" for m in missing)
            + "\n\nCheck --schema-dir points to your schema/ directory.\n"
        )

    # ── Connect ────────────────────────────────────────────────
    if not dry_run:
        wait_for_postgres(conn_params)

    try:
        conn = psycopg2.connect(**conn_params) if not dry_run else None
    except psycopg2.OperationalError as exc:
        sys.exit(f"\nERROR: Could not connect to Postgres:\n  {exc}\n")

    if conn:
        # autocommit = True lets each DDL file commit independently.
        # This means a failure in file N doesn't roll back files 1…N-1,
        # which is safe because all DDL is idempotent.
        conn.autocommit = True

    try:
        cur = conn.cursor() if conn else None

        print("\nApplying schema files …\n")
        total_time = 0.0

        for filename in SCHEMA_FILES:
            path = schema_dir / filename
            t0   = time.monotonic()

            try:
                apply_file(cur, path, dry_run)
                elapsed = time.monotonic() - t0
                total_time += elapsed

                status = "[DRY RUN]" if dry_run else "✓"
                print(f"  {status}  {filename:<28}  ({elapsed:.1f}s)")

            except psycopg2.Error as exc:
                elapsed = time.monotonic() - t0
                print(f"\n  ✗  {filename} FAILED after {elapsed:.1f}s")
                print(f"\n  Postgres error:\n    {exc}")
                print(
                    "\n  Tip: The error line number above refers to the "
                    ".sql file, not this script.\n"
                    "  Open the file and jump to that line to inspect the "
                    "failing statement.\n"
                )
                if conn:
                    conn.close()
                sys.exit(1)

        print(f"\n{'=' * 60}")
        if dry_run:
            print("Dry run complete — no changes made.")
        else:
            print(f"All {len(SCHEMA_FILES)} schema files applied successfully.")
            print(f"Total time: {total_time:.1f}s")
            print("\nNext steps:")
            print("  python scripts/generate_data.py")
            print("  python scripts/load_data.py")
        print("=" * 60)

    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Apply SQL schema files to the port analytics Postgres database.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/apply_schema.py
  python scripts/apply_schema.py --schema-dir ./schema
  python scripts/apply_schema.py --dry-run
        """,
    )
    parser.add_argument(
        "--schema-dir",
        type=Path,
        default=Path(__file__).parent.parent / "schema",
        help="Directory containing the SQL schema files (default: ../schema)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the SQL that would be executed without running it",
    )
    args = parser.parse_args()

    if not args.schema_dir.is_dir():
        sys.exit(
            f"\nERROR: schema directory not found: {args.schema_dir}\n"
            "Use --schema-dir to specify the correct path.\n"
        )

    main(schema_dir=args.schema_dir, dry_run=args.dry_run)
