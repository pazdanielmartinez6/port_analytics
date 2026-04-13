#!/usr/bin/env python3
"""
Port Operations Analytics Warehouse — Data Loader
==================================================
Loads seed CSVs into Postgres using COPY FROM STDIN
(fastest bulk ingestion method — avoids row-by-row inserts).

Prerequisites:
  1. docker-compose up -d
  2. pip install -r requirements.txt
  3. python scripts/generate_data.py   (creates seeds/*.csv)

Usage:
  python scripts/load_data.py [--env .env]
"""

import os
import sys
import io
import time
import argparse
import psycopg2
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

# ── Optional .env loading ──────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv not installed; rely on shell env


def get_conn_params() -> dict:
    return {
        'host':     '127.0.0.1',  # explicit IPv4 — avoids Windows IPv6 resolution
        'port':     int(os.getenv('POSTGRES_PORT', '5432')),
        'dbname':   os.getenv('POSTGRES_DB',       'port_analytics'),
        'user':     os.getenv('POSTGRES_USER',     'portadmin'),
        'password': os.getenv('POSTGRES_PASSWORD', 'portpass123'),
    }


# ── Load order matters: dimensions before facts ────────────────
LOAD_ORDER = [
    # (csv_stem, table_name)
    ('dim_date',            'dim_date'),
    ('dim_port',            'dim_port'),
    ('dim_terminal',        'dim_terminal'),
    ('dim_ship_size_class', 'dim_ship_size_class'),
    ('dim_vessel',          'dim_vessel'),
    ('dim_carrier',         'dim_carrier'),
    ('dim_cargo_type',      'dim_cargo_type'),
    ('dim_direction',       'dim_direction'),
    ('dim_country',         'dim_country'),
    ('dim_route',           'dim_route'),
    ('dim_incoterm',        'dim_incoterm'),
    ('fact_port_calls',     'fact_port_calls'),
    ('fact_cargo_movements','fact_cargo_movements'),
]


def wait_for_postgres(conn_params: dict, retries: int = 15, delay: int = 3) -> None:
    """Poll until Postgres is ready, using 127.0.0.1 to avoid IPv6 issues on Windows."""
    print('Waiting for Postgres …', end='', flush=True)
    # Force IPv4 by using 127.0.0.1 instead of 'localhost'
    # On Windows, 'localhost' may resolve to ::1 (IPv6) but Docker
    # binds on 0.0.0.0 (IPv4), causing silent connection failures.
    params = {**conn_params, 'host': '127.0.0.1'}
    for attempt in range(retries):
        try:
            conn = psycopg2.connect(**params, connect_timeout=3)
            conn.close()
            print(' ready.')
            return
        except psycopg2.OperationalError as e:
            msg = str(e).lower()
            if 'password' in msg or 'role' in msg or 'database' in msg:
                print()
                sys.exit(
                    f'\nERROR: Postgres is running but rejected the connection:\n'
                    f'  {str(e).strip()}\n'
                    f'Check your .env credentials match the Docker volume.\n'
                )
            print('.', end='', flush=True)
            time.sleep(delay)
    print()
    sys.exit('ERROR: Could not connect to Postgres after multiple retries.')


def load_csv(cur, seeds_dir: Path, csv_stem: str, table: str) -> int:
    csv_path = seeds_dir / f'{csv_stem}.csv'
    if not csv_path.exists():
        print(f'  SKIP  {csv_path} not found — run generate_data.py first')
        return 0

    data = csv_path.read_text(encoding='utf-8')
    buf  = io.StringIO(data)

    header   = buf.readline()
    columns  = [c.strip() for c in header.split(',')]
    col_list = ', '.join(columns)

    sql = (
        f"COPY {table} ({col_list}) "
        f"FROM STDIN WITH (FORMAT csv, HEADER false, NULL '')"
    )
    cur.copy_expert(sql, buf)
    return cur.rowcount


def truncate_all(cur) -> None:
    """Truncate in reverse FK order before reload."""
    tables = [t for _, t in reversed(LOAD_ORDER)]
    cur.execute(
        f"TRUNCATE {', '.join(tables)} RESTART IDENTITY CASCADE;"
    )
    print('  Tables truncated.')


def main(seeds_dir: Path, truncate: bool) -> None:
    conn_params = get_conn_params()
    wait_for_postgres(conn_params)

    conn = psycopg2.connect(**conn_params)
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            if truncate:
                print('\nTruncating existing data …')
                truncate_all(cur)

            print('\nLoading seed CSVs …')
            total_rows = 0
            for csv_stem, table in LOAD_ORDER:
                t0   = time.monotonic()
                rows = load_csv(cur, seeds_dir, csv_stem, table)
                elapsed = time.monotonic() - t0
                print(f'  ✓  {table:<30}  {rows:>8,} rows  ({elapsed:.1f}s)')
                total_rows += rows

        conn.commit()
        print(f'\nCommitted. Total rows inserted: {total_rows:,}')

    except Exception as exc:
        conn.rollback()
        print(f'\nERROR — rolled back: {exc}')
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Load synthetic port data into Postgres'
    )
    parser.add_argument(
        '--seeds-dir',
        type=Path,
        default=Path(__file__).parent.parent / 'seeds',
        help='Directory containing seed CSV files (default: ../seeds)',
    )
    parser.add_argument(
        '--truncate',
        action='store_true',
        default=True,
        help='Truncate tables before loading (default: True)',
    )
    parser.add_argument(
        '--no-truncate',
        dest='truncate',
        action='store_false',
        help='Append data without truncating',
    )
    args = parser.parse_args()
    main(seeds_dir=args.seeds_dir, truncate=args.truncate)
