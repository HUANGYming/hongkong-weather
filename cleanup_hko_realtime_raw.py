#!/usr/bin/env python3
"""Prune old HKO realtime raw observations from PostgreSQL."""

from __future__ import annotations

import argparse
import os

from hko_common import (
    REALTIME_RAW_TABLE,
    connect_database,
    database_url_from_env,
)


def connect(database_url: str):
    return connect_database(database_url)


def cleanup(conn, retention_days: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            DELETE FROM {REALTIME_RAW_TABLE}
            WHERE obs_time < now() - (%s * interval '1 day')
            """,
            (retention_days,),
        )
        raw_deleted = cur.rowcount
    conn.commit()
    return raw_deleted


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Delete old HKO realtime raw observations.")
    parser.add_argument("--database-url", default=database_url_from_env())
    parser.add_argument("--retention-days", type=int, default=int(os.environ.get("HKO_RAW_RETENTION_DAYS", "60")))
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if not args.database_url:
        raise SystemExit("DATABASE_URL is required")
    if args.retention_days < 1:
        raise SystemExit("--retention-days must be positive")

    conn = connect(args.database_url)
    try:
        raw_deleted = cleanup(conn, retention_days=args.retention_days)
        print(f"Deleted {raw_deleted:,} raw realtime observations")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
