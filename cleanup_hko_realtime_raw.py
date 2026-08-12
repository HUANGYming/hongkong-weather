#!/usr/bin/env python3
"""Prune old HKO realtime raw observations from PostgreSQL."""

from __future__ import annotations

import argparse

from hko_common import (
    OFFICIAL_DAILY_TABLE,
    PROVISIONAL_DAILY_TABLE,
    REALTIME_RAW_TABLE,
    connect_database,
    database_url_from_env,
)


def connect(database_url: str):
    return connect_database(database_url)


def cleanup(conn, retention_days: int, prune_official_covered_provisional: bool) -> tuple[int, int]:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            DELETE FROM {REALTIME_RAW_TABLE}
            WHERE obs_time < now() - (%s * interval '1 day')
            """,
            (retention_days,),
        )
        raw_deleted = cur.rowcount

        provisional_deleted = 0
        if prune_official_covered_provisional:
            cur.execute(
                f"""
                DELETE FROM {PROVISIONAL_DAILY_TABLE} p
                USING {OFFICIAL_DAILY_TABLE} o
                WHERE p.date = o.date
                  AND p.date < (now() AT TIME ZONE 'Asia/Hong_Kong')::date - (%s * interval '1 day')
                """,
                (retention_days,),
            )
            provisional_deleted = cur.rowcount
    conn.commit()
    return raw_deleted, provisional_deleted


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Delete old HKO realtime raw observations.")
    parser.add_argument("--database-url", default=database_url_from_env())
    parser.add_argument("--retention-days", type=int, default=60)
    parser.add_argument(
        "--keep-official-covered-provisional",
        action="store_true",
        help="Keep provisional daily rows even when an official row exists for the same old date.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if not args.database_url:
        raise SystemExit("DATABASE_URL is required")
    if args.retention_days < 1:
        raise SystemExit("--retention-days must be positive")

    conn = connect(args.database_url)
    try:
        raw_deleted, provisional_deleted = cleanup(
            conn,
            retention_days=args.retention_days,
            prune_official_covered_provisional=not args.keep_official_covered_provisional,
        )
        print(f"Deleted {raw_deleted:,} raw realtime observations")
        print(f"Deleted {provisional_deleted:,} old official-covered provisional rows")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
