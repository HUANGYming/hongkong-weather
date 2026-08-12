#!/usr/bin/env python3
"""Update a SQLite database with HKO daily wide weather data.

The target table is one row per date, using `date` as the primary key. Running
this script repeatedly is safe: rows are inserted or replaced by date.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from scrape_hko_history import (
    HKO_ELEMENTS,
    STATION_CODE,
    STATION_NAME,
    Element,
    download_raw_csv,
    parse_daily_csv,
)


DEFAULT_DB_PATH = Path("data/hko_weather.sqlite")
DEFAULT_START_DATE = date(2020, 1, 1)
DEFAULT_TABLE = "hko_daily_weather_wide"


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def parse_iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid ISO date: {value}") from exc


def value_to_float(value: str) -> float | None:
    value = value.strip()
    if not value:
        return None
    return float(value)


def create_schema(conn: sqlite3.Connection, table: str, elements: list[Element]) -> None:
    value_columns: list[str] = []
    for element in elements:
        value_columns.extend(
            [
                f"{quote_identifier(element.column)} REAL",
                f"{quote_identifier(element.column + '_raw')} TEXT",
                f"{quote_identifier(element.column + '_completeness')} TEXT",
            ]
        )

    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {quote_identifier(table)} (
            date TEXT PRIMARY KEY,
            date_is_valid INTEGER NOT NULL,
            station_code TEXT NOT NULL,
            station_name TEXT NOT NULL,
            {", ".join(value_columns)},
            updated_at_utc TEXT NOT NULL
        )
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS {quote_identifier(table + '_station_date_idx')}
        ON {quote_identifier(table)} (station_code, date)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS hko_ingest_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at_utc TEXT NOT NULL,
            finished_at_utc TEXT,
            status TEXT NOT NULL,
            table_name TEXT NOT NULL,
            years TEXT NOT NULL,
            rows_upserted INTEGER,
            min_date TEXT,
            max_date TEXT,
            error TEXT
        )
        """
    )
    conn.commit()


def latest_loaded_date(conn: sqlite3.Connection, table: str) -> date | None:
    row = conn.execute(f"SELECT max(date) FROM {quote_identifier(table)}").fetchone()
    if not row or not row[0]:
        return None
    return date.fromisoformat(row[0])


def years_to_update(
    conn: sqlite3.Connection,
    table: str,
    start_date: date,
    end_date: date,
    lookback_days: int,
    force_full: bool,
) -> list[int]:
    if force_full:
        first_year = start_date.year
    else:
        latest = latest_loaded_date(conn, table)
        if latest is None:
            first_year = start_date.year
        else:
            refresh_from = max(start_date, latest - timedelta(days=lookback_days))
            first_year = refresh_from.year

    return list(range(first_year, end_date.year + 1))


def build_wide_rows(
    long_rows: list[dict[str, str]],
    elements: list[Element],
    start_date: date,
    end_date: date,
    updated_at_utc: str,
) -> list[dict[str, object]]:
    allowed_codes = {element.code for element in elements}
    by_date: dict[str, dict[str, object]] = {}

    for row in long_rows:
        observed_date = date.fromisoformat(row["date"])
        if observed_date < start_date or observed_date > end_date or row["element_code"] not in allowed_codes:
            continue

        item = by_date.setdefault(
            row["date"],
            {
                "date": row["date"],
                "date_is_valid": 1 if row["date_is_valid"] == "true" else 0,
                "station_code": STATION_CODE,
                "station_name": STATION_NAME,
                "updated_at_utc": updated_at_utc,
            },
        )
        element = HKO_ELEMENTS[row["element_code"]]
        item[element.column] = value_to_float(row["value"])
        item[f"{element.column}_raw"] = row["raw_value"] or None
        item[f"{element.column}_completeness"] = row["completeness"] or None

    for item in by_date.values():
        for element in elements:
            item.setdefault(element.column, None)
            item.setdefault(f"{element.column}_raw", None)
            item.setdefault(f"{element.column}_completeness", None)

    return [by_date[key] for key in sorted(by_date)]


def upsert_rows(conn: sqlite3.Connection, table: str, rows: list[dict[str, object]], elements: list[Element]) -> int:
    if not rows:
        return 0

    columns = ["date", "date_is_valid", "station_code", "station_name"]
    for element in elements:
        columns.extend([element.column, f"{element.column}_raw", f"{element.column}_completeness"])
    columns.append("updated_at_utc")

    placeholders = ", ".join("?" for _ in columns)
    quoted_columns = ", ".join(quote_identifier(column) for column in columns)
    update_columns = [column for column in columns if column != "date"]
    assignments = ", ".join(
        f"{quote_identifier(column)} = excluded.{quote_identifier(column)}" for column in update_columns
    )

    sql = f"""
        INSERT INTO {quote_identifier(table)} ({quoted_columns})
        VALUES ({placeholders})
        ON CONFLICT(date) DO UPDATE SET {assignments}
    """
    values = [tuple(row[column] for column in columns) for row in rows]
    conn.executemany(sql, values)
    conn.commit()
    return len(rows)


def insert_run(conn: sqlite3.Connection, table: str, years: list[int]) -> int:
    cursor = conn.execute(
        """
        INSERT INTO hko_ingest_runs (started_at_utc, status, table_name, years)
        VALUES (?, ?, ?, ?)
        """,
        (utc_now(), "running", table, ",".join(str(year) for year in years)),
    )
    conn.commit()
    return int(cursor.lastrowid)


def finish_run(
    conn: sqlite3.Connection,
    run_id: int,
    status: str,
    rows: list[dict[str, object]],
    error: str | None = None,
) -> None:
    conn.execute(
        """
        UPDATE hko_ingest_runs
        SET finished_at_utc = ?,
            status = ?,
            rows_upserted = ?,
            min_date = ?,
            max_date = ?,
            error = ?
        WHERE id = ?
        """,
        (
            utc_now(),
            status,
            len(rows),
            rows[0]["date"] if rows else None,
            rows[-1]["date"] if rows else None,
            error,
            run_id,
        ),
    )
    conn.commit()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Update SQLite with HKO daily wide weather rows.")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH, help="SQLite database path.")
    parser.add_argument("--table", default=DEFAULT_TABLE, help="Destination table name.")
    parser.add_argument("--start-date", type=parse_iso_date, default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", type=parse_iso_date, default=date.today())
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--lookback-days", type=int, default=14, help="Refresh this many days before latest DB date.")
    parser.add_argument("--full-refresh", action="store_true", help="Rebuild from --start-date through --end-date.")
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--delay", type=float, default=1.0)
    parser.add_argument("--sleep", type=float, default=0.2)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.lookback_days < 0:
        raise SystemExit("--lookback-days cannot be negative")
    if args.start_date > args.end_date:
        raise SystemExit("--start-date cannot be after --end-date")

    elements = list(HKO_ELEMENTS.values())
    args.db.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(args.db)
    try:
        create_schema(conn, args.table, elements)
        years = years_to_update(
            conn=conn,
            table=args.table,
            start_date=args.start_date,
            end_date=args.end_date,
            lookback_days=args.lookback_days,
            force_full=args.full_refresh,
        )
        run_id = insert_run(conn, args.table, years)
        updated_at = utc_now()

        long_rows: list[dict[str, str]] = []
        for element in elements:
            for year in years:
                print(f"Downloading {STATION_CODE} {element.code} {year}...", file=sys.stderr)
                raw_path, source_url = download_raw_csv(
                    element=element,
                    year=str(year),
                    raw_dir=args.raw_dir,
                    retries=args.retries,
                    delay=args.delay,
                    overwrite=True,
                )
                long_rows.extend(parse_daily_csv(raw_path, element, source_url))
                time.sleep(args.sleep)

        wide_rows = build_wide_rows(long_rows, elements, args.start_date, args.end_date, updated_at)
        rows_upserted = upsert_rows(conn, args.table, wide_rows, elements)
        finish_run(conn, run_id, "success", wide_rows)

        print(f"Upserted {rows_upserted:,} wide rows into {args.db}:{args.table}", file=sys.stderr)
        if wide_rows:
            print(f"Date range: {wide_rows[0]['date']} to {wide_rows[-1]['date']}", file=sys.stderr)
        return 0
    except Exception as exc:
        if "run_id" in locals():
            finish_run(conn, run_id, "failed", [], error=str(exc))
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
