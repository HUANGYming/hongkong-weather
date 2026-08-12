#!/usr/bin/env python3
"""Load official HKO D1 daily weather data into PostgreSQL."""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date, timedelta
from urllib.parse import urlencode

import requests

from hko_common import (
    DB_SCHEMA,
    HKO_ELEMENTS,
    INGEST_RUN_TABLE,
    LONG_VARCHAR,
    MEDIUM_VARCHAR,
    OFFICIAL_DAILY_TABLE,
    SCHEMA_LOCK_KEY,
    SHORT_VARCHAR,
    STATION_CODE,
    STATION_NAME,
    build_official_wide_rows,
    connect_database,
    database_url_from_env,
    drop_view_if_exists,
    ensure_database_schema,
    hk_today,
    OFFICIAL_DAILY_TABLE_NAME,
    official_insert_columns,
    parse_d1_csv_text,
    parse_daily_extract_json_text,
)


D1_BASE_URL = "https://data.weather.gov.hk/weatherAPI/D1/caller.php"
DAILY_EXTRACT_YEAR_URL = "https://www.hko.gov.hk/cis/dailyExtract/dailyExtract_{year}.xml"
DAILY_EXTRACT_MONTH_URL = "https://www.hko.gov.hk/cis/dailyExtract/dailyExtract_{year}{month:02d}.xml"
DEFAULT_START_DATE = date(2020, 1, 1)


def connect(database_url: str):
    return connect_database(database_url)


def d1_url(element_code: str, year: int) -> str:
    return f"{D1_BASE_URL}?{urlencode({'stn': STATION_CODE, 'ele': element_code, 'yr': str(year)})}"


def fetch_text(url: str, timeout: int = 60) -> str:
    response = requests.get(url, timeout=timeout, headers={"User-Agent": "hko-postgres-updater/1.0"})
    response.raise_for_status()
    response.encoding = "utf-8-sig"
    if response.text.startswith("File data not existed"):
        return ""
    return response.text


def fetch_optional_text(url: str, timeout: int = 60) -> str:
    response = requests.get(url, timeout=timeout, headers={"User-Agent": "hko-postgres-updater/1.0"})
    if response.status_code == 404:
        return ""
    response.raise_for_status()
    response.encoding = "utf-8-sig"
    return response.text


def create_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_lock(hashtext(%s))", (SCHEMA_LOCK_KEY,))
    try:
        columns_sql = []
        for element in HKO_ELEMENTS.values():
            columns_sql.extend(
                [
                    f"{element.column} double precision",
                    f"{element.column}_raw {MEDIUM_VARCHAR}",
                    f"{element.column}_completeness {SHORT_VARCHAR}",
                ]
            )

        with conn.cursor() as cur:
            ensure_database_schema(cur)
            drop_view_if_exists(cur, OFFICIAL_DAILY_TABLE_NAME)
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {OFFICIAL_DAILY_TABLE} (
                    date date PRIMARY KEY,
                    station_code {SHORT_VARCHAR} NOT NULL DEFAULT 'HKO',
                    station_name {SHORT_VARCHAR} NOT NULL DEFAULT 'Hong Kong Observatory',
                    {", ".join(columns_sql)},
                    source {SHORT_VARCHAR} NOT NULL DEFAULT 'hko_d1',
                    updated_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {INGEST_RUN_TABLE} (
                    id bigint PRIMARY KEY,
                    started_at timestamptz NOT NULL DEFAULT now(),
                    finished_at timestamptz,
                    status {SHORT_VARCHAR} NOT NULL,
                    job_name {SHORT_VARCHAR} NOT NULL,
                    years {MEDIUM_VARCHAR},
                    rows_upserted integer,
                    min_date date,
                    max_date date,
                    error {LONG_VARCHAR}
                )
                """
            )
            validate_official_table_columns(cur)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(hashtext(%s))", (SCHEMA_LOCK_KEY,))
        conn.commit()


def validate_official_table_columns(cur) -> None:
    required_columns = set(official_insert_columns()) | {"source", "updated_at"}
    if DB_SCHEMA:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
            """,
            (DB_SCHEMA, OFFICIAL_DAILY_TABLE_NAME),
        )
    else:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = %s
            """,
            (OFFICIAL_DAILY_TABLE_NAME,),
        )
    existing_columns = {row[0] for row in cur.fetchall()}
    missing_columns = sorted(required_columns - existing_columns)
    if missing_columns:
        raise RuntimeError(
            f"Existing {OFFICIAL_DAILY_TABLE_NAME} table is missing columns: "
            f"{', '.join(missing_columns)}. Drop/recreate the old object before rerunning."
        )


def latest_official_date(conn) -> date | None:
    with conn.cursor() as cur:
        cur.execute(f"SELECT max(date) FROM {OFFICIAL_DAILY_TABLE}")
        row = cur.fetchone()
    return row[0] if row and row[0] else None


def years_to_update(conn, start_date: date, end_date: date, lookback_days: int, full_refresh: bool) -> list[int]:
    if full_refresh:
        first_year = start_date.year
    else:
        latest = latest_official_date(conn)
        if latest is None:
            first_year = start_date.year
        else:
            first_year = max(start_date, latest - timedelta(days=lookback_days)).year
    return list(range(first_year, end_date.year + 1))


def recent_months(start_date: date, end_date: date, lookback_days: int) -> list[tuple[int, int]]:
    month_start = max(start_date, end_date - timedelta(days=max(lookback_days, 75))).replace(day=1)
    months: list[tuple[int, int]] = []
    cursor = month_start
    while cursor <= end_date:
        months.append((cursor.year, cursor.month))
        if cursor.month == 12:
            cursor = date(cursor.year + 1, 1, 1)
        else:
            cursor = date(cursor.year, cursor.month + 1, 1)
    return months


def upsert_official_rows(conn, rows: list[dict[str, object]]) -> int:
    if not rows:
        return 0

    min_date = min(row["date"] for row in rows)
    max_date = max(row["date"] for row in rows)
    columns = official_insert_columns()
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f"""
        INSERT INTO {OFFICIAL_DAILY_TABLE} ({", ".join(columns)})
        VALUES ({placeholders})
    """
    values = [tuple(row[column] for column in columns) for row in rows]
    with conn.cursor() as cur:
        print(f"Deleting official rows from {min_date} to {max_date}", file=sys.stderr)
        cur.execute(f"DELETE FROM {OFFICIAL_DAILY_TABLE} WHERE date BETWEEN %s AND %s", (min_date, max_date))
        print(f"Inserting {len(values):,} official rows", file=sys.stderr)
        cur.executemany(sql, values)
    conn.commit()
    return len(rows)


def insert_run(conn, years: list[int]) -> int:
    run_id = time.time_ns()
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {INGEST_RUN_TABLE} (id, status, job_name, years)
            VALUES (%s, 'running', 'hko_weather_official_daily_ingest', %s)
            """,
            (run_id, ",".join(str(year) for year in years)),
        )
    conn.commit()
    return run_id


def finish_run(conn, run_id: int, status: str, rows: list[dict[str, object]], error: str | None = None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {INGEST_RUN_TABLE}
            SET finished_at = now(),
                status = %s,
                rows_upserted = %s,
                min_date = %s,
                max_date = %s,
                error = %s
            WHERE id = %s
            """,
            (
                status,
                len(rows),
                rows[0]["date"] if rows else None,
                rows[-1]["date"] if rows else None,
                error,
                run_id,
            ),
        )
    conn.commit()


def parse_iso_date(value: str) -> date:
    return date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Upsert official HKO D1 daily data into PostgreSQL.")
    parser.add_argument("--database-url", default=database_url_from_env())
    parser.add_argument("--start-date", type=parse_iso_date, default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", type=parse_iso_date, default=hk_today())
    parser.add_argument("--lookback-days", type=int, default=45)
    parser.add_argument("--full-refresh", action="store_true")
    parser.add_argument("--sleep", type=float, default=0.2)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if not args.database_url:
        raise SystemExit("DATABASE_URL is required")
    if args.start_date > args.end_date:
        raise SystemExit("--start-date cannot be after --end-date")

    conn = connect(args.database_url)
    try:
        create_schema(conn)
        years = years_to_update(conn, args.start_date, args.end_date, args.lookback_days, args.full_refresh)
        run_id = insert_run(conn, years)
        long_rows: list[dict[str, object]] = []
        for element in HKO_ELEMENTS.values():
            for year in years:
                url = d1_url(element.code, year)
                print(f"Downloading {element.code} {year}", file=sys.stderr)
                text = fetch_text(url)
                if text:
                    long_rows.extend(parse_d1_csv_text(text, element))
                time.sleep(args.sleep)

        for year in years:
            url = DAILY_EXTRACT_YEAR_URL.format(year=year)
            print(f"Downloading Daily Extract {year}", file=sys.stderr)
            text = fetch_optional_text(url)
            if text:
                long_rows.extend(parse_daily_extract_json_text(text, year))
            time.sleep(args.sleep)

        for year, month in recent_months(args.start_date, args.end_date, args.lookback_days):
            url = DAILY_EXTRACT_MONTH_URL.format(year=year, month=month)
            print(f"Downloading Daily Extract {year}-{month:02d}", file=sys.stderr)
            text = fetch_optional_text(url)
            if text:
                long_rows.extend(parse_daily_extract_json_text(text, year))
            time.sleep(args.sleep)

        print("Building official wide rows", file=sys.stderr)
        wide_rows = build_official_wide_rows(long_rows, args.start_date, args.end_date)
        upserted = upsert_official_rows(conn, wide_rows)
        print("Finishing official ingest run", file=sys.stderr)
        finish_run(conn, run_id, "success", wide_rows)
        print(f"Upserted {upserted:,} official rows", file=sys.stderr)
        return 0
    except Exception as exc:
        if "run_id" in locals():
            finish_run(conn, run_id, "failed", [], error=str(exc))
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
