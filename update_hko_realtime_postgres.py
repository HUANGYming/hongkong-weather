#!/usr/bin/env python3
"""Load HKO realtime and historical-archive observations into PostgreSQL."""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, timedelta
from urllib.parse import urlencode

import requests

from hko_common import (
    HOURLY_RAINFALL_URL,
    MEDIUM_VARCHAR,
    REALTIME_CSV_RESOURCES,
    REALTIME_RAW_TABLE,
    SCHEMA_LOCK_KEY,
    SHORT_VARCHAR,
    STATION_CODE,
    STATION_NAME,
    Observation,
    connect_database,
    database_url_from_env,
    ensure_database_schema,
    hk_today,
    parse_hourly_rainfall_json,
    parse_realtime_archive_zip,
    parse_realtime_csv_text,
)


ARCHIVE_LIST_URL = "https://app.data.gov.hk/v1/historical-archive/list-file-versions"
ARCHIVE_GET_URL = "https://app.data.gov.hk/v1/historical-archive/get-file"


def connect(database_url: str):
    return connect_database(database_url)


def fetch_text(url: str, timeout: int = 60) -> str:
    response = requests.get(url, timeout=timeout, headers={"User-Agent": "hko-realtime-updater/1.0"})
    response.raise_for_status()
    response.encoding = "utf-8-sig"
    return response.text


def fetch_bytes(url: str, params: dict[str, str], timeout: int = 120) -> bytes:
    response = requests.get(
        url,
        params=params,
        timeout=timeout,
        headers={"User-Agent": "hko-realtime-updater/1.0"},
    )
    response.raise_for_status()
    return response.content


def create_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_lock(hashtext(%s))", (SCHEMA_LOCK_KEY,))
    try:
        with conn.cursor() as cur:
            ensure_database_schema(cur)
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {REALTIME_RAW_TABLE} (
                obs_time timestamptz NOT NULL,
                obs_date_hk date NOT NULL,
                station_code {SHORT_VARCHAR} NOT NULL DEFAULT 'HKO',
                station_name {SHORT_VARCHAR} NOT NULL DEFAULT 'Hong Kong Observatory',
                source {SHORT_VARCHAR} NOT NULL,
                metric {SHORT_VARCHAR} NOT NULL,
                value double precision,
                raw_value {MEDIUM_VARCHAR},
                unit {SHORT_VARCHAR},
                fetched_at timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (obs_time, source, metric, station_code)
                )
                """
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(hashtext(%s))", (SCHEMA_LOCK_KEY,))
        conn.commit()


def upsert_observations(conn, observations: list[Observation]) -> int:
    if not observations:
        return 0

    deduped = dedupe_observation_rows(observations)
    values = list(deduped.values())
    with conn.cursor() as cur:
        cur.executemany(
            f"""
            DELETE FROM {REALTIME_RAW_TABLE}
            WHERE obs_time = %s
              AND source = %s
              AND metric = %s
              AND station_code = %s
            """,
            list(deduped.keys()),
        )
        insert_observation_rows(cur, values)
    conn.commit()
    return len(values)


def replace_archive_observations(conn, observations: list[Observation]) -> tuple[int, int]:
    if not observations:
        return 0, 0

    deduped = dedupe_observation_rows(observations)
    values = list(deduped.values())
    delete_keys = sorted({(row[1], row[4], row[5], row[2]) for row in values})
    with conn.cursor() as cur:
        cur.executemany(
            f"""
            DELETE FROM {REALTIME_RAW_TABLE}
            WHERE obs_date_hk = %s
              AND source = %s
              AND metric = %s
              AND station_code = %s
            """,
            delete_keys,
        )
        insert_observation_rows(cur, values)
    conn.commit()
    return len(values), len(delete_keys)


def dedupe_observation_rows(observations: list[Observation]) -> dict[tuple[object, str, str, str], tuple[object, ...]]:
    return {
        (obs.obs_time, obs.source, obs.metric, STATION_CODE): (
            obs.obs_time,
            obs.obs_date_hk,
            STATION_CODE,
            STATION_NAME,
            obs.source,
            obs.metric,
            obs.value,
            obs.raw_value,
            obs.unit,
        )
        for obs in observations
    }


def insert_observation_rows(cur, values: list[tuple[object, ...]]) -> None:
    cur.executemany(
        f"""
        INSERT INTO {REALTIME_RAW_TABLE} (
            obs_time,
            obs_date_hk,
            station_code,
            station_name,
            source,
            metric,
            value,
            raw_value,
            unit
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        values,
    )


def fetch_current_observations(include_rainfall: bool) -> list[Observation]:
    observations: list[Observation] = []
    for key, config in REALTIME_CSV_RESOURCES.items():
        text = fetch_text(str(config["url"]))
        observations.extend(parse_realtime_csv_text(text, key))
    if include_rainfall:
        observations.extend(parse_hourly_rainfall_json(fetch_text(HOURLY_RAINFALL_URL)))
    return observations


def archive_zip_bytes(resource_url: str, archive_date: date) -> bytes:
    return fetch_bytes(
        ARCHIVE_GET_URL,
        {
            "url": resource_url,
            "time": archive_date.strftime("%Y%m%d"),
        },
    )


def archive_available_dates(resource_url: str, start_date: date, end_date: date) -> set[date]:
    response = requests.get(
        ARCHIVE_LIST_URL,
        params={
            "url": resource_url,
            "start": start_date.strftime("%Y%m%d"),
            "end": end_date.strftime("%Y%m%d"),
        },
        timeout=60,
        headers={"User-Agent": "hko-realtime-updater/1.0"},
    )
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        detail = response.text.strip().replace("\n", " ")[:500]
        raise RuntimeError(
            "DATA.GOV.HK archive list request failed "
            f"for {start_date} to {end_date}: HTTP {response.status_code}. {detail}"
        ) from exc
    payload = response.json()
    dates: set[date] = set()
    for item in payload.get("data-files", []):
        stamp = str(item["timestamp"])
        dates.add(date(int(stamp[0:4]), int(stamp[4:6]), int(stamp[6:8])))
    return dates


def parse_archive_zip(zip_bytes: bytes, resource_key: str) -> list[Observation]:
    return parse_realtime_archive_zip(zip_bytes, resource_key)


def fetch_archive_observations(start_date: date, end_date: date) -> list[Observation]:
    observations: list[Observation] = []
    for key, config in REALTIME_CSV_RESOURCES.items():
        resource_url = str(config["url"])
        available_dates = archive_available_dates(resource_url, start_date, end_date)
        for archive_date in sorted(available_dates):
            print(f"Archive {key} {archive_date}", file=sys.stderr)
            observations.extend(parse_archive_zip(archive_zip_bytes(resource_url, archive_date), key))
    return observations


def parse_iso_date(value: str) -> date:
    return date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Upsert HKO realtime observations into PostgreSQL.")
    parser.add_argument("--database-url", default=database_url_from_env())
    parser.add_argument("--mode", choices=["current", "archive", "both"], default="current")
    parser.add_argument("--start-date", type=parse_iso_date, help="Archive start date.")
    parser.add_argument("--end-date", type=parse_iso_date, help="Archive end date.")
    parser.add_argument("--archive-lookback-days", type=int, default=14)
    parser.add_argument("--include-rainfall", action="store_true", help="Fetch current hourly rainfall API.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if not args.database_url:
        raise SystemExit("DATABASE_URL is required")

    today = hk_today()
    latest_archive_date = today - timedelta(days=1)
    if args.mode in {"archive", "both"}:
        end_date = args.end_date or latest_archive_date
        if end_date > latest_archive_date:
            print(
                "DATA.GOV.HK Historical Archive is available through "
                f"{latest_archive_date}; using that instead of {end_date}. "
                "Run --mode current --include-rainfall for today's live rows.",
                file=sys.stderr,
            )
            end_date = latest_archive_date
    else:
        end_date = args.end_date or today
    start_date = args.start_date or (end_date - timedelta(days=args.archive_lookback_days - 1))
    if start_date > end_date:
        raise SystemExit("--start-date cannot be after --end-date")

    conn = connect(args.database_url)
    try:
        create_schema(conn)
        current_observations: list[Observation] = []
        archive_observations: list[Observation] = []
        if args.mode in {"current", "both"}:
            current_observations = fetch_current_observations(args.include_rainfall)
        if args.mode in {"archive", "both"}:
            archive_observations = fetch_archive_observations(start_date, end_date)

        if archive_observations:
            replaced, groups = replace_archive_observations(conn, archive_observations)
            print(
                f"Replaced {replaced:,} archive observations across {groups:,} date/source/metric groups",
                file=sys.stderr,
            )
        if current_observations:
            upserted = upsert_observations(conn, current_observations)
            print(f"Upserted {upserted:,} current observations", file=sys.stderr)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
