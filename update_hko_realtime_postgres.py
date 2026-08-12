#!/usr/bin/env python3
"""Load provisional HKO realtime and historical-archive snapshots into PostgreSQL."""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, timedelta
from urllib.parse import urlencode

import requests

from hko_common import (
    HOURLY_RAINFALL_URL,
    LATEST_DAILY_VIEW,
    MEDIUM_VARCHAR,
    OFFICIAL_DAILY_TABLE,
    PROVISIONAL_DAILY_TABLE,
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
                CREATE TABLE IF NOT EXISTS {OFFICIAL_DAILY_TABLE} (
                date date PRIMARY KEY,
                station_code {SHORT_VARCHAR} NOT NULL DEFAULT 'HKO',
                station_name {SHORT_VARCHAR} NOT NULL DEFAULT 'Hong Kong Observatory',
                mslp_hpa double precision,
                mslp_hpa_raw {MEDIUM_VARCHAR},
                mslp_hpa_completeness {SHORT_VARCHAR},
                mean_temp_c double precision,
                mean_temp_c_raw {MEDIUM_VARCHAR},
                mean_temp_c_completeness {SHORT_VARCHAR},
                mean_dew_point_c double precision,
                mean_dew_point_c_raw {MEDIUM_VARCHAR},
                mean_dew_point_c_completeness {SHORT_VARCHAR},
                mean_wet_bulb_c double precision,
                mean_wet_bulb_c_raw {MEDIUM_VARCHAR},
                mean_wet_bulb_c_completeness {SHORT_VARCHAR},
                mean_relative_humidity_pct double precision,
                mean_relative_humidity_pct_raw {MEDIUM_VARCHAR},
                mean_relative_humidity_pct_completeness {SHORT_VARCHAR},
                mean_cloud_amount_pct double precision,
                mean_cloud_amount_pct_raw {MEDIUM_VARCHAR},
                mean_cloud_amount_pct_completeness {SHORT_VARCHAR},
                total_rainfall_mm double precision,
                total_rainfall_mm_raw {MEDIUM_VARCHAR},
                total_rainfall_mm_completeness {SHORT_VARCHAR},
                max_temp_c double precision,
                max_temp_c_raw {MEDIUM_VARCHAR},
                max_temp_c_completeness {SHORT_VARCHAR},
                min_temp_c double precision,
                min_temp_c_raw {MEDIUM_VARCHAR},
                min_temp_c_completeness {SHORT_VARCHAR},
                grass_min_temp_c double precision,
                grass_min_temp_c_raw {MEDIUM_VARCHAR},
                grass_min_temp_c_completeness {SHORT_VARCHAR},
                source {SHORT_VARCHAR} NOT NULL DEFAULT 'hko_d1',
                updated_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )
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
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {PROVISIONAL_DAILY_TABLE} (
                date date PRIMARY KEY,
                station_code {SHORT_VARCHAR} NOT NULL DEFAULT 'HKO',
                station_name {SHORT_VARCHAR} NOT NULL DEFAULT 'Hong Kong Observatory',
                mslp_hpa double precision,
                mean_temp_c double precision,
                mean_relative_humidity_pct double precision,
                total_rainfall_mm double precision,
                max_temp_c double precision,
                min_temp_c double precision,
                sample_count_temp integer,
                sample_count_humidity integer,
                sample_count_pressure integer,
                sample_count_rainfall integer,
                data_status {SHORT_VARCHAR} NOT NULL DEFAULT 'provisional',
                source {SHORT_VARCHAR} NOT NULL DEFAULT 'hko_realtime',
                first_obs_time timestamptz,
                last_obs_time timestamptz,
                updated_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                f"""
                DROP VIEW IF EXISTS {LATEST_DAILY_VIEW}
                """
            )
            cur.execute(
                f"""
                CREATE OR REPLACE VIEW {LATEST_DAILY_VIEW} AS
                SELECT
                    COALESCE(o.date, p.date) AS date,
                    CAST('HKO' AS {SHORT_VARCHAR}) AS station_code,
                    CAST('Hong Kong Observatory' AS {SHORT_VARCHAR}) AS station_name,
                    COALESCE(o.mslp_hpa, p.mslp_hpa) AS mslp_hpa,
                    COALESCE(o.mean_temp_c, p.mean_temp_c) AS mean_temp_c,
                    o.mean_dew_point_c,
                    o.mean_wet_bulb_c,
                    COALESCE(o.mean_relative_humidity_pct, p.mean_relative_humidity_pct) AS mean_relative_humidity_pct,
                    o.mean_cloud_amount_pct,
                    COALESCE(o.total_rainfall_mm, p.total_rainfall_mm) AS total_rainfall_mm,
                    COALESCE(o.max_temp_c, p.max_temp_c) AS max_temp_c,
                    COALESCE(o.min_temp_c, p.min_temp_c) AS min_temp_c,
                    o.grass_min_temp_c,
                    CAST(
                        CASE WHEN o.date IS NOT NULL THEN 'official' ELSE 'provisional' END
                        AS {SHORT_VARCHAR}
                    ) AS data_status,
                    COALESCE(o.source, p.source) AS source,
                    GREATEST(
                        COALESCE(o.updated_at, '-infinity'::timestamptz),
                        COALESCE(p.updated_at, '-infinity'::timestamptz)
                    ) AS updated_at
                FROM {OFFICIAL_DAILY_TABLE} o
                FULL OUTER JOIN {PROVISIONAL_DAILY_TABLE} p ON o.date = p.date
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


def recompute_provisional(conn, dates: set[date]) -> int:
    if not dates:
        return 0

    sql = f"""
        WITH rainfall_ranked AS (
            SELECT
                obs_date_hk,
                date_trunc('hour', obs_time) AS rain_hour,
                value,
                row_number() OVER (
                    PARTITION BY obs_date_hk, date_trunc('hour', obs_time)
                    ORDER BY obs_time DESC
                ) AS rn
            FROM {REALTIME_RAW_TABLE}
            WHERE obs_date_hk = %s
              AND station_code = 'HKO'
              AND metric = 'hourly_rainfall_mm'
              AND value IS NOT NULL
        ),
        rainfall_per_hour AS (
            SELECT obs_date_hk, rain_hour, value
            FROM rainfall_ranked
            WHERE rn = 1
        ),
        base AS (
            SELECT
                obs_date_hk AS date,
                avg(CASE WHEN metric = 'pressure_hpa' THEN value END) AS mslp_hpa,
                avg(CASE WHEN metric = 'temperature_c' THEN value END) AS mean_temp_c,
                avg(CASE WHEN metric = 'humidity_pct' THEN value END) AS mean_relative_humidity_pct,
                max(CASE WHEN metric = 'max_temp_since_midnight_c' THEN value END) AS max_temp_c,
                min(CASE WHEN metric = 'min_temp_since_midnight_c' THEN value END) AS min_temp_c,
                count(CASE WHEN metric = 'temperature_c' AND value IS NOT NULL THEN 1 END) AS sample_count_temp,
                count(CASE WHEN metric = 'humidity_pct' AND value IS NOT NULL THEN 1 END) AS sample_count_humidity,
                count(CASE WHEN metric = 'pressure_hpa' AND value IS NOT NULL THEN 1 END) AS sample_count_pressure,
                min(obs_time) AS first_obs_time,
                max(obs_time) AS last_obs_time
            FROM {REALTIME_RAW_TABLE}
            WHERE obs_date_hk = %s
              AND station_code = 'HKO'
            GROUP BY obs_date_hk
        ),
        rain AS (
            SELECT
                obs_date_hk AS date,
                sum(value) AS total_rainfall_mm,
                count(*) AS sample_count_rainfall
            FROM rainfall_per_hour
            GROUP BY obs_date_hk
        )
        INSERT INTO {PROVISIONAL_DAILY_TABLE} (
            date,
            station_code,
            station_name,
            mslp_hpa,
            mean_temp_c,
            mean_relative_humidity_pct,
            total_rainfall_mm,
            max_temp_c,
            min_temp_c,
            sample_count_temp,
            sample_count_humidity,
            sample_count_pressure,
            sample_count_rainfall,
            data_status,
            source,
            first_obs_time,
            last_obs_time,
            updated_at
        )
        SELECT
            base.date,
            'HKO',
            'Hong Kong Observatory',
            base.mslp_hpa,
            base.mean_temp_c,
            base.mean_relative_humidity_pct,
            rain.total_rainfall_mm,
            base.max_temp_c,
            base.min_temp_c,
            base.sample_count_temp,
            base.sample_count_humidity,
            base.sample_count_pressure,
            COALESCE(rain.sample_count_rainfall, 0),
            'provisional',
            'hko_realtime',
            base.first_obs_time,
            base.last_obs_time,
            now()
        FROM base
        LEFT JOIN rain ON rain.date = base.date
    """
    with conn.cursor() as cur:
        for obs_date in sorted(dates):
            cur.execute(f"DELETE FROM {PROVISIONAL_DAILY_TABLE} WHERE date = %s", (obs_date,))
            cur.execute(sql, (obs_date, obs_date))
    conn.commit()
    return len(dates)


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
    parser = argparse.ArgumentParser(description="Upsert HKO realtime/provisional data into PostgreSQL.")
    parser.add_argument("--database-url", default=database_url_from_env())
    parser.add_argument("--mode", choices=["current", "archive", "both", "recompute-only"], default="current")
    parser.add_argument("--start-date", type=parse_iso_date, help="Archive/recompute start date.")
    parser.add_argument("--end-date", type=parse_iso_date, help="Archive/recompute end date.")
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

        if args.mode == "recompute-only":
            affected_dates = {start_date + timedelta(days=offset) for offset in range((end_date - start_date).days + 1)}
        else:
            affected_dates = {obs.obs_date_hk for obs in current_observations + archive_observations}
            if archive_observations:
                replaced, groups = replace_archive_observations(conn, archive_observations)
                print(
                    f"Replaced {replaced:,} archive observations across {groups:,} date/source/metric groups",
                    file=sys.stderr,
                )
            if current_observations:
                upserted = upsert_observations(conn, current_observations)
                print(f"Upserted {upserted:,} current observations", file=sys.stderr)

        recomputed = recompute_provisional(conn, affected_dates)
        print(f"Recomputed {recomputed:,} provisional daily rows", file=sys.stderr)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
