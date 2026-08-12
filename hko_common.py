"""Shared HKO parsing and schema helpers."""

from __future__ import annotations

import csv
import io
import json
import os
import re
import zipfile
from dataclasses import dataclass
from datetime import date, datetime
from zoneinfo import ZoneInfo


HK_TZ = ZoneInfo("Asia/Hong_Kong")
STATION_CODE = "HKO"
STATION_NAME = "Hong Kong Observatory"
REALTIME_CSV_STATION_NAME = "HK Observatory"
RAINFALL_STATION_ID = "RF023"

# Naming convention:
# [department]_[project]_[OK_entity]_[data_field]_[window_size]_[frequency]_[version]
OFFICIAL_DAILY_TABLE = "fact_feature_date_hkweather_official_1day_daily_v1"
PROVISIONAL_DAILY_TABLE = "fact_feature_date_hkweather_provisional_1day_daily_v1"
LATEST_DAILY_VIEW = "fact_feature_date_hkweather_1day_daily_v1"
REALTIME_RAW_TABLE = "ods_feature_observation_hkweather_10min_realtime_v1"
INGEST_RUN_TABLE = "meta_feature_run_hkweather_ingest_1run_event_v1"
SCHEMA_LOCK_KEY = "hko_weather_schema_v1"

DB_SCHEMA = os.environ.get("HKO_DB_SCHEMA") or os.environ.get("DB_SCHEMA") or os.environ.get("SCHEMA")
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def quote_identifier(identifier: str) -> str:
    if not _IDENTIFIER_RE.fullmatch(identifier):
        raise ValueError(f"Unsafe SQL identifier: {identifier!r}")
    return f'"{identifier}"'


def qualify_relation(name: str) -> str:
    quoted_name = quote_identifier(name)
    if DB_SCHEMA:
        return f"{quote_identifier(DB_SCHEMA)}.{quoted_name}"
    return quoted_name


def ensure_database_schema(cur) -> None:
    if DB_SCHEMA:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {quote_identifier(DB_SCHEMA)}")


OFFICIAL_DAILY_TABLE = qualify_relation(OFFICIAL_DAILY_TABLE)
PROVISIONAL_DAILY_TABLE = qualify_relation(PROVISIONAL_DAILY_TABLE)
LATEST_DAILY_VIEW = qualify_relation(LATEST_DAILY_VIEW)
REALTIME_RAW_TABLE = qualify_relation(REALTIME_RAW_TABLE)
INGEST_RUN_TABLE = qualify_relation(INGEST_RUN_TABLE)


@dataclass(frozen=True)
class Element:
    code: str
    name: str
    unit: str
    column: str


@dataclass(frozen=True)
class Observation:
    obs_time: datetime
    obs_date_hk: date
    source: str
    metric: str
    value: float | None
    raw_value: str | None
    unit: str


HKO_ELEMENTS: dict[str, Element] = {
    "MSLP": Element("MSLP", "Daily Mean Pressure", "hPa", "mslp_hpa"),
    "TEMP": Element("TEMP", "Daily Mean Temperature", "deg_c", "mean_temp_c"),
    "DEW": Element("DEW", "Daily Mean Dew Point Temperature", "deg_c", "mean_dew_point_c"),
    "WET": Element("WET", "Daily Mean Wet-Bulb Temperature", "deg_c", "mean_wet_bulb_c"),
    "RH": Element("RH", "Daily Mean Relative Humidity", "percent", "mean_relative_humidity_pct"),
    "CLD": Element("CLD", "Daily Mean Amount of Cloud", "percent", "mean_cloud_amount_pct"),
    "RF": Element("RF", "Daily Total Rainfall", "mm", "total_rainfall_mm"),
    "MAXT": Element("MAXT", "Daily Maximum Temperature", "deg_c", "max_temp_c"),
    "MINT": Element("MINT", "Daily Minimum Temperature", "deg_c", "min_temp_c"),
    "GMT": Element("GMT", "Daily Grass Minimum Temperature", "deg_c", "grass_min_temp_c"),
}


REALTIME_CSV_RESOURCES = {
    "temperature": {
        "url": "https://data.weather.gov.hk/weatherAPI/hko_data/regional-weather/latest_1min_temperature.csv",
        "source": "latest_1min_temperature",
        "columns": {
            "Air Temperature(degree Celsius)": ("temperature_c", "deg_c"),
        },
    },
    "maxmin": {
        "url": "https://data.weather.gov.hk/weatherAPI/hko_data/regional-weather/latest_since_midnight_maxmin.csv",
        "source": "latest_since_midnight_maxmin",
        "columns": {
            "Maximum Air Temperature Since Midnight(degree Celsius)": ("max_temp_since_midnight_c", "deg_c"),
            "Minimum Air Temperature Since Midnight(degree Celsius)": ("min_temp_since_midnight_c", "deg_c"),
        },
    },
    "humidity": {
        "url": "https://data.weather.gov.hk/weatherAPI/hko_data/regional-weather/latest_1min_humidity.csv",
        "source": "latest_1min_humidity",
        "columns": {
            "Relative Humidity(percent)": ("humidity_pct", "percent"),
        },
    },
    "pressure": {
        "url": "https://data.weather.gov.hk/weatherAPI/hko_data/regional-weather/latest_1min_pressure.csv",
        "source": "latest_1min_pressure",
        "columns": {
            "Mean Sea Level Pressure(hPa)": ("pressure_hpa", "hPa"),
        },
    },
}

HOURLY_RAINFALL_URL = "https://data.weather.gov.hk/weatherAPI/opendata/hourlyRainfall.php?lang=en"


def normalize_d1_value(value: str) -> str:
    value = value.strip()
    if value == "Trace":
        return "0.025"
    if value in {"***", "---", "N/A", ""}:
        return ""
    return value


def value_to_float(value: str | None) -> float | None:
    if value is None:
        return None
    value = value.strip()
    if value in {"", "***", "---", "N/A", "M"}:
        return None
    if value == "Trace":
        return 0.025
    return float(value)


def parse_hko_minute(value: str) -> datetime:
    return datetime.strptime(value.strip(), "%Y%m%d%H%M").replace(tzinfo=HK_TZ)


def parse_hko_iso(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.strip())
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=HK_TZ)
    return parsed.astimezone(HK_TZ)


def hk_today() -> date:
    return datetime.now(HK_TZ).date()


def parse_d1_csv_text(text: str, element: Element) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    reader = csv.reader(io.StringIO(text.lstrip("\ufeff")))
    for raw in reader:
        if len(raw) < 4 or not re.fullmatch(r"\d{4}", raw[0].strip()):
            continue

        year, month, day = (int(raw[0]), int(raw[1]), int(raw[2]))
        observed_date = date(year, month, day)
        raw_value = raw[3].strip()
        completeness = raw[4].strip() if len(raw) > 4 else None
        rows.append(
            {
                "date": observed_date,
                "element_code": element.code,
                "column": element.column,
                "value": value_to_float(normalize_d1_value(raw_value)),
                "raw_value": raw_value or None,
                "completeness": completeness or None,
            }
        )
    return rows


def build_official_wide_rows(
    long_rows: list[dict[str, object]],
    start_date: date,
    end_date: date,
) -> list[dict[str, object]]:
    by_date: dict[date, dict[str, object]] = {}
    for row in long_rows:
        observed_date = row["date"]
        if not isinstance(observed_date, date) or observed_date < start_date or observed_date > end_date:
            continue
        item = by_date.setdefault(
            observed_date,
            {
                "date": observed_date,
                "station_code": STATION_CODE,
                "station_name": STATION_NAME,
            },
        )
        column = str(row["column"])
        item[column] = row["value"]
        item[f"{column}_raw"] = row["raw_value"]
        item[f"{column}_completeness"] = row["completeness"]

    for item in by_date.values():
        for element in HKO_ELEMENTS.values():
            item.setdefault(element.column, None)
            item.setdefault(f"{element.column}_raw", None)
            item.setdefault(f"{element.column}_completeness", None)

    return [by_date[key] for key in sorted(by_date)]


def parse_realtime_csv_text(text: str, resource_key: str) -> list[Observation]:
    config = REALTIME_CSV_RESOURCES[resource_key]
    reader = csv.DictReader(io.StringIO(text.lstrip("\ufeff")))
    observations: list[Observation] = []
    for row in reader:
        station = (row.get("Automatic Weather Station") or "").strip()
        if station != REALTIME_CSV_STATION_NAME:
            continue

        obs_time = parse_hko_minute(row["Date time"])
        for column, (metric, unit) in config["columns"].items():
            raw_value = (row.get(column) or "").strip()
            observations.append(
                Observation(
                    obs_time=obs_time,
                    obs_date_hk=obs_time.date(),
                    source=str(config["source"]),
                    metric=metric,
                    value=value_to_float(raw_value),
                    raw_value=raw_value or None,
                    unit=unit,
                )
            )
    return observations


def parse_hourly_rainfall_json(text: str) -> list[Observation]:
    payload = json.loads(text)
    obs_time = parse_hko_iso(payload["obsTime"])
    observations: list[Observation] = []
    for row in payload.get("hourlyRainfall", []):
        if row.get("automaticWeatherStationID") != RAINFALL_STATION_ID:
            continue
        raw_value = str(row.get("value", "")).strip()
        observations.append(
            Observation(
                obs_time=obs_time,
                obs_date_hk=obs_time.date(),
                source="hourlyRainfall",
                metric="hourly_rainfall_mm",
                value=value_to_float(raw_value),
                raw_value=raw_value or None,
                unit=str(row.get("unit") or "mm"),
            )
        )
    return observations


def parse_realtime_archive_zip(zip_bytes: bytes, resource_key: str) -> list[Observation]:
    observations: list[Observation] = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
        for name in archive.namelist():
            if name.endswith("/") or not name.lower().endswith(".csv"):
                continue
            text = archive.read(name).decode("utf-8-sig")
            observations.extend(parse_realtime_csv_text(text, resource_key))
    return observations


def official_value_columns() -> list[str]:
    columns: list[str] = []
    for element in HKO_ELEMENTS.values():
        columns.extend([element.column, f"{element.column}_raw", f"{element.column}_completeness"])
    return columns


def official_insert_columns() -> list[str]:
    return ["date", "station_code", "station_name", *official_value_columns()]
