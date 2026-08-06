#!/usr/bin/env python3
"""Download historical daily climate data for HKO's Hong Kong Observatory station.

The Hong Kong Observatory daily download UI calls a public CSV endpoint:
https://data.weather.gov.hk/weatherAPI/D1/caller.php?stn=HKO&ele=TEMP&yr=ALL

This scraper intentionally keeps station fixed to HKO and excludes HKO page
elements whose download UI maps to King's Park, Waglan Island, North Point,
Hong Kong International Airport, or all-Hong-Kong aggregate datasets.
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


BASE_URL = "https://data.weather.gov.hk/weatherAPI/D1/caller.php"
STATION_CODE = "HKO"
STATION_NAME = "Hong Kong Observatory"
REQUEST_TIMEOUT_SECONDS = 60


@dataclass(frozen=True)
class Element:
    code: str
    slug: str
    name: str
    unit: str
    column: str


HKO_ELEMENTS: dict[str, Element] = {
    "MSLP": Element("MSLP", "mslp", "Daily Mean Pressure", "hPa", "mslp_hpa"),
    "TEMP": Element("TEMP", "temp", "Daily Mean Temperature", "deg_c", "mean_temp_c"),
    "DEW": Element("DEW", "dew", "Daily Mean Dew Point Temperature", "deg_c", "mean_dew_point_c"),
    "WET": Element("WET", "wet", "Daily Mean Wet-Bulb Temperature", "deg_c", "mean_wet_bulb_c"),
    "RH": Element("RH", "rh", "Daily Mean Relative Humidity", "percent", "mean_relative_humidity_pct"),
    "CLD": Element("CLD", "cld", "Daily Mean Amount of Cloud", "percent", "mean_cloud_amount_pct"),
    "RF": Element("RF", "rf", "Daily Total Rainfall", "mm", "total_rainfall_mm"),
    "MAXT": Element("MAXT", "maxt", "Daily Maximum Temperature", "deg_c", "max_temp_c"),
    "MINT": Element("MINT", "mint", "Daily Minimum Temperature", "deg_c", "min_temp_c"),
    "GMT": Element("GMT", "gmt", "Daily Grass Minimum Temperature", "deg_c", "grass_min_temp_c"),
}


def build_url(element_code: str, year: str) -> str:
    return f"{BASE_URL}?{urlencode({'stn': STATION_CODE, 'ele': element_code, 'yr': year})}"


def fetch_text(url: str, retries: int, delay: float) -> str:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            request = Request(
                url,
                headers={
                    "User-Agent": "hko-history-scraper/1.0 (+https://www.hko.gov.hk/)",
                    "Accept": "text/csv,*/*",
                },
            )
            with urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                return response.read().decode("utf-8-sig")
        except (HTTPError, URLError, TimeoutError) as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(delay * attempt)
    raise RuntimeError(f"failed to fetch {url}: {last_error}") from last_error


def download_raw_csv(
    element: Element,
    year: str,
    raw_dir: Path,
    retries: int,
    delay: float,
    overwrite: bool,
) -> tuple[Path, str]:
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_path = raw_dir / f"daily_{STATION_CODE}_{element.code}_{year}.csv"
    url = build_url(element.code, year)

    if raw_path.exists() and not overwrite:
        return raw_path, url

    text = fetch_text(url, retries=retries, delay=delay)
    if text.startswith("File data not existed"):
        raise RuntimeError(f"HKO returned no data for {element.code} {year}")

    raw_path.write_text(text, encoding="utf-8")
    return raw_path, url


def normalize_value(value: str) -> str:
    value = value.strip()
    if value == "Trace":
        return "0.025"
    if value in {"***", "---", ""}:
        return ""
    return value


def parse_daily_csv(raw_path: Path, element: Element, source_url: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with raw_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        for raw in reader:
            if len(raw) < 4 or not re.fullmatch(r"\d{4}", raw[0].strip()):
                continue

            year, month, day = (int(raw[0]), int(raw[1]), int(raw[2]))
            observed_date = f"{year:04d}-{month:02d}-{day:02d}"
            try:
                date(year, month, day)
                date_is_valid = "true"
            except ValueError:
                date_is_valid = "false"
            raw_value = raw[3].strip()
            completeness = raw[4].strip() if len(raw) > 4 else ""

            rows.append(
                {
                    "date": observed_date,
                    "date_is_valid": date_is_valid,
                    "station_code": STATION_CODE,
                    "station_name": STATION_NAME,
                    "element_code": element.code,
                    "element_name": element.name,
                    "unit": element.unit,
                    "value": normalize_value(raw_value),
                    "raw_value": raw_value,
                    "completeness": completeness,
                    "source_url": source_url,
                }
            )
    return rows


def write_long_csv(path: Path, rows: Iterable[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "date",
        "date_is_valid",
        "station_code",
        "station_name",
        "element_code",
        "element_name",
        "unit",
        "value",
        "raw_value",
        "completeness",
        "source_url",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def write_wide_csv(path: Path, rows: Iterable[dict[str, str]], elements: list[Element]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    by_date: dict[str, dict[str, str]] = {}
    for row in rows:
        item = by_date.setdefault(
            row["date"],
            {
                "date": row["date"],
                "date_is_valid": row["date_is_valid"],
                "station_code": STATION_CODE,
                "station_name": STATION_NAME,
            },
        )
        element = HKO_ELEMENTS[row["element_code"]]
        item[element.column] = row["value"]
        item[f"{element.column}_raw"] = row["raw_value"]
        item[f"{element.column}_completeness"] = row["completeness"]

    fields = ["date", "date_is_valid", "station_code", "station_name"]
    for element in elements:
        fields.extend([element.column, f"{element.column}_raw", f"{element.column}_completeness"])

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for observed_date in sorted(by_date):
            writer.writerow(by_date[observed_date])


def parse_elements(value: str) -> list[Element]:
    if value.lower() == "all":
        return list(HKO_ELEMENTS.values())

    elements: list[Element] = []
    for code in re.split(r"[,\s]+", value.strip().upper()):
        if not code:
            continue
        if code not in HKO_ELEMENTS:
            valid = ", ".join(HKO_ELEMENTS)
            raise argparse.ArgumentTypeError(f"unknown element {code!r}; valid values: all, {valid}")
        elements.append(HKO_ELEMENTS[code])
    return elements


def parse_years(args: argparse.Namespace) -> list[str]:
    current_year = date.today().year
    if args.year:
        return [str(args.year).upper()]

    start_year = args.start_year or 2020
    end_year = args.end_year or current_year
    if start_year > end_year:
        raise SystemExit("--start-year cannot be greater than --end-year")

    return [str(year) for year in range(start_year, end_year + 1)]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Scrape daily historical climate data for HKO station only.",
    )
    parser.add_argument(
        "--elements",
        type=parse_elements,
        default=list(HKO_ELEMENTS.values()),
        help="Comma/space separated element codes, or 'all'. Defaults to all HKO-only elements.",
    )
    parser.add_argument(
        "--year",
        help="Single year or ALL. If omitted, downloads year-by-year from --start-year to --end-year.",
    )
    parser.add_argument("--start-year", type=int, default=2020, help="First year when downloading year-by-year.")
    parser.add_argument("--end-year", type=int, help="Last year when downloading year-by-year.")
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw"), help="Directory for raw HKO CSV files.")
    parser.add_argument("--long-csv", type=Path, default=Path("data/hko_daily_long.csv"))
    parser.add_argument("--wide-csv", type=Path, default=Path("data/hko_daily_wide.csv"))
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--delay", type=float, default=1.0, help="Base retry delay in seconds.")
    parser.add_argument("--sleep", type=float, default=0.2, help="Delay between successful requests.")
    parser.add_argument("--overwrite", action="store_true", help="Re-download raw files that already exist.")
    parser.add_argument("--no-wide", action="store_true", help="Skip the wide CSV output.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    elements: list[Element] = args.elements
    years = parse_years(args)

    all_rows: list[dict[str, str]] = []
    for element in elements:
        for year in years:
            print(f"Downloading {STATION_CODE} {element.code} {year}...", file=sys.stderr)
            raw_path, source_url = download_raw_csv(
                element=element,
                year=year,
                raw_dir=args.raw_dir,
                retries=args.retries,
                delay=args.delay,
                overwrite=args.overwrite,
            )
            all_rows.extend(parse_daily_csv(raw_path, element, source_url))
            time.sleep(args.sleep)

    all_rows.sort(key=lambda row: (row["date"], row["element_code"]))
    write_long_csv(args.long_csv, all_rows)
    if not args.no_wide:
        write_wide_csv(args.wide_csv, all_rows, elements)

    print(f"Wrote {len(all_rows):,} observations to {args.long_csv}", file=sys.stderr)
    if not args.no_wide:
        print(f"Wrote wide table to {args.wide_csv}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
