# Qwen Postgres Implementation Guide

## Goal

Build a server-side updater for Hong Kong Observatory daily wide weather data.

Requirements:

- Source station: `HKO` only, meaning `Hong Kong Observatory`.
- Start date: `2020-01-01`.
- Output format: wide table, one row per date.
- Database: PostgreSQL by default.
- Update behavior: run daily and upsert by `date`.
- Keep raw value and completeness flag for every weather element.

## Data Source

Use the HKO public CSV endpoint:

```text
https://data.weather.gov.hk/weatherAPI/D1/caller.php?stn=HKO&ele=<ELEMENT>&yr=<YEAR>
```

Example:

```text
https://data.weather.gov.hk/weatherAPI/D1/caller.php?stn=HKO&ele=TEMP&yr=2025
```

Important:

- Always keep `stn=HKO`.
- Do not use HKO page elements that map to other stations such as King's Park, Waglan Island, North Point, Hong Kong International Airport, or Hong Kong-wide lightning aggregates.

## Elements To Fetch

Only fetch these HKO station elements:

```python
HKO_ELEMENTS = {
    "MSLP": "mslp_hpa",
    "TEMP": "mean_temp_c",
    "DEW": "mean_dew_point_c",
    "WET": "mean_wet_bulb_c",
    "RH": "mean_relative_humidity_pct",
    "CLD": "mean_cloud_amount_pct",
    "RF": "total_rainfall_mm",
    "MAXT": "max_temp_c",
    "MINT": "min_temp_c",
    "GMT": "grass_min_temp_c",
}
```

For each element, store:

```text
<column>
<column>_raw
<column>_completeness
```

Example:

```text
mean_temp_c
mean_temp_c_raw
mean_temp_c_completeness
```

## CSV Parsing Rules

HKO CSV files contain two title lines, then a header line, then data rows.

Typical rows:

```csv
年/Year,月/Month,日/Day,數值/Value,數據完整性/data Completeness
2025,1,1,17.8,C
2025,1,2,19.1,C
```

Parsing rules:

- Use rows whose first column is a 4-digit year.
- Build date as `YYYY-MM-DD`.
- Keep only dates >= `2020-01-01`.
- Convert numeric values to `float`.
- Preserve original HKO value in `<column>_raw`.
- Preserve completeness flag in `<column>_completeness`.
- Convert rainfall `Trace` to numeric `0.025`, but keep raw value as `Trace`.
- Convert `***`, `---`, and blank numeric values to SQL `NULL`.
- Completeness flag `C` means complete.
- Completeness flag `#` means incomplete.

## PostgreSQL Schema

Use this table:

```sql
CREATE TABLE IF NOT EXISTS hko_daily_weather_wide (
    date date PRIMARY KEY,
    station_code text NOT NULL DEFAULT 'HKO',
    station_name text NOT NULL DEFAULT 'Hong Kong Observatory',

    mslp_hpa double precision,
    mslp_hpa_raw text,
    mslp_hpa_completeness text,

    mean_temp_c double precision,
    mean_temp_c_raw text,
    mean_temp_c_completeness text,

    mean_dew_point_c double precision,
    mean_dew_point_c_raw text,
    mean_dew_point_c_completeness text,

    mean_wet_bulb_c double precision,
    mean_wet_bulb_c_raw text,
    mean_wet_bulb_c_completeness text,

    mean_relative_humidity_pct double precision,
    mean_relative_humidity_pct_raw text,
    mean_relative_humidity_pct_completeness text,

    mean_cloud_amount_pct double precision,
    mean_cloud_amount_pct_raw text,
    mean_cloud_amount_pct_completeness text,

    total_rainfall_mm double precision,
    total_rainfall_mm_raw text,
    total_rainfall_mm_completeness text,

    max_temp_c double precision,
    max_temp_c_raw text,
    max_temp_c_completeness text,

    min_temp_c double precision,
    min_temp_c_raw text,
    min_temp_c_completeness text,

    grass_min_temp_c double precision,
    grass_min_temp_c_raw text,
    grass_min_temp_c_completeness text,

    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS hko_daily_weather_wide_station_date_idx
ON hko_daily_weather_wide (station_code, date);
```

Optional ingest log table:

```sql
CREATE TABLE IF NOT EXISTS hko_ingest_runs (
    id bigserial PRIMARY KEY,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    status text NOT NULL,
    years text NOT NULL,
    rows_upserted integer,
    min_date date,
    max_date date,
    error text
);
```

## Upsert Strategy

Use `INSERT ... ON CONFLICT (date) DO UPDATE`.

Example pattern:

```sql
INSERT INTO hko_daily_weather_wide (
    date,
    station_code,
    station_name,
    mean_temp_c,
    mean_temp_c_raw,
    mean_temp_c_completeness,
    updated_at
)
VALUES (
    %(date)s,
    'HKO',
    'Hong Kong Observatory',
    %(mean_temp_c)s,
    %(mean_temp_c_raw)s,
    %(mean_temp_c_completeness)s,
    now()
)
ON CONFLICT (date) DO UPDATE SET
    station_code = EXCLUDED.station_code,
    station_name = EXCLUDED.station_name,
    mean_temp_c = EXCLUDED.mean_temp_c,
    mean_temp_c_raw = EXCLUDED.mean_temp_c_raw,
    mean_temp_c_completeness = EXCLUDED.mean_temp_c_completeness,
    updated_at = now();
```

In real code, include all weather columns in the insert and update list.

## Recommended Python Dependencies

Use either:

```bash
pip install requests psycopg[binary]
```

Or:

```bash
pip install requests psycopg2-binary
```

Prefer `psycopg` v3 if starting fresh.

## Environment Variables

Read Postgres connection info from environment:

```bash
export DATABASE_URL="postgresql://user:password@host:5432/dbname"
```

Python connection example:

```python
import os
import psycopg

conn = psycopg.connect(os.environ["DATABASE_URL"])
```

## Update Algorithm

Recommended behavior:

1. Ensure schema exists.
2. Find latest loaded date:

```sql
SELECT max(date) FROM hko_daily_weather_wide;
```

3. If no data exists, download years from `2020` through current year.
4. If data exists, refresh from `latest_date - 14 days` through current year.
5. For each year and each element, download CSV from HKO.
6. Parse rows into long records.
7. Pivot long records into wide rows by date.
8. Upsert wide rows into Postgres by `date`.
9. Record success or failure in `hko_ingest_runs`.

Why refresh the last 14 days:

- HKO data may lag or be revised.
- Daily update should catch late-published or corrected values.

## Daily Cron

Run once per day, for example 08:15 server local time:

```cron
15 8 * * * cd /path/to/hko-weather && /usr/bin/python3 update_hko_postgres.py >> logs/hko_update.log 2>&1
```

If the server timezone is UTC and you want Hong Kong morning time, adjust the cron schedule accordingly.

## Validation Queries

Check row count and date range:

```sql
SELECT count(*), min(date), max(date)
FROM hko_daily_weather_wide;
```

Check latest data:

```sql
SELECT
    date,
    mean_temp_c,
    max_temp_c,
    min_temp_c,
    total_rainfall_mm,
    total_rainfall_mm_raw
FROM hko_daily_weather_wide
ORDER BY date DESC
LIMIT 10;
```

Check station isolation:

```sql
SELECT station_code, count(*)
FROM hko_daily_weather_wide
GROUP BY station_code;
```

Expected result:

```text
HKO only
```

## Existing Local Files

Current project has:

```text
scrape_hko_history.py
update_hko_database.py
data/hko_daily_wide.csv
data/hko_daily_long.csv
```

The existing `update_hko_database.py` uses SQLite. For Postgres, keep the scraping/parsing logic but replace:

```text
sqlite3 connection
SQLite CREATE TABLE
SQLite upsert_rows
```

with:

```text
psycopg connection using DATABASE_URL
PostgreSQL CREATE TABLE
PostgreSQL INSERT ... ON CONFLICT (date) DO UPDATE
```

## Deliverables

Please produce:

```text
update_hko_postgres.py
requirements.txt
README server run section
```

The final server command should be:

```bash
DATABASE_URL="postgresql://user:password@host:5432/dbname" python3 update_hko_postgres.py
```

