# Qwen Realtime Daily Data Guide

## Why This Is Needed

The HKO `D1` daily climate endpoint is not suitable for realtime daily updates. It is a monthly historical/climate feed and may lag behind today by weeks.

For daily realtime data, build a second ingestion layer using HKO provisional regional weather APIs.

Recommended model:

```text
official monthly history table  +  realtime provisional table/view
```

The historical table remains the source of truth after HKO publishes official monthly daily data. The realtime table fills today and recent missing days.

## Key Principle

Realtime data is provisional.

Do not mix it silently with official HKO monthly climate daily data. Either:

1. store it in a separate table, or
2. store it in the same wide table with a `data_status` column.

Recommended:

```text
hko_daily_weather_wide              official/monthly backfill
hko_realtime_observations           raw realtime samples
hko_daily_weather_realtime_wide     provisional daily aggregates
hko_daily_weather_latest_v          view combining official + provisional
```

## Official Realtime Sources

Use HKO/Data.gov.hk provisional regional weather feeds.

### 1-minute air temperature

Frequency: every 10 minutes.

```text
https://data.weather.gov.hk/weatherAPI/hko_data/regional-weather/latest_1min_temperature.csv
```

Station name in CSV:

```text
HK Observatory
```

Column:

```text
Air Temperature(degree Celsius)
```

### Maximum/minimum temperature since midnight

Frequency: every 10 minutes.

```text
https://data.weather.gov.hk/weatherAPI/hko_data/regional-weather/latest_since_midnight_maxmin.csv
```

Station name in CSV:

```text
HK Observatory
```

Columns:

```text
Maximum Air Temperature Since Midnight(degree Celsius)
Minimum Air Temperature Since Midnight(degree Celsius)
```

### 1-minute relative humidity

Frequency: every 10 minutes.

```text
https://data.weather.gov.hk/weatherAPI/hko_data/regional-weather/latest_1min_humidity.csv
```

Station name in CSV:

```text
HK Observatory
```

Column:

```text
Relative Humidity(percent)
```

### 1-minute sea-level pressure

Frequency: every 10 minutes.

```text
https://data.weather.gov.hk/weatherAPI/hko_data/regional-weather/latest_1min_pressure.csv
```

Station name in CSV:

```text
HK Observatory
```

Column:

```text
Mean Sea Level Pressure(hPa)
```

### Hourly rainfall

Frequency: hourly.

```text
https://data.weather.gov.hk/weatherAPI/opendata/hourlyRainfall.php?lang=en
```

Station name in JSON:

```text
Hong Kong Observatory
```

Station ID:

```text
RF023
```

Value meaning:

```text
rainfall during the 1-hour period ending at obsTime
```

Important: do not double-count rainfall. Store each hourly rainfall observation by `obs_time` and sum distinct hourly observations for each Hong Kong date.

## Fields Available In Realtime

Good realtime coverage:

```text
mean_temp_c                  derive by averaging sampled 1-minute temperatures
max_temp_c                   use latest_since_midnight_maxmin
min_temp_c                   use latest_since_midnight_maxmin
mean_relative_humidity_pct   derive by averaging sampled 1-minute RH
mslp_hpa                     derive by averaging sampled 1-minute pressure
total_rainfall_mm            sum hourlyRainfall values by day
```

Not directly equivalent in realtime:

```text
mean_dew_point_c
mean_wet_bulb_c
mean_cloud_amount_pct
grass_min_temp_c
```

Recommended handling:

- Keep these realtime fields `NULL` unless you add a clearly documented estimation method.
- When monthly official D1 data arrives, backfill these fields into the official table.

Do not estimate dew point or wet bulb unless downstream users accept computed values. If computed, add explicit columns such as:

```text
estimated_dew_point_c
estimated_wet_bulb_c
```

## PostgreSQL Schema

### Raw realtime observations

```sql
CREATE TABLE IF NOT EXISTS hko_realtime_observations (
    obs_time timestamptz NOT NULL,
    obs_date_hk date NOT NULL,
    station_code text NOT NULL DEFAULT 'HKO',
    station_name text NOT NULL DEFAULT 'Hong Kong Observatory',
    source text NOT NULL,
    metric text NOT NULL,
    value double precision,
    raw_value text,
    unit text,
    fetched_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (obs_time, source, metric, station_code)
);

CREATE INDEX IF NOT EXISTS hko_realtime_observations_date_idx
ON hko_realtime_observations (obs_date_hk, station_code);
```

Use metrics:

```text
temperature_c
humidity_pct
pressure_hpa
max_temp_since_midnight_c
min_temp_since_midnight_c
hourly_rainfall_mm
```

### Provisional daily wide table

```sql
CREATE TABLE IF NOT EXISTS hko_daily_weather_realtime_wide (
    date date PRIMARY KEY,
    station_code text NOT NULL DEFAULT 'HKO',
    station_name text NOT NULL DEFAULT 'Hong Kong Observatory',

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

    data_status text NOT NULL DEFAULT 'provisional',
    first_obs_time timestamptz,
    last_obs_time timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now()
);
```

### Latest daily view

This view prefers official monthly values when present and falls back to realtime provisional values.

```sql
CREATE OR REPLACE VIEW hko_daily_weather_latest_v AS
SELECT
    COALESCE(o.date, r.date) AS date,
    'HKO' AS station_code,
    'Hong Kong Observatory' AS station_name,

    COALESCE(o.mslp_hpa, r.mslp_hpa) AS mslp_hpa,
    COALESCE(o.mean_temp_c, r.mean_temp_c) AS mean_temp_c,
    o.mean_dew_point_c,
    o.mean_wet_bulb_c,
    COALESCE(o.mean_relative_humidity_pct, r.mean_relative_humidity_pct) AS mean_relative_humidity_pct,
    o.mean_cloud_amount_pct,
    COALESCE(o.total_rainfall_mm, r.total_rainfall_mm) AS total_rainfall_mm,
    COALESCE(o.max_temp_c, r.max_temp_c) AS max_temp_c,
    COALESCE(o.min_temp_c, r.min_temp_c) AS min_temp_c,
    o.grass_min_temp_c,

    CASE
        WHEN o.date IS NOT NULL THEN 'official'
        ELSE 'provisional'
    END AS data_status,

    GREATEST(o.updated_at, r.updated_at) AS updated_at
FROM hko_daily_weather_wide o
FULL OUTER JOIN hko_daily_weather_realtime_wide r
ON o.date = r.date;
```

If `hko_daily_weather_wide` does not have `updated_at`, add it or adjust the view.

## Ingestion Algorithm

Create a script:

```text
update_hko_realtime_postgres.py
```

Steps:

1. Connect to Postgres using `DATABASE_URL`.
2. Ensure realtime schemas exist.
3. Fetch these sources:

```text
latest_1min_temperature.csv
latest_since_midnight_maxmin.csv
latest_1min_humidity.csv
latest_1min_pressure.csv
hourlyRainfall.php?lang=en
```

4. Extract only HKO station:

```text
CSV station:  HK Observatory
JSON station: Hong Kong Observatory / RF023
```

5. Parse HKO datetime:

```text
202608061530 -> 2026-08-06 15:30 Asia/Hong_Kong
```

6. Insert raw observations with `ON CONFLICT DO UPDATE`.
7. Recompute provisional daily aggregates for the affected Hong Kong dates.
8. Upsert into `hko_daily_weather_realtime_wide`.

## Aggregation SQL

For a given date, recompute provisional wide data from raw observations:

```sql
INSERT INTO hko_daily_weather_realtime_wide (
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
    first_obs_time,
    last_obs_time,
    updated_at
)
SELECT
    obs_date_hk AS date,
    'HKO',
    'Hong Kong Observatory',
    avg(value) FILTER (WHERE metric = 'pressure_hpa') AS mslp_hpa,
    avg(value) FILTER (WHERE metric = 'temperature_c') AS mean_temp_c,
    avg(value) FILTER (WHERE metric = 'humidity_pct') AS mean_relative_humidity_pct,
    sum(value) FILTER (WHERE metric = 'hourly_rainfall_mm') AS total_rainfall_mm,
    max(value) FILTER (WHERE metric = 'max_temp_since_midnight_c') AS max_temp_c,
    min(value) FILTER (WHERE metric = 'min_temp_since_midnight_c') AS min_temp_c,
    count(*) FILTER (WHERE metric = 'temperature_c') AS sample_count_temp,
    count(*) FILTER (WHERE metric = 'humidity_pct') AS sample_count_humidity,
    count(*) FILTER (WHERE metric = 'pressure_hpa') AS sample_count_pressure,
    count(*) FILTER (WHERE metric = 'hourly_rainfall_mm') AS sample_count_rainfall,
    'provisional',
    min(obs_time),
    max(obs_time),
    now()
FROM hko_realtime_observations
WHERE obs_date_hk = %(date)s
  AND station_code = 'HKO'
GROUP BY obs_date_hk
ON CONFLICT (date) DO UPDATE SET
    mslp_hpa = EXCLUDED.mslp_hpa,
    mean_temp_c = EXCLUDED.mean_temp_c,
    mean_relative_humidity_pct = EXCLUDED.mean_relative_humidity_pct,
    total_rainfall_mm = EXCLUDED.total_rainfall_mm,
    max_temp_c = EXCLUDED.max_temp_c,
    min_temp_c = EXCLUDED.min_temp_c,
    sample_count_temp = EXCLUDED.sample_count_temp,
    sample_count_humidity = EXCLUDED.sample_count_humidity,
    sample_count_pressure = EXCLUDED.sample_count_pressure,
    sample_count_rainfall = EXCLUDED.sample_count_rainfall,
    first_obs_time = EXCLUDED.first_obs_time,
    last_obs_time = EXCLUDED.last_obs_time,
    updated_at = now();
```

## Cron Schedule

Run realtime ingestion every 10 minutes:

```cron
*/10 * * * * cd /path/to/hko-weather && DATABASE_URL="postgresql://user:password@host:5432/dbname" uv run python update_hko_realtime_postgres.py >> logs/hko_realtime.log 2>&1
```

Also keep the monthly/official updater:

```cron
15 8 * * * cd /path/to/hko-weather && DATABASE_URL="postgresql://user:password@host:5432/dbname" uv run python update_hko_postgres.py >> logs/hko_official.log 2>&1
```

## Validation Queries

Latest provisional rows:

```sql
SELECT *
FROM hko_daily_weather_realtime_wide
ORDER BY date DESC
LIMIT 5;
```

Combined latest daily data:

```sql
SELECT
    date,
    data_status,
    mean_temp_c,
    max_temp_c,
    min_temp_c,
    total_rainfall_mm,
    updated_at
FROM hko_daily_weather_latest_v
ORDER BY date DESC
LIMIT 10;
```

Raw sample coverage for today:

```sql
SELECT metric, count(*), min(obs_time), max(obs_time)
FROM hko_realtime_observations
WHERE obs_date_hk = (now() AT TIME ZONE 'Asia/Hong_Kong')::date
GROUP BY metric
ORDER BY metric;
```

## Expected Behavior

After this is implemented:

- Today appears in `hko_daily_weather_realtime_wide`.
- Today appears in `hko_daily_weather_latest_v` with `data_status = 'provisional'`.
- Historical monthly values appear with `data_status = 'official'`.
- When HKO monthly D1 data is released, the official table can overwrite provisional values for completed months.
