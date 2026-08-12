# Hong Kong Observatory Historical Data Scraper

This project downloads daily climate data from 2020-01-01 onward for the Hong Kong Observatory centre station only.

Source endpoint:

```text
https://data.weather.gov.hk/weatherAPI/D1/caller.php?stn=HKO&ele=<ELEMENT>&yr=<YEAR_OR_ALL>
```

The script keeps `stn=HKO` fixed. By default it excludes HKO download-page elements that are mapped to other stations such as King's Park, Waglan Island, North Point, Hong Kong International Airport, or all-Hong-Kong lightning aggregates.

## Run

```bash
python3 scrape_hko_history.py
```

Default output:

```text
data/raw/daily_HKO_<ELEMENT>_<YEAR>.csv
data/hko_daily_long.csv
data/hko_daily_wide.csv
```

The default run downloads year-by-year from `2020` through the current year.

## DEV ZONE Yellowbrick Runbook

Use the deployment branch:

```bash
cd /opt/llm/chrishuang
git clone https://github.com/HUANGYming/hongkong-weather.git
cd /opt/llm/chrishuang/hongkong-weather
git checkout codex/local-airflow-demo
git pull
uv sync --locked
```

Create the local `.env` file:

```bash
cp .env.example .env
vi .env
```

DEV ZONE values:

```dotenv
DB_HOST=codppybkdbd01.melco-resorts.com
DB_PORT=5432
DB_NAME=bigdata_prod
DB_USER=1018195
DB_PASS=replace_with_real_password
HKO_DB_SCHEMA=generic_sma_ai_shared

HKO_PROJECT_DIR=/opt/llm/chrishuang/hongkong-weather
HKO_ARCHIVE_LOOKBACK_DAYS=14
HKO_RAW_RETENTION_DAYS=60
```

Do not commit `.env`.

Run offline tests:

```bash
uv run python -m unittest discover -s tests -v
```

Smoke test the Yellowbrick connection and current realtime path:

```bash
uv run python update_hko_realtime_postgres.py --mode current --include-rainfall
```

Expected output:

```text
Upserted ... realtime observations
Recomputed ... provisional daily rows
```

Load official HKO D1 data from `2020-01-01` onward:

```bash
uv run python update_hko_postgres.py --full-refresh
```

Backfill provisional daily rows from DATA.GOV.HK historical archive:

```bash
uv run python update_hko_realtime_postgres.py \
  --mode archive \
  --start-date 2026-07-01 \
  --end-date 2026-08-12
```

Run current realtime again:

```bash
uv run python update_hko_realtime_postgres.py --mode current --include-rainfall
```

Verify the business-facing view:

```bash
source .env
export DATABASE_URL="postgresql://${DB_USER}:${DB_PASS}@${DB_HOST}:${DB_PORT}/${DB_NAME}"

psql "$DATABASE_URL" -c "
SELECT
    date,
    data_status,
    mean_temp_c,
    max_temp_c,
    min_temp_c,
    mslp_hpa,
    mean_relative_humidity_pct,
    total_rainfall_mm
FROM generic_sma_ai_shared.fact_feature_date_hkweather_1day_daily_v1
ORDER BY date DESC
LIMIT 20;
"
```

Yellowbrick notes:

```text
The project uses psycopg2.
The project does not create schemas.
The project does not create secondary indexes.
The project uses varchar columns instead of PostgreSQL text columns.
Refresh writes use DELETE plus INSERT instead of PostgreSQL ON CONFLICT.
```

Main tables/views:

```text
fact_feature_date_hkweather_1day_daily_v1                Official-first daily wide view for business queries
fact_feature_date_hkweather_official_1day_daily_v1       Official monthly HKO D1 daily rows
fact_feature_date_hkweather_provisional_1day_daily_v1    Provisional daily aggregates
ods_feature_observation_hkweather_10min_realtime_v1      Raw realtime/archive snapshots
meta_feature_run_hkweather_ingest_1run_event_v1          Ingest run log
```

Naming convention:

```text
[department]_[project]_[OK_entity]_[data_field]_[window_size]_[frequency]_[version]
```

The business-facing object intentionally uses the requested name:

```text
fact_feature_date_hkweather_1day_daily_v1
```

Retention policy:

```text
fact_feature_date_hkweather_official_1day_daily_v1      Keep forever
fact_feature_date_hkweather_provisional_1day_daily_v1   Keep unless old rows are already covered by official data
ods_feature_observation_hkweather_10min_realtime_v1     Keep recent raw snapshots only, default 60 days
```

Cleanup command:

```bash
uv run python cleanup_hko_realtime_raw.py --retention-days 60
```

Use this view for application queries:

```sql
SELECT *
FROM fact_feature_date_hkweather_1day_daily_v1
ORDER BY date DESC;
```

Production cron example:

```cron
# Current provisional snapshots, every 10 minutes
*/10 * * * * cd /path/to/hongkong-weather && uv run python update_hko_realtime_postgres.py --mode current --include-rainfall >> logs/hko_realtime.log 2>&1

# Historical archive backfill for yesterday/recent corrections, daily
25 1 * * * cd /path/to/hongkong-weather && uv run python update_hko_realtime_postgres.py --mode archive --archive-lookback-days 14 >> logs/hko_archive.log 2>&1

# Official D1 checker, daily
15 8 * * * cd /path/to/hongkong-weather && uv run python update_hko_postgres.py >> logs/hko_official.log 2>&1

# Raw realtime retention cleanup, daily
40 2 * * * cd /path/to/hongkong-weather && uv run python cleanup_hko_realtime_raw.py --retention-days 60 >> logs/hko_cleanup.log 2>&1
```

## Airflow

Airflow is supported for production orchestration. See:

```text
AIRFLOW_RUNBOOK.md
dags/hko_weather_airflow.py
```

The Airflow deployment includes separate DAGs for current realtime updates, historical archive replay, official D1 refresh, raw cleanup, and manual bootstrap.

## Tests

Offline parser tests:

```bash
uv run python -m unittest discover -s tests -v
```

## SQLite Wide Table

Create or update the SQLite database:

```bash
uv run python update_hko_database.py
```

Default database and table:

```text
data/hko_weather.sqlite
hko_daily_weather_wide
```

The table is wide format: one row per date, `date` as the primary key, and one set of value/raw/completeness columns per HKO element.

Example query:

```bash
sqlite3 data/hko_weather.sqlite "select date, mean_temp_c, total_rainfall_mm from hko_daily_weather_wide order by date desc limit 10;"
```

First run or forced rebuild:

```bash
uv run python update_hko_database.py --full-refresh
```

Daily update behavior:

```text
First run: loads 2020-01-01 through the latest available HKO date.
Later runs: refreshes recent/current-year data and upserts by date.
```

Each run is recorded in `hko_ingest_runs`.

## Realtime Daily Data

The HKO `D1` daily climate endpoint is monthly, so it may not include today or the latest completed month. For daily realtime/provisional data, use:

```text
QWEN_REALTIME_DAILY_GUIDE.md
```

That guide adds a realtime Postgres layer using HKO provisional regional weather APIs, then combines official monthly rows with provisional current rows.

## Daily macOS Schedule

The launchd template is:

```text
scripts/com.hko.daily-weather-update.plist
```

It runs `update_hko_database.py` every day at `08:15` local machine time. Because macOS LaunchAgents can be blocked from reading files under Desktop, the scheduled runtime lives in:

```text
/Users/YOUR_USERNAME/.hko_weather
```

Scheduled database and logs:

```text
/Users/YOUR_USERNAME/.hko_weather/data/hko_weather.sqlite
/Users/YOUR_USERNAME/.hko_weather/logs/hko_update.out.log
/Users/YOUR_USERNAME/.hko_weather/logs/hko_update.err.log
```

## Elements

Default HKO-only elements:

```text
MSLP  Daily Mean Pressure, hPa
TEMP  Daily Mean Temperature, deg C
DEW   Daily Mean Dew Point Temperature, deg C
WET   Daily Mean Wet-Bulb Temperature, deg C
RH    Daily Mean Relative Humidity, percent
CLD   Daily Mean Amount of Cloud, percent
RF    Daily Total Rainfall, mm
MAXT  Daily Maximum Temperature, deg C
MINT  Daily Minimum Temperature, deg C
GMT   Daily Grass Minimum Temperature, deg C
```

Download selected elements:

```bash
python3 scrape_hko_history.py --elements TEMP,RF,MAXT,MINT
```

Download a single year:

```bash
python3 scrape_hko_history.py --year 2025 --overwrite
```

Download year-by-year:

```bash
python3 scrape_hko_history.py --start-year 2020 --end-year 2025 --overwrite
```

Download full available history if needed:

```bash
python3 scrape_hko_history.py --year ALL --overwrite
```

Notes:

- `Trace` rainfall is normalized to `0.025` in the `value` column and preserved as `Trace` in `raw_value`.
- `***`, `---`, and blank values are normalized to blank in `value` and preserved in `raw_value`.
- Completeness flags are kept from HKO: `C` means complete, `#` means incomplete, blank usually means unavailable.
- `date_is_valid` marks calendar-valid dates. HKO `ALL` history includes a few official placeholder rows such as `1900-02-29`; these rows are kept.
