# DEV ZONE Runbook

This runbook starts from an empty PostgreSQL database and builds the final HKO daily dataset.

## 1. Install

```bash
git clone https://github.com/HUANGYming/hongkong-weather.git
cd hongkong-weather

# Install uv first if needed:
# curl -LsSf https://astral.sh/uv/install.sh | sh
uv sync --locked
```

## 2. Configure

Create the local `.env` file:

```bash
cp .env.example .env
vi .env
mkdir -p logs
```

Set the real `DB_PASS` in `.env`. Do not commit `.env`.

For DEV ZONE, the values are:

```text
DB_HOST=codppybkdbd01.melco-resorts.com
DB_PORT=5432
DB_NAME=bigdata_prod
DB_USER=1018195
HKO_DB_SCHEMA=generic_sma_ai_shared
HKO_PROJECT_DIR=/opt/llm/hongkong-weather
```

The Python scripts and Airflow tasks load `.env` automatically from the project root.
Yellowbrick does not support PostgreSQL `TEXT` columns, so the project DDL uses bounded `varchar` columns.
Yellowbrick in DEV ZONE does not support PostgreSQL-style `CREATE INDEX`, so the project DDL does not create secondary indexes.

## 3. Run Offline Tests

```bash
uv run python -m unittest discover -s tests -v
```

## 4. Load Official HKO D1 Data

This loads official monthly historical daily data from `2020-01-01` onward.

```bash
uv run python update_hko_postgres.py --full-refresh
```

Expected official data currently lags behind realtime data because HKO D1 is monthly.

## 5. Backfill Missing Provisional Days

For the current known gap:

```bash
uv run python update_hko_realtime_postgres.py \
  --mode archive \
  --start-date 2026-07-01 \
  --end-date 2026-08-09
```

This uses DATA.GOV.HK Historical Archive to replay HKO 10-minute snapshots and aggregate daily provisional rows.

Fields covered by archive backfill:

```text
mean_temp_c
max_temp_c
min_temp_c
mslp_hpa
mean_relative_humidity_pct
```

Rainfall is not reliably available from the same archive route. Start collecting rainfall from live hourly observations and let D1 official data fill final rainfall once published.

## 6. Start Current Realtime Updates

Run once manually:

```bash
uv run python update_hko_realtime_postgres.py --mode current --include-rainfall
```

Then query:

```sql
SELECT
    date,
    data_status,
    mean_temp_c,
    max_temp_c,
    min_temp_c,
    mslp_hpa,
    mean_relative_humidity_pct,
    total_rainfall_mm
FROM fact_feature_date_hkweather_1day_daily_v1
ORDER BY date DESC
LIMIT 20;
```

If you use the DEV ZONE shared schema directly:

```sql
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
```

## 7. Cron

```cron
# Current provisional snapshots, every 10 minutes
*/10 * * * * cd /path/to/hongkong-weather && uv run python update_hko_realtime_postgres.py --mode current --include-rainfall >> logs/hko_realtime.log 2>&1

# Historical archive backfill for yesterday/recent corrections, daily
25 1 * * * cd /path/to/hongkong-weather && uv run python update_hko_realtime_postgres.py --mode archive --archive-lookback-days 14 >> logs/hko_archive.log 2>&1

# Official D1 checker, daily
15 8 * * * cd /path/to/hongkong-weather && uv run python update_hko_postgres.py >> logs/hko_official.log 2>&1
```

Airflow alternative:

```text
AIRFLOW_RUNBOOK.md
dags/hko_weather_airflow.py
```

## 8. Main Objects

```text
fact_feature_date_hkweather_1day_daily_v1                Official-first daily wide view for business queries
fact_feature_date_hkweather_official_1day_daily_v1       Official monthly HKO D1 daily rows
fact_feature_date_hkweather_provisional_1day_daily_v1    Provisional daily aggregates
ods_feature_observation_hkweather_10min_realtime_v1      Raw realtime/archive snapshots
meta_feature_run_hkweather_ingest_1run_event_v1          Ingest run log
```

In DEV ZONE these objects are created under:

```text
generic_sma_ai_shared
```

## 9. Cleanup

Raw realtime snapshots are only needed for recent recomputation. Keep 60 days by default:

```bash
uv run python cleanup_hko_realtime_raw.py --retention-days 60
```

Cron:

```cron
40 2 * * * cd /path/to/hongkong-weather && uv run python cleanup_hko_realtime_raw.py --retention-days 60 >> logs/hko_cleanup.log 2>&1
```
