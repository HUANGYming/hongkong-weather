# DEV ZONE Runbook

This runbook starts from DEV ZONE Yellowbrick and builds the final HKO daily dataset.

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

For DEV ZONE manual runs, `DB_PASS` is usually enough because the code has defaults for the other database settings:

```text
DB_PASS=replace_with_real_password
```

The Python scripts load `.env` automatically for manual/local runs. Airflow Docker mode does not read the project `.env`; non-secret DEV ZONE defaults are in the code, and `DB_PASS` comes from Airflow Admin Variable `hko_weather_db_password`.
Yellowbrick does not support PostgreSQL `TEXT` columns, so the project DDL uses bounded `varchar` columns.
Yellowbrick in DEV ZONE does not support PostgreSQL-style `CREATE INDEX`, so the project DDL does not create secondary indexes.
Yellowbrick tables can allow duplicate rows, so refresh writes use `DELETE` plus `INSERT` instead of PostgreSQL `ON CONFLICT`.

## 3. Run Offline Tests

```bash
uv run python -m unittest discover -s tests -v
```

## 4. Load Official HKO Daily Data

This loads official HKO historical daily data from `2020-01-01` onward. The loader uses D1 plus Daily Extract so recent published months can arrive before the D1 API catches up.

If you deployed an older version, check the old objects first:

```sql
SELECT table_schema, table_name, table_type
FROM information_schema.tables
WHERE table_schema = 'generic_sma_ai_shared'
  AND table_name IN (
      'fact_feature_date_hkweather_1day_daily_v1',
      'fact_feature_date_hkweather_official_1day_daily_v1',
      'fact_feature_date_hkweather_provisional_1day_daily_v1',
      'ods_feature_observation_hkweather_10min_realtime_v1',
      'fact_feature_observation_hkweather_10min_realtime_v1'
  )
ORDER BY table_name;
```

The new target `fact_feature_date_hkweather_1day_daily_v1` must be a base table. If it is still a view, the official loader will drop that view and recreate it as a table. If it is an old base table with missing columns, drop or rename that old table before rerunning.

The realtime observation table is now `fact_feature_observation_hkweather_10min_realtime_v1`. If an older `ods_feature_observation_hkweather_10min_realtime_v1` table exists, keep it as backup or drop/rename it after confirming the new fact table is populated.

```bash
uv run python update_hko_postgres.py --full-refresh
```

Expected official data can still lag by roughly one day because Daily Extract is published after observations are processed.

## 5. Optional Archive Raw Backfill

For the current known gap:

```bash
uv run python update_hko_realtime_postgres.py \
  --mode archive \
  --start-date 2026-07-01
```

This uses DATA.GOV.HK Historical Archive to replay HKO 10-minute snapshots into the raw observation table. It does not create daily business rows.

## 6. Start Current Realtime Updates

Run once manually:

```bash
uv run python update_hko_realtime_postgres.py --mode current --include-rainfall
```

Then query:

```sql
SELECT
    date,
    source,
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
    source,
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

Realtime observations are queried from the raw table by observation/update time:

```sql
SELECT
    obs_time,
    fetched_at,
    source,
    metric,
    value,
    unit
FROM generic_sma_ai_shared.fact_feature_observation_hkweather_10min_realtime_v1
ORDER BY obs_time DESC, source, metric
LIMIT 20;
```

## 7. Cron

```cron
# Current realtime raw observations, every 10 minutes
*/10 * * * * cd /path/to/hongkong-weather && uv run python update_hko_realtime_postgres.py --mode current --include-rainfall >> logs/hko_realtime.log 2>&1

# Official daily checker, daily
15 8 * * * cd /path/to/hongkong-weather && uv run python update_hko_postgres.py >> logs/hko_official.log 2>&1
```

Airflow alternative:

```text
AIRFLOW_RUNBOOK.md
dags/hko_weather_airflow.py
```

## 8. Main Objects

```text
fact_feature_date_hkweather_1day_daily_v1                Official HKO D1/Daily Extract daily table
fact_feature_observation_hkweather_10min_realtime_v1      Realtime/archive observations by obs_time
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
