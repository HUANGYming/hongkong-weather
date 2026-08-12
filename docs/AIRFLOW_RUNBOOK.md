# Airflow Runbook

Airflow is a good fit for this project. Use it instead of cron when you want task history, retries, manual backfills, and a visible operations UI.

## What The DAGs Do

The DAG file is:

```text
dags/hko_weather_airflow.py
```

It defines four DAGs:

```text
hko_realtime_current            Every 10 minutes, current provisional observations
hko_daily_backfill_cleanup      Daily 01:25 HK time, archive replay then raw cleanup
hko_official_d1                 Daily 08:15 HK time, official D1/Daily Extract checker
hko_initial_backfill            Manual bootstrap DAG
```

## Worker Requirements

Every Airflow worker must have:

```text
uv
git checkout of this repo
project `.env` file or equivalent Airflow environment variables
```

Recommended `.env` values:

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

`HKO_PROJECT_DIR` is optional if the `dags/` folder lives inside this repository. Set it explicitly in production if Airflow does not resolve the DAG symlink back to the repository.

## Quick Deploy For `/opt/llm/airflow/dags`

```bash
cd /opt/llm/chrishuang
git clone https://github.com/HUANGYming/hongkong-weather.git
cd /opt/llm/chrishuang/hongkong-weather
uv sync --locked
cp .env.example .env
vi .env

ln -s /opt/llm/chrishuang/hongkong-weather/dags/hko_weather_airflow.py /opt/llm/airflow/dags/hko_weather_airflow.py
```

If the symlink already exists:

```bash
rm /opt/llm/airflow/dags/hko_weather_airflow.py
ln -s /opt/llm/chrishuang/hongkong-weather/dags/hko_weather_airflow.py /opt/llm/airflow/dags/hko_weather_airflow.py
```

## Install Project Dependencies

On each Airflow worker:

```bash
cd /opt/llm/chrishuang/hongkong-weather
uv sync --locked
```

## Deploy DAG

Option A: copy/symlink the project DAG file into Airflow's DAG folder:

```bash
ln -s /opt/llm/chrishuang/hongkong-weather/dags/hko_weather_airflow.py /opt/llm/airflow/dags/hko_weather_airflow.py
```

Option B: copy the DAG file directly:

```bash
cp /opt/llm/chrishuang/hongkong-weather/dags/hko_weather_airflow.py /opt/llm/airflow/dags/hko_weather_airflow.py
```

Option C: if Airflow already scans this repo's `dags/` directory, no symlink is needed.

## Bootstrap

Trigger this manual DAG first:

```text
hko_initial_backfill
```

It runs:

```text
1. uv run python update_hko_postgres.py --full-refresh
2. uv run python update_hko_realtime_postgres.py --mode archive --start-date 2026-07-01
3. uv run python update_hko_realtime_postgres.py --mode current --include-rainfall
```

After it succeeds, enable the scheduled DAGs:

```text
hko_realtime_current
hko_daily_backfill_cleanup
hko_official_d1
```

If you deployed an older version, pause or delete the retired DAG records:

```bash
airflow dags pause hko_realtime_archive_backfill || true
airflow dags pause hko_realtime_raw_cleanup || true
airflow dags delete hko_realtime_archive_backfill --yes || true
airflow dags delete hko_realtime_raw_cleanup --yes || true
```

## Validate

Run these SQL checks:

```sql
SELECT count(*), min(date), max(date)
FROM fact_feature_date_hkweather_official_1day_daily_v1;

SELECT count(*), min(date), max(date)
FROM fact_feature_date_hkweather_provisional_1day_daily_v1;

SELECT count(*), min(obs_date_hk), max(obs_date_hk)
FROM ods_feature_observation_hkweather_10min_realtime_v1;

SELECT
    date,
    data_status,
    mean_temp_c,
    max_temp_c,
    min_temp_c,
    total_rainfall_mm
FROM fact_feature_date_hkweather_1day_daily_v1
ORDER BY date DESC
LIMIT 20;
```

## Notes

- Business queries should use `fact_feature_date_hkweather_1day_daily_v1`.
- Official D1 rows win over provisional rows in the view.
- Raw realtime observations are retained for 60 days by default.
- Airflow should not run multiple active instances of the same DAG; each DAG sets `max_active_runs=1`.
- Table/view names follow `[department]_[project]_[OK_entity]_[data_field]_[window_size]_[frequency]_[version]`.
