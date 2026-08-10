# Airflow Runbook

Airflow is a good fit for this project. Use it instead of cron when you want task history, retries, manual backfills, and a visible operations UI.

## What The DAGs Do

The DAG file is:

```text
dags/hko_weather_airflow.py
```

It defines five DAGs:

```text
hko_realtime_current            Every 10 minutes, current provisional observations
hko_realtime_archive_backfill   Daily 01:25 HK time, recent historical archive replay
hko_official_d1                 Daily 08:15 HK time, official D1 checker
hko_realtime_raw_cleanup        Daily 02:40 HK time, raw table retention cleanup
hko_initial_backfill            Manual bootstrap DAG
```

## Worker Requirements

Every Airflow worker must have:

```text
uv
git checkout of this repo
DATABASE_URL environment variable
```

Recommended environment variables:

```bash
export DATABASE_URL="postgresql://user:password@host:5432/dbname"
export HKO_PROJECT_DIR="/opt/airflow/hongkong-weather"
export HKO_ARCHIVE_LOOKBACK_DAYS=14
export HKO_RAW_RETENTION_DAYS=60
```

`HKO_PROJECT_DIR` is optional if the `dags/` folder lives inside this repository. Set it explicitly in production.

## Install Project Dependencies

On each Airflow worker:

```bash
cd /opt/airflow/hongkong-weather
uv sync --locked
```

## Deploy DAG

Option A: copy/symlink the project DAG file into Airflow's DAG folder:

```bash
ln -s /opt/airflow/hongkong-weather/dags/hko_weather_airflow.py /opt/airflow/dags/hko_weather_airflow.py
```

Option B: if Airflow already scans this repo's `dags/` directory, no symlink is needed.

## Bootstrap

Trigger this manual DAG first:

```text
hko_initial_backfill
```

It runs:

```text
1. uv run python update_hko_postgres.py --full-refresh
2. uv run python update_hko_realtime_postgres.py --mode archive --start-date 2026-07-01 --end-date 2026-08-09
3. uv run python update_hko_realtime_postgres.py --mode current --include-rainfall
```

After it succeeds, enable the scheduled DAGs:

```text
hko_realtime_current
hko_realtime_archive_backfill
hko_official_d1
hko_realtime_raw_cleanup
```

## Validate

Run these SQL checks:

```sql
SELECT count(*), min(date), max(date)
FROM hko_daily_weather_official;

SELECT count(*), min(date), max(date)
FROM hko_daily_weather_provisional;

SELECT count(*), min(obs_date_hk), max(obs_date_hk)
FROM hko_realtime_observations;

SELECT
    date,
    data_status,
    mean_temp_c,
    max_temp_c,
    min_temp_c,
    total_rainfall_mm
FROM hko_daily_weather_latest_v
ORDER BY date DESC
LIMIT 20;
```

## Notes

- Business queries should use `hko_daily_weather_latest_v`.
- Official D1 rows win over provisional rows in the view.
- Raw realtime observations are retained for 60 days by default.
- Airflow should not run multiple active instances of the same DAG; each DAG sets `max_active_runs=1`.
