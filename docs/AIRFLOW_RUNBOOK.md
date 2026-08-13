# Airflow Runbook

Airflow is a good fit for this project. Use it instead of cron when you want task history, retries, manual backfills, and a visible operations UI.

## What The DAGs Do

The DAG file is:

```text
dags/hko_weather_airflow.py
```

It defines four DAGs:

```text
hko_weather_realtime_ingest          Every 10 minutes, current realtime observations
hko_weather_realtime_cleanup         Daily 01:25 HK time, raw observation cleanup
hko_weather_official_daily_ingest    Daily 08:15 HK time, official D1/Daily Extract
hko_weather_bootstrap                Manual bootstrap DAG
```

## Worker Requirements

Default Docker mode requires every Airflow worker to have:

```text
Docker CLI access
hongkong-weather:latest image available on the worker host
Airflow Admin Variable hko_weather_db_password
```

Create this Airflow Admin Variable:

```text
key:   hko_weather_db_password
value: replace_with_real_password
```

The code contains the DEV ZONE defaults for `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `HKO_DB_SCHEMA`, and `HKO_RAW_RETENTION_DAYS`. The DAG does not read the project `.env` in Docker mode. `HKO_PROJECT_DIR` is only needed for local-uv mode.

## Docker Runner

Docker mode avoids most project-directory and Python-environment permission issues. It is the default Airflow execution mode. Build the image on the Airflow worker host:

```bash
cd /opt/llm/chrishuang/hongkong-weather
docker image build --tag hongkong-weather:latest .
```

The only required runtime secret is the Airflow Admin Variable:

```text
hko_weather_db_password
```

Optional Airflow worker environment values:

```dotenv
HKO_DOCKER_IMAGE=hongkong-weather:latest
HKO_DOCKER_BIN=docker
HKO_DB_PASS_VARIABLE=hko_weather_db_password
# HKO_DOCKER_RUN_ARGS=--network host
```

In Docker mode, each task runs like this:

```bash
docker run --rm -e DB_PASS \
  hongkong-weather:latest update_hko_realtime_postgres.py --mode current --include-rainfall
```

Airflow workers must be able to run Docker. If Airflow itself runs in Docker Compose, mount the Docker socket and make sure the worker image has a Docker CLI:

```text
/var/run/docker.sock:/var/run/docker.sock
```

Test from inside the Airflow worker container:

```bash
docker run --rm -e DB_PASS \
  hongkong-weather:latest update_hko_realtime_postgres.py --mode current --include-rainfall
```

To temporarily switch back to local-uv mode, set `HKO_EXECUTION_MODE=uv`. In uv mode every Airflow worker also needs `uv` and a working checkout of this repo.

## Quick Deploy For `/opt/llm/airflow/dags`

```bash
cd /opt/llm/chrishuang
git clone https://github.com/HUANGYming/hongkong-weather.git
cd /opt/llm/chrishuang/hongkong-weather
docker image build --tag hongkong-weather:latest .

ln -s /opt/llm/chrishuang/hongkong-weather/dags/hko_weather_airflow.py /opt/llm/airflow/dags/hko_weather_airflow.py
```

If the symlink already exists:

```bash
rm /opt/llm/airflow/dags/hko_weather_airflow.py
ln -s /opt/llm/chrishuang/hongkong-weather/dags/hko_weather_airflow.py /opt/llm/airflow/dags/hko_weather_airflow.py
```

## Install Project Dependencies

Only needed for local-uv mode. On each Airflow worker:

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
hko_weather_bootstrap
```

It runs:

```text
1. uv run python update_hko_postgres.py --full-refresh
2. uv run python update_hko_realtime_postgres.py --mode current --include-rainfall
```

After it succeeds, enable the scheduled DAGs:

```text
hko_weather_realtime_ingest
hko_weather_realtime_cleanup
hko_weather_official_daily_ingest
```

If you deployed an older version, pause or delete the retired DAG records:

```bash
airflow dags pause hko_realtime_current || true
airflow dags pause hko_daily_backfill_cleanup || true
airflow dags pause hko_official_d1 || true
airflow dags pause hko_initial_backfill || true
airflow dags pause hko_realtime_archive_backfill || true
airflow dags pause hko_realtime_raw_cleanup || true

airflow dags delete hko_realtime_current --yes || true
airflow dags delete hko_daily_backfill_cleanup --yes || true
airflow dags delete hko_official_d1 --yes || true
airflow dags delete hko_initial_backfill --yes || true
airflow dags delete hko_realtime_archive_backfill --yes || true
airflow dags delete hko_realtime_raw_cleanup --yes || true
```

## Validate

Run these SQL checks:

```sql
SELECT count(*), min(date), max(date)
FROM fact_feature_date_hkweather_1day_daily_v1;

SELECT count(*), min(obs_date_hk), max(obs_date_hk)
FROM fact_feature_observation_hkweather_10min_realtime_v1;

SELECT
    date,
    source,
    mean_temp_c,
    max_temp_c,
    min_temp_c,
    total_rainfall_mm
FROM fact_feature_date_hkweather_1day_daily_v1
ORDER BY date DESC
LIMIT 20;

SELECT
    obs_time,
    fetched_at,
    source,
    metric,
    value
FROM fact_feature_observation_hkweather_10min_realtime_v1
ORDER BY obs_time DESC
LIMIT 20;
```

## Notes

- Business daily queries should use `fact_feature_date_hkweather_1day_daily_v1`.
- Realtime queries should use `fact_feature_observation_hkweather_10min_realtime_v1`.
- Raw realtime observations are retained for 60 days by default.
- Airflow should not run multiple active instances of the same DAG; each DAG sets `max_active_runs=1`.
- Table names follow `[department]_[project]_[OK_entity]_[data_field]_[window_size]_[frequency]_[version]`.
