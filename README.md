# Hong Kong Weather

Hong Kong Observatory weather ingestion for DEV ZONE Yellowbrick.

Only the HKO center station is loaded:

```text
station_code = HKO
station_name = Hong Kong Observatory
```

## Tables

```text
generic_sma_ai_shared.fact_feature_date_hkweather_1day_daily_v1
  Official daily weather table from HKO D1 and Daily Extract.

generic_sma_ai_shared.fact_feature_observation_hkweather_10min_realtime_v1
  Realtime observation rows by obs_time/fetched_at.

generic_sma_ai_shared.meta_feature_run_hkweather_ingest_1run_event_v1
  Ingestion run log.
```

## Deploy

Use branch `codex/local-airflow-demo`.

```bash
cd /opt/llm/chrishuang
git clone https://github.com/HUANGYming/hongkong-weather.git
cd /opt/llm/chrishuang/hongkong-weather
git checkout codex/local-airflow-demo
git pull
cp .env.example .env
vi .env
```

Required `.env` values:

```dotenv
DB_HOST=codppybkdbd01.melco-resorts.com
DB_PORT=5432
DB_NAME=bigdata_prod
DB_USER=1018195
DB_PASS=replace_with_real_password
HKO_DB_SCHEMA=generic_sma_ai_shared

HKO_PROJECT_DIR=/opt/llm/chrishuang/hongkong-weather
HKO_RAW_RETENTION_DAYS=60

HKO_EXECUTION_MODE=docker
HKO_DOCKER_IMAGE=hongkong-weather:latest
HKO_DOCKER_ENV_FILE=/opt/llm/chrishuang/hongkong-weather/.env
HKO_DOCKER_BIN=docker
# HKO_DOCKER_RUN_ARGS=--network host
```

Build the Docker image:

```bash
docker image build --tag hongkong-weather:latest .
```

If the container cannot reach Yellowbrick, uncomment this in `.env`:

```dotenv
HKO_DOCKER_RUN_ARGS=--network host
```

## Airflow

Airflow uses Docker mode by default.

Copy the DAG:

```bash
cp /opt/llm/chrishuang/hongkong-weather/dags/hko_weather_airflow.py \
  /opt/llm/airflow/dags/hko_weather_airflow.py
```

DAGs:

```text
hko_weather_realtime_ingest          every 10 minutes
hko_weather_realtime_cleanup         daily 01:25 HK time
hko_weather_official_daily_ingest    daily 08:15 HK time
hko_weather_bootstrap                manual bootstrap
```

Check Airflow:

```bash
airflow dags list-import-errors
airflow dags list | grep hko_weather
```

## Manual Run

Test Docker:

```bash
docker run --rm --env-file /opt/llm/chrishuang/hongkong-weather/.env \
  hongkong-weather:latest update_hko_realtime_postgres.py --help
```

Realtime current ingestion:

```bash
docker run --rm --env-file /opt/llm/chrishuang/hongkong-weather/.env \
  hongkong-weather:latest update_hko_realtime_postgres.py --mode current --include-rainfall
```

Official daily full refresh:

```bash
docker run --rm --env-file /opt/llm/chrishuang/hongkong-weather/.env \
  hongkong-weather:latest update_hko_postgres.py --full-refresh
```

Local tests:

```bash
uv run python -m unittest discover -s tests -v
```

## Verify

```bash
source .env
export DATABASE_URL="postgresql://${DB_USER}:${DB_PASS}@${DB_HOST}:${DB_PORT}/${DB_NAME}"
```

Official daily:

```bash
psql "$DATABASE_URL" -c "
SELECT date, source, mean_temp_c, max_temp_c, min_temp_c,
       mslp_hpa, mean_relative_humidity_pct, total_rainfall_mm
FROM generic_sma_ai_shared.fact_feature_date_hkweather_1day_daily_v1
ORDER BY date DESC
LIMIT 20;
"
```

Realtime:

```bash
psql "$DATABASE_URL" -c "
SELECT obs_time, fetched_at, source, metric, value, unit
FROM generic_sma_ai_shared.fact_feature_observation_hkweather_10min_realtime_v1
ORDER BY obs_time DESC, source, metric
LIMIT 20;
"
```

## More Notes

```text
docs/AIRFLOW_RUNBOOK.md
docs/DEV_ZONE_RUNBOOK.md
docs/LOCAL_AIRFLOW_DEMO.md
```
