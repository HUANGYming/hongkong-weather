# Hong Kong Weather

HKO center station weather ingestion for DEV ZONE Yellowbrick.

## Tables

```text
generic_sma_ai_shared.fact_feature_date_hkweather_1day_daily_v1
generic_sma_ai_shared.fact_feature_observation_hkweather_10min_realtime_v1
generic_sma_ai_shared.meta_feature_run_hkweather_ingest_1run_event_v1
```

## Deploy

```bash
cd /opt/llm/chrishuang
git clone https://github.com/HUANGYming/hongkong-weather.git
cd /opt/llm/chrishuang/hongkong-weather
git checkout codex/local-airflow-demo
git pull origin codex/local-airflow-demo

cp .env.example .env
vi .env

docker image build \
  --network host \
  --build-arg UV_DEFAULT_INDEX=https://pypi.tuna.tsinghua.edu.cn/simple \
  --build-arg UV_HTTP_TIMEOUT=300 \
  --tag hongkong-weather:latest .

rm -f /opt/llm/airflow/dags/hko_weather_airflow.py
ln -s /opt/llm/chrishuang/hongkong-weather/dags/hko_weather_airflow.py \
  /opt/llm/airflow/dags/hko_weather_airflow.py
chmod 777 /opt/llm/chrishuang/hongkong-weather/dags/hko_weather_airflow.py
```

`.env`:

```dotenv
DB_HOST=codppybkdbd01.melco-resorts.com
DB_PORT=5432
DB_NAME=bigdata_prod
DB_USER=1018195
DB_PASS=replace_with_real_password
HKO_DB_SCHEMA=generic_sma_ai_shared
HKO_RAW_RETENTION_DAYS=60
```

Docker build copies `.env` into the image as `/app/.env`. Rebuild the image after changing `.env`.
If DEV ZONE has an internal PyPI mirror, replace `UV_DEFAULT_INDEX` with that mirror URL.

If Docker needs host networking, set this in the Airflow worker environment:

```dotenv
HKO_DOCKER_RUN_ARGS=--network host
```

## Airflow DAGs

```text
hko_weather_realtime_ingest          every 10 minutes
hko_weather_realtime_cleanup         daily 01:25 HK time
hko_weather_official_daily_ingest    daily 08:15 HK time
hko_weather_bootstrap                manual bootstrap
```

Check:

```bash
airflow dags list-import-errors
airflow dags list | grep hko_weather
```

## Verify

```bash
source .env
export DATABASE_URL="postgresql://${DB_USER}:${DB_PASS}@${DB_HOST}:${DB_PORT}/${DB_NAME}"

psql "$DATABASE_URL" -c "
SELECT date, source, mean_temp_c, max_temp_c, min_temp_c,
       mslp_hpa, mean_relative_humidity_pct, total_rainfall_mm
FROM generic_sma_ai_shared.fact_feature_date_hkweather_1day_daily_v1
ORDER BY date DESC
LIMIT 20;
"

psql "$DATABASE_URL" -c "
SELECT obs_time, fetched_at, source, metric, value, unit
FROM generic_sma_ai_shared.fact_feature_observation_hkweather_10min_realtime_v1
ORDER BY obs_time DESC, source, metric
LIMIT 20;
"
```
