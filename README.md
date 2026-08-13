# Hong Kong Weather

Hong Kong Observatory weather ingestion for DEV ZONE Yellowbrick.

This project loads the Hong Kong Observatory center station only:

```text
station_code = HKO
station_name = Hong Kong Observatory
```

The business-facing official daily table is:

```text
generic_sma_ai_shared.fact_feature_date_hkweather_1day_daily_v1
```

Realtime observations are stored separately by observation/update time in `fact_feature_observation_hkweather_10min_realtime_v1`.

## Repository Layout

```text
.
├── README.md
├── .env.example
├── hko_common.py
├── update_hko_postgres.py
├── update_hko_realtime_postgres.py
├── cleanup_hko_realtime_raw.py
├── dags/
│   └── hko_weather_airflow.py
├── docs/
│   ├── AIRFLOW_RUNBOOK.md
│   ├── DEV_ZONE_RUNBOOK.md
│   └── LOCAL_AIRFLOW_DEMO.md
├── legacy/
│   ├── QWEN_POSTGRES_GUIDE.md
│   ├── QWEN_REALTIME_DAILY_GUIDE.md
│   ├── scrape_hko_history.py
│   ├── update_hko_database.py
│   └── scripts/
├── tests/
└── pyproject.toml
```

Current production path is Yellowbrick + Airflow. The `legacy/` folder keeps older SQLite, Qwen, and macOS launchd materials for reference only.

## DEV ZONE Setup

Use the deployment branch:

```bash
cd /opt/llm/chrishuang
git clone https://github.com/HUANGYming/hongkong-weather.git
cd /opt/llm/chrishuang/hongkong-weather
git checkout codex/local-airflow-demo
git pull
uv sync --locked
```

Create `.env`:

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
HKO_RAW_RETENTION_DAYS=60
```

Do not commit `.env`.

## Run

Offline tests:

```bash
uv run python -m unittest discover -s tests -v
```

Smoke test realtime raw ingestion:

```bash
uv run python update_hko_realtime_postgres.py --mode current --include-rainfall
```

Load official HKO daily data from `2020-01-01` onward. This uses HKO D1 plus Daily Extract so recent published months can arrive before the D1 API catches up:

```bash
uv run python update_hko_postgres.py --full-refresh
```

Optional archive raw backfill:

```bash
uv run python update_hko_realtime_postgres.py \
  --mode archive \
  --start-date 2026-07-01
```

DATA.GOV.HK Historical Archive only publishes through yesterday. This writes raw observation rows only; it does not produce daily business rows.

Run current realtime again:

```bash
uv run python update_hko_realtime_postgres.py --mode current --include-rainfall
```

## Docker

Build the project image:

```bash
docker build -t hongkong-weather:latest .
```

Run one job with the project `.env`:

```bash
docker run --rm --env-file .env \
  hongkong-weather:latest update_hko_realtime_postgres.py --mode current --include-rainfall
```

Airflow can use Docker mode by setting:

```dotenv
HKO_DOCKER_IMAGE=hongkong-weather:latest
HKO_DOCKER_ENV_FILE=/opt/llm/chrishuang/hongkong-weather/.env
# HKO_DOCKER_RUN_ARGS=--network host
```

## Verify

Build `DATABASE_URL` for `psql` if you only configured `DB_*` in `.env`:

```bash
source .env
export DATABASE_URL="postgresql://${DB_USER}:${DB_PASS}@${DB_HOST}:${DB_PORT}/${DB_NAME}"
```

Query the business-facing official daily table:

```bash
psql "$DATABASE_URL" -c "
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
"
```

Query recent realtime raw observations:

```bash
psql "$DATABASE_URL" -c "
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
"
```

## Main Objects

```text
fact_feature_date_hkweather_1day_daily_v1                Official HKO D1/Daily Extract daily table
fact_feature_observation_hkweather_10min_realtime_v1      Realtime/archive observations by obs_time
meta_feature_run_hkweather_ingest_1run_event_v1          Ingest run log
```

## Yellowbrick Notes

```text
The project uses psycopg2.
The project does not create schemas.
The project does not create secondary indexes.
The project uses varchar columns instead of PostgreSQL text columns.
Refresh writes use DELETE plus INSERT instead of PostgreSQL ON CONFLICT.
```

## Airflow

The production DAG is:

```text
dags/hko_weather_airflow.py
```

Detailed deployment notes:

```text
docs/AIRFLOW_RUNBOOK.md
docs/DEV_ZONE_RUNBOOK.md
```

Recommended DEV ZONE symlink:

```bash
ln -s /opt/llm/chrishuang/hongkong-weather/dags/hko_weather_airflow.py \
  /opt/llm/airflow/dags/hko_weather_airflow.py
```
