#!/usr/bin/env bash
set -euo pipefail

export HKO_PROJECT_DIR="${HKO_PROJECT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
export HKO_AIRFLOW_HOME="${HKO_AIRFLOW_HOME:-/tmp/hko-airflow-local-20260810}"
export HKO_AIRFLOW_VENV="${HKO_AIRFLOW_VENV:-/tmp/hko-airflow-venv}"
export HKO_POSTGRES_CONTAINER="${HKO_POSTGRES_CONTAINER:-hko-airflow-postgres}"
export HKO_POSTGRES_PORT="${HKO_POSTGRES_PORT:-55432}"

export AIRFLOW_HOME="$HKO_AIRFLOW_HOME"
export AIRFLOW__CORE__DAGS_FOLDER="$HKO_AIRFLOW_HOME/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__EXECUTOR=SequentialExecutor
export AIRFLOW__CORE__DEFAULT_TIMEZONE=Asia/Hong_Kong
export AIRFLOW__WEBSERVER__DEFAULT_UI_TIMEZONE=Asia/Hong_Kong
export AIRFLOW__SCHEDULER__CATCHUP_BY_DEFAULT=False

export PATH="$HKO_AIRFLOW_VENV/bin:$PATH"
export DATABASE_URL="${DATABASE_URL:-postgresql://hko:postgres@localhost:${HKO_POSTGRES_PORT}/hko_weather}"
export HKO_ARCHIVE_LOOKBACK_DAYS="${HKO_ARCHIVE_LOOKBACK_DAYS:-2}"
export HKO_RAW_RETENTION_DAYS="${HKO_RAW_RETENTION_DAYS:-60}"
