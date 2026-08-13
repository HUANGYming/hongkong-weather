"""Airflow DAGs for HKO weather ingestion.

Deployment assumptions:
- The DAG file is available to Airflow.
- Docker is available on every Airflow worker by default.
- Database settings are available in the Airflow worker environment.

Optional environment variables:
- HKO_EXECUTION_MODE: docker or uv, default docker.
- HKO_DOCKER_IMAGE: Docker image to run, default hongkong-weather:latest.
- HKO_DOCKER_ENV_FILE: optional env-file path if Airflow can read it.
- HKO_PROJECT_DIR: absolute path to this repository, only needed for uv mode.
- HKO_RAW_RETENTION_DAYS: raw realtime observation retention, default 60.
"""

from __future__ import annotations

import os
import shlex
from datetime import timedelta
from pathlib import Path

import pendulum

try:
    from airflow.sdk import DAG
except ImportError:  # Airflow 2.x compatibility.
    from airflow import DAG

try:
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:  # Airflow 2.x compatibility.
    from airflow.operators.bash import BashOperator


DAG_DIR = Path(__file__).resolve().parent
DEFAULT_PROJECT_DIR = DAG_DIR.parent
DEV_ZONE_PROJECT_DIR = Path("/opt/llm/chrishuang/hongkong-weather")
PROJECT_DIR = os.environ.get(
    "HKO_PROJECT_DIR",
    str(DEV_ZONE_PROJECT_DIR if DEV_ZONE_PROJECT_DIR.exists() else DEFAULT_PROJECT_DIR),
)
HK_TZ = pendulum.timezone("Asia/Hong_Kong")


DEFAULT_ARGS = {
    "owner": "data",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


DOCKER_PASSTHROUGH_ENV = (
    "DATABASE_URL",
    "DB_HOST",
    "DB_PORT",
    "DB_NAME",
    "DB_USER",
    "DB_PASS",
    "HKO_DB_SCHEMA",
    "HKO_RAW_RETENTION_DAYS",
)


def task_command(command: str) -> str:
    quoted_project_dir = shlex.quote(PROJECT_DIR)
    passthrough_names = " ".join(DOCKER_PASSTHROUGH_ENV)
    return (
        "set -euo pipefail; "
        f'PROJECT_DIR={quoted_project_dir}; '
        'HKO_EXECUTION_MODE="${HKO_EXECUTION_MODE:-docker}"; '
        'if [ "${HKO_EXECUTION_MODE}" = "docker" ]; then '
        'HKO_DOCKER_BIN="${HKO_DOCKER_BIN:-docker}"; '
        'HKO_DOCKER_IMAGE="${HKO_DOCKER_IMAGE:-hongkong-weather:latest}"; '
        'DOCKER_ENV_ARGS=""; '
        'if [ -n "${HKO_DOCKER_ENV_FILE:-}" ]; then '
        'DOCKER_ENV_ARGS="--env-file ${HKO_DOCKER_ENV_FILE}"; '
        "else "
        'if [ -z "${DATABASE_URL:-}" ] && [ -z "${DB_HOST:-}" ]; then '
        'echo "DATABASE_URL or DB_* settings are required in the Airflow worker environment"; exit 1; '
        "fi; "
        f"for env_name in {passthrough_names}; do "
        'if [ -n "${!env_name:-}" ]; then DOCKER_ENV_ARGS="${DOCKER_ENV_ARGS} -e ${env_name}"; fi; '
        "done; "
        "fi; "
        '"${HKO_DOCKER_BIN}" run --rm ${DOCKER_ENV_ARGS} ${HKO_DOCKER_RUN_ARGS:-} '
        f'"${{HKO_DOCKER_IMAGE}}" {command}; '
        'elif [ "${HKO_EXECUTION_MODE}" = "uv" ]; then '
        'if [ -f "${PROJECT_DIR}/.env" ]; then set -a; . "${PROJECT_DIR}/.env"; set +a; fi; '
        'if [ -z "${DATABASE_URL:-}" ] && [ -z "${DB_HOST:-}" ]; then '
        'echo "DATABASE_URL or DB_* settings are required"; exit 1; '
        "fi; "
        'cd "${PROJECT_DIR}"; '
        f"uv run python {command}; "
        "else "
        'echo "Unsupported HKO_EXECUTION_MODE=${HKO_EXECUTION_MODE}; use docker or uv"; exit 1; '
        "fi"
    )


with DAG(
    dag_id="hko_weather_realtime_ingest",
    description="Fetch current HKO realtime observations and update raw observation rows.",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 8, 10, tz=HK_TZ),
    schedule="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["hko", "weather", "realtime"],
) as realtime_ingest_dag:
    BashOperator(
        task_id="ingest_current_observations",
        bash_command=task_command("update_hko_realtime_postgres.py --mode current --include-rainfall"),
        execution_timeout=timedelta(minutes=8),
    )


with DAG(
    dag_id="hko_weather_realtime_cleanup",
    description="Prune old HKO realtime raw observations.",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 8, 10, tz=HK_TZ),
    schedule="25 1 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["hko", "weather", "daily", "cleanup"],
) as realtime_cleanup_dag:
    BashOperator(
        task_id="cleanup_realtime_observations",
        bash_command=task_command("cleanup_hko_realtime_raw.py"),
        execution_timeout=timedelta(minutes=10),
    )


with DAG(
    dag_id="hko_weather_official_daily_ingest",
    description="Fetch official HKO daily data and update the business daily table.",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 8, 10, tz=HK_TZ),
    schedule="15 8 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["hko", "weather", "official"],
) as official_daily_ingest_dag:
    BashOperator(
        task_id="ingest_official_daily",
        bash_command=task_command("update_hko_postgres.py"),
        execution_timeout=timedelta(minutes=30),
    )


with DAG(
    dag_id="hko_weather_bootstrap",
    description="Manual bootstrap DAG: official daily full refresh, then current realtime.",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 8, 10, tz=HK_TZ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["hko", "weather", "bootstrap"],
) as bootstrap_dag:
    full_refresh_official_daily = BashOperator(
        task_id="full_refresh_official_daily",
        bash_command=task_command("update_hko_postgres.py --full-refresh"),
        execution_timeout=timedelta(hours=1),
    )

    ingest_current_observations = BashOperator(
        task_id="ingest_current_observations",
        bash_command=task_command("update_hko_realtime_postgres.py --mode current --include-rainfall"),
        execution_timeout=timedelta(minutes=8),
    )

    full_refresh_official_daily >> ingest_current_observations
