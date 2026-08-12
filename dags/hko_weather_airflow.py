"""Airflow DAGs for HKO weather ingestion.

Deployment assumptions:
- This repository is available on every Airflow worker.
- `uv` is installed on every Airflow worker.
- `DATABASE_URL` is available in the Airflow worker environment.

Optional environment variables:
- HKO_PROJECT_DIR: absolute path to this repository on the Airflow worker.
- HKO_RAW_RETENTION_DAYS: raw realtime observation retention, default 60.
- HKO_ARCHIVE_LOOKBACK_DAYS: daily archive replay window, default 14.
"""

from __future__ import annotations

import os
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
PROJECT_DIR = os.environ.get("HKO_PROJECT_DIR", str(DEFAULT_PROJECT_DIR))
HK_TZ = pendulum.timezone("Asia/Hong_Kong")


DEFAULT_ARGS = {
    "owner": "data",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def uv_command(command: str) -> str:
    return (
        "set -euo pipefail; "
        f"cd {PROJECT_DIR!r}; "
        "if [ -f .env ]; then set -a; . ./.env; set +a; fi; "
        'if [ -z "${DATABASE_URL:-}" ] && [ -z "${DB_HOST:-}" ]; then '
        'echo "DATABASE_URL or DB_* settings are required"; exit 1; '
        "fi; "
        f"uv run python {command}"
    )


with DAG(
    dag_id="hko_realtime_current",
    description="Fetch current HKO realtime observations and update provisional daily rows.",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 8, 10, tz=HK_TZ),
    schedule="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["hko", "weather", "realtime"],
) as realtime_current_dag:
    BashOperator(
        task_id="update_current_realtime",
        bash_command=uv_command("update_hko_realtime_postgres.py --mode current --include-rainfall"),
        execution_timeout=timedelta(minutes=8),
    )


with DAG(
    dag_id="hko_daily_backfill_cleanup",
    description="Replay recent HKO archive snapshots, then prune old realtime raw observations.",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 8, 10, tz=HK_TZ),
    schedule="25 1 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["hko", "weather", "daily", "backfill", "cleanup"],
) as daily_backfill_cleanup_dag:
    backfill_recent_archive = BashOperator(
        task_id="backfill_recent_archive",
        bash_command=uv_command(
            "update_hko_realtime_postgres.py "
            "--mode archive "
            '--archive-lookback-days "${HKO_ARCHIVE_LOOKBACK_DAYS:-14}"'
        ),
        execution_timeout=timedelta(hours=1),
    )

    cleanup_realtime_raw = BashOperator(
        task_id="cleanup_realtime_raw",
        bash_command=uv_command(
            'cleanup_hko_realtime_raw.py --retention-days "${HKO_RAW_RETENTION_DAYS:-60}"'
        ),
        execution_timeout=timedelta(minutes=10),
    )

    backfill_recent_archive >> cleanup_realtime_raw


with DAG(
    dag_id="hko_official_d1",
    description="Fetch official HKO D1 monthly daily data and update official table.",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 8, 10, tz=HK_TZ),
    schedule="15 8 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["hko", "weather", "official"],
) as official_d1_dag:
    BashOperator(
        task_id="update_official_d1",
        bash_command=uv_command("update_hko_postgres.py"),
        execution_timeout=timedelta(minutes=30),
    )


with DAG(
    dag_id="hko_initial_backfill",
    description="Manual bootstrap DAG: official D1 full refresh, known archive gap backfill, then current realtime.",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 8, 10, tz=HK_TZ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["hko", "weather", "bootstrap"],
) as initial_backfill_dag:
    official_full_refresh = BashOperator(
        task_id="official_full_refresh",
        bash_command=uv_command("update_hko_postgres.py --full-refresh"),
        execution_timeout=timedelta(hours=1),
    )

    archive_gap_backfill = BashOperator(
        task_id="archive_gap_backfill",
        bash_command=uv_command(
            "update_hko_realtime_postgres.py "
            "--mode archive "
            "--start-date 2026-07-01 "
            "--end-date 2026-08-09"
        ),
        execution_timeout=timedelta(hours=2),
    )

    current_realtime = BashOperator(
        task_id="current_realtime",
        bash_command=uv_command("update_hko_realtime_postgres.py --mode current --include-rainfall"),
        execution_timeout=timedelta(minutes=8),
    )

    official_full_refresh >> archive_gap_backfill >> current_realtime
