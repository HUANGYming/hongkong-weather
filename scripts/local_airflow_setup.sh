#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/local_airflow_env.sh"

PYTHON_BIN="${PYTHON_BIN:-}"
if [[ -z "$PYTHON_BIN" ]]; then
  if command -v uv >/dev/null 2>&1; then
    PYTHON_BIN="$(uv python find 3.11)"
  else
    PYTHON_BIN="$(command -v python3)"
  fi
fi

if [[ ! -x "$HKO_AIRFLOW_VENV/bin/airflow" ]]; then
  uv venv --python "$PYTHON_BIN" "$HKO_AIRFLOW_VENV"
  uv pip install --python "$HKO_AIRFLOW_VENV/bin/python" \
    "apache-airflow==2.10.5" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt"
fi

if ! docker ps --format '{{.Names}}' | grep -qx "$HKO_POSTGRES_CONTAINER"; then
  if docker ps -a --format '{{.Names}}' | grep -qx "$HKO_POSTGRES_CONTAINER"; then
    docker start "$HKO_POSTGRES_CONTAINER" >/dev/null
  else
    docker run \
      --name "$HKO_POSTGRES_CONTAINER" \
      -e POSTGRES_PASSWORD=postgres \
      -e POSTGRES_USER=hko \
      -e POSTGRES_DB=hko_weather \
      -p "${HKO_POSTGRES_PORT}:5432" \
      -d postgres:16-alpine >/dev/null
  fi
fi

until docker exec "$HKO_POSTGRES_CONTAINER" pg_isready -U hko -d hko_weather >/dev/null 2>&1; do
  sleep 1
done

mkdir -p "$HKO_AIRFLOW_HOME/dags" "$HKO_AIRFLOW_HOME/logs"
ln -sf "$HKO_PROJECT_DIR/dags/hko_weather_airflow.py" "$HKO_AIRFLOW_HOME/dags/hko_weather_airflow.py"

airflow db migrate

if ! airflow users list | awk '{print $2}' | grep -qx admin; then
  airflow users create \
    --username admin \
    --firstname Local \
    --lastname Admin \
    --role Admin \
    --email admin@example.com \
    --password admin
fi

airflow dags list

echo
echo "Local Airflow is ready."
echo "Run: scripts/local_airflow_webserver.sh"
echo "Login: admin / admin"
