#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/local_airflow_env.sh"

airflow api-server --port "${AIRFLOW_WEBSERVER_PORT:-8080}"
