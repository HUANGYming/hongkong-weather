#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/local_airflow_env.sh"

EXECUTION_DATE="${1:-$(python3 - <<'PY'
from datetime import datetime, timedelta, timezone

print((datetime.now(timezone.utc) - timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%S+00:00"))
PY
)}"

airflow tasks test hko_realtime_current update_current_realtime "$EXECUTION_DATE"

docker exec "$HKO_POSTGRES_CONTAINER" psql -U hko -d hko_weather \
  -c "select count(*) as raw_rows from ods_feature_observation_hkweather_10min_realtime_v1;" \
  -c "select obs_time, fetched_at, source, metric, value from ods_feature_observation_hkweather_10min_realtime_v1 order by obs_time desc, source, metric limit 10;"
