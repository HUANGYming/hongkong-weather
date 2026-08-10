# DEV ZONE Runbook

This runbook starts from an empty PostgreSQL database and builds the final HKO daily dataset.

## 1. Install

```bash
git clone https://github.com/HUANGYming/hongkong-weather.git
cd hongkong-weather

python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

## 2. Configure

```bash
export DATABASE_URL="postgresql://user:password@host:5432/dbname"
mkdir -p logs
```

## 3. Run Offline Tests

```bash
python3 -m unittest discover -s tests -v
```

## 4. Load Official HKO D1 Data

This loads official monthly historical daily data from `2020-01-01` onward.

```bash
python3 update_hko_postgres.py --full-refresh
```

Expected official data currently lags behind realtime data because HKO D1 is monthly.

## 5. Backfill Missing Provisional Days

For the current known gap:

```bash
python3 update_hko_realtime_postgres.py \
  --mode archive \
  --start-date 2026-07-01 \
  --end-date 2026-08-09
```

This uses DATA.GOV.HK Historical Archive to replay HKO 10-minute snapshots and aggregate daily provisional rows.

Fields covered by archive backfill:

```text
mean_temp_c
max_temp_c
min_temp_c
mslp_hpa
mean_relative_humidity_pct
```

Rainfall is not reliably available from the same archive route. Start collecting rainfall from live hourly observations and let D1 official data fill final rainfall once published.

## 6. Start Current Realtime Updates

Run once manually:

```bash
python3 update_hko_realtime_postgres.py --mode current --include-rainfall
```

Then query:

```sql
SELECT
    date,
    data_status,
    mean_temp_c,
    max_temp_c,
    min_temp_c,
    mslp_hpa,
    mean_relative_humidity_pct,
    total_rainfall_mm
FROM hko_daily_weather_latest_v
ORDER BY date DESC
LIMIT 20;
```

## 7. Cron

```cron
# Current provisional snapshots, every 10 minutes
*/10 * * * * cd /path/to/hongkong-weather && . .venv/bin/activate && python update_hko_realtime_postgres.py --mode current --include-rainfall >> logs/hko_realtime.log 2>&1

# Historical archive backfill for yesterday/recent corrections, daily
25 1 * * * cd /path/to/hongkong-weather && . .venv/bin/activate && python update_hko_realtime_postgres.py --mode archive --archive-lookback-days 14 >> logs/hko_archive.log 2>&1

# Official D1 checker, daily
15 8 * * * cd /path/to/hongkong-weather && . .venv/bin/activate && python update_hko_postgres.py >> logs/hko_official.log 2>&1
```

## 8. Main Objects

```text
hko_daily_weather_official      Official monthly HKO D1 daily rows
hko_realtime_observations       Raw realtime/archive snapshots
hko_daily_weather_provisional   Provisional daily aggregates
hko_daily_weather_latest_v      Official-first daily wide view
```

