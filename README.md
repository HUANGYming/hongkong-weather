# Hong Kong Observatory Historical Data Scraper

This project downloads daily climate data from 2020-01-01 onward for the Hong Kong Observatory centre station only.

Source endpoint:

```text
https://data.weather.gov.hk/weatherAPI/D1/caller.php?stn=HKO&ele=<ELEMENT>&yr=<YEAR_OR_ALL>
```

The script keeps `stn=HKO` fixed. By default it excludes HKO download-page elements that are mapped to other stations such as King's Park, Waglan Island, North Point, Hong Kong International Airport, or all-Hong-Kong lightning aggregates.

## Run

```bash
python3 scrape_hko_history.py
```

Default output:

```text
data/raw/daily_HKO_<ELEMENT>_<YEAR>.csv
data/hko_daily_long.csv
data/hko_daily_wide.csv
```

The default run downloads year-by-year from `2020` through the current year.

## PostgreSQL Production Tables

Install dependencies:

```bash
# Install uv first if the server does not have it:
# curl -LsSf https://astral.sh/uv/install.sh | sh
uv sync --locked
```

Configure Postgres:

```bash
export DATABASE_URL="postgresql://user:password@host:5432/dbname"
```

Load official HKO D1 data from 2020 onward:

```bash
uv run python update_hko_postgres.py --full-refresh
```

Backfill the missing provisional period from DATA.GOV.HK historical archive:

```bash
uv run python update_hko_realtime_postgres.py \
  --mode archive \
  --start-date 2026-07-01 \
  --end-date 2026-08-09
```

Update current provisional observations:

```bash
uv run python update_hko_realtime_postgres.py --mode current --include-rainfall
```

Main tables/views:

```text
hko_daily_weather_official      Official monthly HKO D1 daily rows
hko_realtime_observations       Raw realtime/archive snapshots
hko_daily_weather_provisional   Provisional daily aggregates
hko_daily_weather_latest_v      Official-first daily wide view
```

Retention policy:

```text
hko_daily_weather_official      Keep forever
hko_daily_weather_provisional   Keep unless old rows are already covered by official data
hko_realtime_observations       Keep recent raw snapshots only, default 60 days
```

Cleanup command:

```bash
uv run python cleanup_hko_realtime_raw.py --retention-days 60
```

Use this view for application queries:

```sql
SELECT *
FROM hko_daily_weather_latest_v
ORDER BY date DESC;
```

Production cron example:

```cron
# Current provisional snapshots, every 10 minutes
*/10 * * * * cd /path/to/hongkong-weather && uv run python update_hko_realtime_postgres.py --mode current --include-rainfall >> logs/hko_realtime.log 2>&1

# Historical archive backfill for yesterday/recent corrections, daily
25 1 * * * cd /path/to/hongkong-weather && uv run python update_hko_realtime_postgres.py --mode archive --archive-lookback-days 14 >> logs/hko_archive.log 2>&1

# Official D1 checker, daily
15 8 * * * cd /path/to/hongkong-weather && uv run python update_hko_postgres.py >> logs/hko_official.log 2>&1

# Raw realtime retention cleanup, daily
40 2 * * * cd /path/to/hongkong-weather && uv run python cleanup_hko_realtime_raw.py --retention-days 60 >> logs/hko_cleanup.log 2>&1
```

## Tests

Offline parser tests:

```bash
uv run python -m unittest discover -s tests -v
```

## SQLite Wide Table

Create or update the SQLite database:

```bash
uv run python update_hko_database.py
```

Default database and table:

```text
data/hko_weather.sqlite
hko_daily_weather_wide
```

The table is wide format: one row per date, `date` as the primary key, and one set of value/raw/completeness columns per HKO element.

Example query:

```bash
sqlite3 data/hko_weather.sqlite "select date, mean_temp_c, total_rainfall_mm from hko_daily_weather_wide order by date desc limit 10;"
```

First run or forced rebuild:

```bash
uv run python update_hko_database.py --full-refresh
```

Daily update behavior:

```text
First run: loads 2020-01-01 through the latest available HKO date.
Later runs: refreshes recent/current-year data and upserts by date.
```

Each run is recorded in `hko_ingest_runs`.

## Realtime Daily Data

The HKO `D1` daily climate endpoint is monthly, so it may not include today or the latest completed month. For daily realtime/provisional data, use:

```text
QWEN_REALTIME_DAILY_GUIDE.md
```

That guide adds a realtime Postgres layer using HKO provisional regional weather APIs, then combines official monthly rows with provisional current rows.

## Daily macOS Schedule

The launchd template is:

```text
scripts/com.hko.daily-weather-update.plist
```

It runs `update_hko_database.py` every day at `08:15` local machine time. Because macOS LaunchAgents can be blocked from reading files under Desktop, the scheduled runtime lives in:

```text
/Users/YOUR_USERNAME/.hko_weather
```

Scheduled database and logs:

```text
/Users/YOUR_USERNAME/.hko_weather/data/hko_weather.sqlite
/Users/YOUR_USERNAME/.hko_weather/logs/hko_update.out.log
/Users/YOUR_USERNAME/.hko_weather/logs/hko_update.err.log
```

## Elements

Default HKO-only elements:

```text
MSLP  Daily Mean Pressure, hPa
TEMP  Daily Mean Temperature, deg C
DEW   Daily Mean Dew Point Temperature, deg C
WET   Daily Mean Wet-Bulb Temperature, deg C
RH    Daily Mean Relative Humidity, percent
CLD   Daily Mean Amount of Cloud, percent
RF    Daily Total Rainfall, mm
MAXT  Daily Maximum Temperature, deg C
MINT  Daily Minimum Temperature, deg C
GMT   Daily Grass Minimum Temperature, deg C
```

Download selected elements:

```bash
python3 scrape_hko_history.py --elements TEMP,RF,MAXT,MINT
```

Download a single year:

```bash
python3 scrape_hko_history.py --year 2025 --overwrite
```

Download year-by-year:

```bash
python3 scrape_hko_history.py --start-year 2020 --end-year 2025 --overwrite
```

Download full available history if needed:

```bash
python3 scrape_hko_history.py --year ALL --overwrite
```

Notes:

- `Trace` rainfall is normalized to `0.025` in the `value` column and preserved as `Trace` in `raw_value`.
- `***`, `---`, and blank values are normalized to blank in `value` and preserved in `raw_value`.
- Completeness flags are kept from HKO: `C` means complete, `#` means incomplete, blank usually means unavailable.
- `date_is_valid` marks calendar-valid dates. HKO `ALL` history includes a few official placeholder rows such as `1900-02-29`; these rows are kept.
