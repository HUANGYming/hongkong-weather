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

## SQLite Wide Table

Create or update the SQLite database:

```bash
python3 update_hko_database.py
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
python3 update_hko_database.py --full-refresh
```

Daily update behavior:

```text
First run: loads 2020-01-01 through the latest available HKO date.
Later runs: refreshes recent/current-year data and upserts by date.
```

Each run is recorded in `hko_ingest_runs`.

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
