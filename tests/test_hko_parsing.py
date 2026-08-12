import io
import json
import os
import tempfile
import unittest
import zipfile
from datetime import date, datetime
from unittest import mock

from hko_common import (
    HKO_ELEMENTS,
    Observation,
    build_official_wide_rows,
    database_url_from_env,
    load_dotenv,
    parse_d1_csv_text,
    parse_daily_extract_json_text,
    parse_hourly_rainfall_json,
    parse_realtime_archive_zip,
    parse_realtime_csv_text,
)
import update_hko_realtime_postgres as realtime


class HkoParsingTests(unittest.TestCase):
    def test_parse_d1_and_build_wide_rows(self):
        text = """Daily Mean Temperature (°C) at the Hong Kong Observatory
年/Year,月/Month,日/Day,數值/Value,數據完整性/data Completeness
2026,7,1,28.8,C
2026,7,2,***,
"""
        rows = parse_d1_csv_text(text, HKO_ELEMENTS["TEMP"])
        wide = build_official_wide_rows(rows, date(2026, 7, 1), date(2026, 7, 2))
        self.assertEqual(len(wide), 2)
        self.assertEqual(wide[0]["date"], date(2026, 7, 1))
        self.assertEqual(wide[0]["mean_temp_c"], 28.8)
        self.assertEqual(wide[0]["mean_temp_c_raw"], "28.8")
        self.assertIsNone(wide[1]["mean_temp_c"])
        self.assertEqual(wide[1]["mean_temp_c_raw"], "***")

    def test_parse_daily_extract_json_and_build_wide_rows(self):
        text = """{"stn":{"data":[{"month":7,"dayData":[["01","1007.9","33.5","30.3","28.2","25.9","79","74","Trace"],["Mean/Total","1006.0","31.2","28.8","26.7","25.4","83","80","719.4"]]}]}}"""
        rows = parse_daily_extract_json_text(text, 2026)
        wide = build_official_wide_rows(rows, date(2026, 7, 1), date(2026, 7, 1))

        self.assertEqual(len(wide), 1)
        self.assertEqual(wide[0]["date"], date(2026, 7, 1))
        self.assertEqual(wide[0]["mslp_hpa"], 1007.9)
        self.assertEqual(wide[0]["max_temp_c"], 33.5)
        self.assertEqual(wide[0]["mean_temp_c"], 30.3)
        self.assertEqual(wide[0]["min_temp_c"], 28.2)
        self.assertEqual(wide[0]["mean_dew_point_c"], 25.9)
        self.assertEqual(wide[0]["mean_relative_humidity_pct"], 79)
        self.assertEqual(wide[0]["mean_cloud_amount_pct"], 74)
        self.assertEqual(wide[0]["total_rainfall_mm"], 0.025)

    def test_parse_realtime_temperature_csv_hko_only(self):
        text = """Date time,Automatic Weather Station,Air Temperature(degree Celsius)
202608101110,Chek Lap Kok,33.8
202608101110,HK Observatory,32.6
"""
        observations = parse_realtime_csv_text(text, "temperature")
        self.assertEqual(len(observations), 1)
        self.assertEqual(observations[0].metric, "temperature_c")
        self.assertEqual(observations[0].value, 32.6)
        self.assertEqual(observations[0].obs_date_hk, date(2026, 8, 10))

    def test_parse_realtime_maxmin_csv_two_metrics(self):
        text = """Date time,Automatic Weather Station,Maximum Air Temperature Since Midnight(degree Celsius),Minimum Air Temperature Since Midnight(degree Celsius)
202608101110,HK Observatory,32.6,30.1
"""
        observations = parse_realtime_csv_text(text, "maxmin")
        self.assertEqual({obs.metric for obs in observations}, {"max_temp_since_midnight_c", "min_temp_since_midnight_c"})
        self.assertEqual([obs.value for obs in observations], [32.6, 30.1])

    def test_parse_hourly_rainfall_json_hko_only(self):
        payload = {
            "obsTime": "2026-08-10T11:15:00+08:00",
            "hourlyRainfall": [
                {"automaticWeatherStation": "Hong Kong Observatory", "automaticWeatherStationID": "RF023", "value": "Trace", "unit": "mm"},
                {"automaticWeatherStation": "King's Park", "automaticWeatherStationID": "RF024", "value": "2", "unit": "mm"},
            ],
        }
        observations = parse_hourly_rainfall_json(json.dumps(payload))
        self.assertEqual(len(observations), 1)
        self.assertEqual(observations[0].metric, "hourly_rainfall_mm")
        self.assertEqual(observations[0].value, 0.025)

    def test_parse_archive_zip(self):
        csv_text = """Date time,Automatic Weather Station,Mean Sea Level Pressure(hPa)
202608101110,HK Observatory,996.1
"""
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            archive.writestr("20260810-1118-latest_1min_pressure.csv", csv_text)
        observations = parse_realtime_archive_zip(buffer.getvalue(), "pressure")
        self.assertEqual(len(observations), 1)
        self.assertEqual(observations[0].metric, "pressure_hpa")
        self.assertEqual(observations[0].value, 996.1)

    def test_load_dotenv_sets_missing_values(self):
        with tempfile.NamedTemporaryFile("w", encoding="utf-8") as env_file:
            env_file.write("HKO_TEST_ENV=value_from_file\n")
            env_file.flush()

            os.environ.pop("HKO_TEST_ENV", None)
            load_dotenv(env_file.name)
            self.assertEqual(os.environ["HKO_TEST_ENV"], "value_from_file")

            os.environ["HKO_TEST_ENV"] = "existing_value"
            load_dotenv(env_file.name)
            self.assertEqual(os.environ["HKO_TEST_ENV"], "existing_value")

    def test_database_url_from_db_env(self):
        saved = {key: os.environ.get(key) for key in ("DATABASE_URL", "DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASS")}
        try:
            for key in saved:
                os.environ.pop(key, None)
            os.environ.update(
                {
                    "DB_HOST": "db.example.com",
                    "DB_PORT": "5432",
                    "DB_NAME": "bigdata_prod",
                    "DB_USER": "1018195",
                    "DB_PASS": "p@ss/word",
                }
            )
            self.assertEqual(
                database_url_from_env(),
                "postgresql://1018195:p%40ss%2Fword@db.example.com:5432/bigdata_prod",
            )
        finally:
            for key, value in saved.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def test_archive_end_date_caps_to_yesterday(self):
        captured = {}

        class DummyConnection:
            def close(self):
                captured["closed"] = True

        def fake_fetch_archive(start_date, end_date):
            captured["start_date"] = start_date
            captured["end_date"] = end_date
            return []

        argv = [
            "update_hko_realtime_postgres.py",
            "--database-url",
            "postgresql://user:pass@localhost:5432/db",
            "--mode",
            "archive",
            "--start-date",
            "2026-07-01",
            "--end-date",
            "2026-08-12",
        ]
        with (
            mock.patch("sys.argv", argv),
            mock.patch.object(realtime, "hk_today", return_value=date(2026, 8, 12)),
            mock.patch.object(realtime, "connect", return_value=DummyConnection()),
            mock.patch.object(realtime, "create_schema"),
            mock.patch.object(realtime, "fetch_archive_observations", side_effect=fake_fetch_archive),
            mock.patch.object(realtime, "upsert_observations", return_value=0),
            mock.patch.object(realtime, "recompute_provisional", return_value=0),
        ):
            self.assertEqual(realtime.main(), 0)

        self.assertEqual(captured["start_date"], date(2026, 7, 1))
        self.assertEqual(captured["end_date"], date(2026, 8, 11))
        self.assertTrue(captured["closed"])

    def test_replace_archive_observations_deletes_by_metric_day_group(self):
        calls = []

        class DummyCursor:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def executemany(self, sql, values):
                calls.append((sql, list(values)))

        class DummyConnection:
            def cursor(self):
                return DummyCursor()

            def commit(self):
                calls.append(("commit", []))

        observations = [
            Observation(
                obs_time=datetime(2026, 8, 11, 10, 0),
                obs_date_hk=date(2026, 8, 11),
                source="latest_1min_pressure",
                metric="pressure_hpa",
                value=1001.1,
                raw_value="1001.1",
                unit="hPa",
            ),
            Observation(
                obs_time=datetime(2026, 8, 11, 10, 10),
                obs_date_hk=date(2026, 8, 11),
                source="latest_1min_pressure",
                metric="pressure_hpa",
                value=1001.3,
                raw_value="1001.3",
                unit="hPa",
            ),
        ]

        inserted, groups = realtime.replace_archive_observations(DummyConnection(), observations)

        self.assertEqual(inserted, 2)
        self.assertEqual(groups, 1)
        self.assertEqual(len(calls[0][1]), 1)
        self.assertEqual(calls[0][1][0], (date(2026, 8, 11), "latest_1min_pressure", "pressure_hpa", "HKO"))
        self.assertEqual(len(calls[1][1]), 2)


if __name__ == "__main__":
    unittest.main()
