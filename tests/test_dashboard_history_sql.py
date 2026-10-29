"""Uji pemilihan prediksi historis dashboard terhadap PostgreSQL sementara."""
from __future__ import annotations

import ast
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
import unittest
from zoneinfo import ZoneInfo

import pandas as pd
import psycopg2


class DashboardChartTest(unittest.TestCase):
    def test_nilai_kosong_memisahkan_garis(self):
        source = (Path(__file__).resolve().parents[1] / "dashboard/app.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        function = next(node for node in tree.body
                        if isinstance(node, ast.FunctionDef) and node.name == "chart_series")
        module = ast.Module(body=[function], type_ignores=[])
        namespace = {"pd": pd}
        exec(compile(module, "<chart_series>", "exec"), namespace)
        times = pd.date_range("2026-01-01", periods=6, freq="h")
        values = pd.DataFrame({"Model": [1, None, 3, 4, None, 6],
                               "CAMS": [None, 2, None, 4, 5, None]}, index=times)
        chart = namespace["chart_series"](values)
        model = chart.loc[chart["seri"] == "Model"]
        cams = chart.loc[chart["seri"] == "CAMS"]
        self.assertEqual(model["waktu"].tolist(), [times[0], times[2], times[3], times[5]])
        self.assertEqual(model["segmen"].tolist(), [0, 1, 1, 2])
        self.assertEqual(cams["segmen"].tolist(), [1, 2, 2])


class DashboardHistorySQLTest(unittest.TestCase):
    def test_run_terbaru_sebelum_target_dan_gap(self):
        source = (Path(__file__).resolve().parents[1] / "dashboard/app.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        expression = next(node.args[0] for node in ast.walk(tree)
                          if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                          and node.func.id == "query" and node.args
                          and isinstance(node.args[0], ast.Call)
                          and isinstance(node.args[0].func, ast.Attribute)
                          and node.args[0].func.attr == "replace"
                          and "historical_model" in node.args[0].func.value.value)
        sql_expression = compile(ast.Expression(expression), "<dashboard query>", "eval")
        sql = eval(sql_expression, {"backtest_available": True})
        for name in ("ml_prediction", "ml_backtest", "dim_location", "dim_time", "fact_air_quality"):
            sql = sql.replace(f"warehouse.{name}", name)
        now = datetime.now(ZoneInfo("Asia/Makassar")).replace(tzinfo=None, minute=0, second=0, microsecond=0)
        origin = now - timedelta(hours=1)
        historical = origin - timedelta(hours=2)
        before = (historical - timedelta(minutes=10)).replace(tzinfo=ZoneInfo("Asia/Makassar"))
        with psycopg2.connect(
            host=os.getenv("DW_HOST", "localhost"), port=os.getenv("DW_PORT", "5432"),
            dbname=os.getenv("DW_DB", "air_quality_dw"), user=os.getenv("DW_USER", "platform"),
            password=os.getenv("DW_PASSWORD", "platform123"),
        ) as conn, conn.cursor() as cur:
            cur.execute("SET LOCAL TIME ZONE 'UTC'")
            cur.execute("CREATE TEMP TABLE ml_prediction (prediction_id int, run_id text, city text, "
                        "origin_at timestamp, target_at timestamp, predicted_pm2_5 float, "
                        "cams_pm2_5 float, created_at timestamptz)")
            cur.execute("CREATE TEMP TABLE dim_location (location_key int, city text)")
            cur.execute("CREATE TEMP TABLE dim_time (time_key int, valid_at timestamp)")
            cur.execute("CREATE TEMP TABLE fact_air_quality (location_key int, time_key int, pm2_5 float)")
            cur.execute("CREATE TEMP TABLE ml_backtest (city text, target_at timestamp, "
                        "predicted_pm2_5 float)")
            cur.execute("INSERT INTO dim_location VALUES (1, 'Palu')")
            cur.execute("INSERT INTO dim_time VALUES (1, %s)", (historical,))
            cur.execute("INSERT INTO fact_air_quality VALUES (1, 1, 40)")
            rows = [
                (1, 'new', 'Palu', origin, origin + timedelta(hours=1), 50, 51, datetime.now(timezone.utc)),
                (2, 'old', 'Palu', origin, origin + timedelta(hours=2), 70, 71, before),
                (3, 'new', 'Palu', origin, origin + timedelta(hours=3), 53, None, datetime.now(timezone.utc)),
                (4, 'history', 'Palu', historical - timedelta(hours=6), historical, 10, 11, before),
                (5, 'history', 'Palu', historical - timedelta(hours=6), historical, 20, None, before),
                (6, 'late', 'Palu', historical - timedelta(hours=6), historical, 99, 99,
                 historical.replace(tzinfo=ZoneInfo('Asia/Makassar'))),
                (7, 'other', 'Kota lain', historical - timedelta(hours=6), historical, 88, 88, before),
                (8, 'other-horizon', 'Palu', historical - timedelta(hours=1), historical, 77, 77, before),
            ]
            cur.executemany("INSERT INTO ml_prediction VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", rows)
            cur.execute("INSERT INTO ml_backtest VALUES (%s, %s, %s), (%s, %s, %s)",
                        ("Palu", historical, 30, "Kota lain", historical, 99))
            cur.execute(sql)
            result = [row for row in cur.fetchall() if row[0] == "Palu"]
            self.assertEqual(len(result), 12)
            selected = next(row for row in result if row[3] == historical)
            self.assertEqual(selected[4:8], (40, 20, None, 30))
            missing = next(row for row in result if row[3] == historical - timedelta(hours=1))
            self.assertEqual(missing[4:8], (None, None, None, None))
            self.assertTrue(all(row[2] == 'new' for row in result))
            future = {row[3]: row[8:] for row in result if row[3] > origin}
            self.assertEqual(future[origin + timedelta(hours=1)], (50, 51))
            self.assertEqual(future[origin + timedelta(hours=2)], (None, None))
            self.assertEqual(future[origin + timedelta(hours=3)], (53, None))
            self.assertEqual(future[origin + timedelta(hours=6)], (None, None))
            cur.execute("DROP TABLE ml_backtest")
            fallback = eval(sql_expression, {"backtest_available": False})
            for name in ("ml_prediction", "dim_location", "dim_time", "fact_air_quality"):
                fallback = fallback.replace(f"warehouse.{name}", name)
            cur.execute(fallback)
            selected = next(row for row in cur.fetchall() if row[0] == "Palu" and row[3] == historical)
            self.assertEqual(selected[4:8], (40, 20, None, None))


if __name__ == "__main__":
    unittest.main()
