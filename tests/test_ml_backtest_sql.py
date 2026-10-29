"""Uji jangkar dan transaksi backtest di PostgreSQL tanpa menulis warehouse."""
from __future__ import annotations

import ast
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
import unittest
from zoneinfo import ZoneInfo

import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "airflow/dags"))
from ml_lib import BACKTEST_ANCHORS_SQL


class BacktestSQLTest(unittest.TestCase):
    def test_jangkar_dan_rollback_rebuild(self):
        source = Path(__file__).resolve().parents[1] / "airflow/dags/air_quality_ml.py"
        tree = ast.parse(source.read_text(encoding="utf-8"))
        ddl = next(n.value.value for n in tree.body if isinstance(n, ast.Assign)
                   and any(isinstance(t, ast.Name) and t.id == "DDL" for t in n.targets))
        table_ddl = ddl[ddl.index("CREATE TABLE IF NOT EXISTS warehouse.ml_backtest"):]
        table_ddl = table_ddl[:table_ddl.index(";") + 1]
        table_ddl = table_ddl.replace("CREATE TABLE IF NOT EXISTS warehouse.ml_backtest",
                                      "CREATE TEMP TABLE ml_backtest")
        with psycopg2.connect(
            host=os.getenv("DW_HOST", "localhost"), port=os.getenv("DW_PORT", "5432"),
            dbname=os.getenv("DW_DB", "air_quality_dw"), user=os.getenv("DW_USER", "platform"),
            password=os.getenv("DW_PASSWORD", "platform123"),
        ) as conn, conn.cursor() as cur:
            cur.execute("CREATE TEMP TABLE ml_prediction (city text, origin_at timestamp, "
                        "target_at timestamp, created_at timestamptz, prediction_id bigint, "
                        "horizon_hours integer)")
            now = datetime.now(ZoneInfo("Asia/Makassar")).replace(tzinfo=None)
            for city, origin, target, created, ident, horizon in [
                ("x", now - timedelta(hours=3), now + timedelta(hours=3), 1, 1, 6),
                ("x", now - timedelta(hours=2), now + timedelta(hours=4), 2, 2, 6),
                ("x", now - timedelta(hours=1), now - timedelta(hours=1), 3, 3, 6),
                ("y", now - timedelta(hours=1), now - timedelta(hours=1), 4, 4, 6),
                ("x", now, now + timedelta(hours=1), 5, 5, 1),
                ("z", now, now + timedelta(hours=1), 6, 6, 1),
            ]:
                cur.execute("INSERT INTO ml_prediction VALUES (%s, %s, %s, "
                            "'2026-01-01 UTC'::timestamptz + %s * INTERVAL '1 hour', %s, %s)",
                            (city, origin, target, created, ident, horizon))
            cur.execute(BACKTEST_ANCHORS_SQL.replace("warehouse.ml_prediction", "ml_prediction"))
            self.assertEqual(cur.fetchall(), [("x", now - timedelta(hours=2))])
            cur.execute(table_ddl)
            cur.execute("INSERT INTO ml_backtest (city, target_at, origin_at, predicted_pm2_5) "
                        "VALUES ('x', %s, %s, 1)", (now, now - timedelta(hours=6)))
            cur.execute("SAVEPOINT before_rebuild")
            cur.execute("TRUNCATE ml_backtest")
            cur.execute("INSERT INTO ml_backtest (city, target_at, origin_at, predicted_pm2_5) "
                        "VALUES ('x', %s, %s, 2)", (now, now - timedelta(hours=6)))
            with self.assertRaises(psycopg2.IntegrityError):
                cur.execute("INSERT INTO ml_backtest (city, target_at, origin_at, predicted_pm2_5) "
                            "VALUES ('x', %s, %s, 3)", (now, now - timedelta(hours=6)))
            cur.execute("ROLLBACK TO SAVEPOINT before_rebuild")
            cur.execute("SELECT city, predicted_pm2_5 FROM ml_backtest")
            self.assertEqual(cur.fetchall(), [("x", 1.0)])


if __name__ == "__main__":
    unittest.main()
