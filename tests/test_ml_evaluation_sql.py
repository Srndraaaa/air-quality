"""Uji evaluasi per horizon di PostgreSQL tanpa menulis warehouse."""
from __future__ import annotations

import ast
import os
from datetime import datetime, timedelta
from pathlib import Path
import unittest

import psycopg2


SOURCE = Path(__file__).resolve().parents[1] / "airflow/dags/air_quality_ml.py"


def task_sql(task_name, fragment):
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    task = next(node for node in ast.walk(tree)
                if isinstance(node, ast.FunctionDef) and node.name == task_name)
    return next(node.value for node in ast.walk(task)
                if isinstance(node, ast.Constant) and isinstance(node.value, str)
                and fragment in node.value)


class EvaluationSQLTest(unittest.TestCase):
    def test_per_horizon_comparable_rows_and_retry(self):
        update = task_sql("evaluate_predictions", "UPDATE warehouse.ml_prediction")
        metrics = task_sql("evaluate_predictions", "INSERT INTO warehouse.ml_metrics")
        with psycopg2.connect(
            host=os.getenv("DW_HOST", "localhost"), port=os.getenv("DW_PORT", "5432"),
            dbname=os.getenv("DW_DB", "air_quality_dw"), user=os.getenv("DW_USER", "platform"),
            password=os.getenv("DW_PASSWORD", "platform123"),
        ) as conn, conn.cursor() as cur:
            cur.execute("CREATE TEMP TABLE ml_prediction (run_id text, model_version text, "
                        "horizon_hours int, city text, target_at timestamp, "
                        "predicted_pm2_5 float, persistence_pm2_5 float, cams_pm2_5 float, "
                        "actual_pm2_5 float, evaluated_at timestamptz)")
            cur.execute("CREATE TEMP TABLE ml_metrics (run_id text, model_version text, "
                        "horizon_hours int, n_predictions int, n_evaluated int, "
                        "mae_model float, mae_persistence float, mae_cams float, "
                        "updated_at timestamptz, PRIMARY KEY (run_id, horizon_hours))")
            cur.execute("CREATE TEMP TABLE fact_air_quality (location_key int, time_key int, pm2_5 float)")
            cur.execute("CREATE TEMP TABLE dim_location (location_key int, city text)")
            cur.execute("CREATE TEMP TABLE dim_time (time_key int, valid_at timestamp)")
            now = datetime(2026, 1, 10, 12)
            cur.execute("INSERT INTO dim_location VALUES (1, 'x')")
            cur.executemany("INSERT INTO dim_time VALUES (%s, %s)",
                            [(1, now - timedelta(hours=4)), (2, now - timedelta(hours=2))])
            cur.executemany("INSERT INTO fact_air_quality VALUES (1, %s, %s)", [(1, 10), (2, 20)])
            cur.executemany(
                "INSERT INTO ml_prediction VALUES (%s, 'v', %s, 'x', %s, %s, %s, %s, NULL, NULL)",
                [('r', 1, now - timedelta(hours=4), 11, 12, 13),
                 ('r', 1, now - timedelta(hours=4), 100, 100, None),
                 ('r', 6, now - timedelta(hours=4), 14, 15, 16),
                 ('r', 6, now - timedelta(hours=2), 20, 20, 20),
                 ('s', 3, now - timedelta(hours=2), 20, 20, 20)],
            )
            update = update.replace("warehouse.", "")
            metrics = metrics.replace("warehouse.", "")
            for _ in range(2):
                cur.execute(update, (now - timedelta(hours=3),))
                cur.execute(metrics)
            cur.execute("SELECT run_id, horizon_hours, n_predictions, n_evaluated, "
                        "mae_model, mae_persistence, mae_cams FROM ml_metrics "
                        "ORDER BY run_id, horizon_hours")
            self.assertEqual(cur.fetchall(), [
                ('r', 1, 2, 1, 1.0, 2.0, 3.0),
                ('r', 6, 2, 1, 4.0, 5.0, 6.0),
                ('s', 3, 1, 0, None, None, None),
            ])
            cur.execute("UPDATE fact_air_quality SET pm2_5 = NULL WHERE time_key = 1")
            cur.execute(update, (now - timedelta(hours=3),))
            cur.execute(metrics)
            cur.execute("SELECT n_evaluated, mae_model FROM ml_metrics WHERE run_id = 'r'")
            self.assertEqual(cur.fetchall(), [(0, None), (0, None)])

    def test_backtest_queries_only_six_hour_features(self):
        query = task_sql("simulate_backtest", "FROM warehouse.ml_features")
        self.assertIn("horizon_hours = 6", query)


if __name__ == "__main__":
    unittest.main()
