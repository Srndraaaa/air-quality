"""Uji query scoring terhadap PostgreSQL tanpa menulis ke warehouse."""
from __future__ import annotations

import ast
import os
from datetime import datetime, timedelta
from pathlib import Path
import unittest
from zoneinfo import ZoneInfo

import psycopg2


class ScoringSQLTest(unittest.TestCase):
    def test_target_harus_masa_depan_saat_dipilih_dan_disimpan(self):
        source = Path(__file__).resolve().parents[1] / "airflow/dags/air_quality_ml.py"
        tree = ast.parse(source.read_text(encoding="utf-8"))
        score = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)
                     and n.name == "score_predictions")
        queries = [n.args[0] for n in ast.walk(score) if isinstance(n, ast.Call)
                   and isinstance(n.func, ast.Attribute) and n.func.attr == "execute" and n.args]
        selection = next(n for n in queries if isinstance(n, ast.JoinedStr)
                         and "warehouse.ml_features" in ast.get_source_segment(source.read_text(), n))
        insert = next(n.value for n in queries if isinstance(n, ast.Constant)
                      and isinstance(n.value, str) and "INSERT INTO warehouse.ml_prediction" in n.value)
        models_query = next(n.value for n in queries if isinstance(n, ast.Constant)
                            and isinstance(n.value, str) and "FROM warehouse.ml_model" in n.value)
        feature_columns = ["lag_0", "lag_1", "lag_2", "lag_3", "lag_6",
                           "roll_mean_6", "roll_mean_24", "pm10_0"]
        selection = eval(compile(ast.Expression(selection), str(source), "eval"), {
            "FEATURE_COLUMNS": feature_columns,
            "PURE_FEATURES": " AND ".join(f"{name} IS NOT NULL" for name in feature_columns),
        }).replace("warehouse.ml_features", "ml_features").replace(
            "warehouse.ml_prediction", "ml_prediction"
        )
        insert = insert.replace("warehouse.ml_prediction", "ml_prediction")

        with psycopg2.connect(
            host=os.getenv("DW_HOST", "localhost"), port=os.getenv("DW_PORT", "5432"),
            dbname=os.getenv("DW_DB", "air_quality_dw"), user=os.getenv("DW_USER", "platform"),
            password=os.getenv("DW_PASSWORD", "platform123"),
        ) as conn, conn.cursor() as cur:
            cur.execute("CREATE TEMP TABLE ml_features (city text, horizon_hours int, "
                        "origin_at timestamp, target_at timestamp, target_hour int, target_dow text, "
                        "label_pm2_5 float, "
                        + ", ".join(f"{name} float" for name in feature_columns) + ")")
            cur.execute("CREATE TEMP TABLE ml_prediction (run_id text, model_version text, "
                        "city text, horizon_hours int, origin_at timestamp, target_at timestamp, "
                        "predicted_pm2_5 float, persistence_pm2_5 float, cams_pm2_5 float, "
                        "created_at timestamptz, UNIQUE (run_id, city, target_at))")
            cur.execute("CREATE TEMP TABLE ml_model (city text, horizon_hours int, "
                        "model_version text, intercept float, coefficients jsonb, "
                        "feature_means jsonb, feature_stds jsonb, trained_at timestamp)")
            for horizon in range(1, 7):
                cur.execute("INSERT INTO ml_model VALUES ('kota', %s, 'lama', 0, '[]', '[]', "
                            "'[]', '2020-01-01'), ('kota', %s, 'baru', 0, '[]', '[]', "
                            "'[]', '2021-01-01')", (horizon, horizon))
            cur.execute(models_query.replace("warehouse.ml_model", "ml_model"))
            models = cur.fetchall()
            self.assertEqual([(row[1], row[2]) for row in models],
                             [(horizon, "baru") for horizon in range(1, 7)])
            now = datetime.now(ZoneInfo("Asia/Makassar")).replace(tzinfo=None)
            origin = now - timedelta(minutes=90)
            for age in (0, 1):
                candidate = origin - timedelta(hours=age)
                for horizon in range(1, 7):
                    cur.execute("INSERT INTO ml_features VALUES (%s, %s, %s, %s, 0, "
                                "'Friday', 1, " + ", ".join(["1"] * len(feature_columns)) + ")",
                                ("kota", horizon, candidate, candidate + timedelta(hours=horizon)))
            cur.execute(selection, (now, "run"))
            rows = cur.fetchall()
            self.assertEqual([row[1] for row in rows], [2, 3, 4, 5, 6])
            self.assertTrue(all(row[2] == origin for row in rows))

            target = origin + timedelta(hours=2)
            cur.execute(insert, ("run", "model", "kota", 2, origin, target, 1, 1, 7, target))
            self.assertEqual(cur.rowcount, 1)
            cur.execute(insert, ("run", "model", "kota", 2, origin, target, 2, 1, 9, target))
            self.assertEqual(cur.rowcount, 0)
            cur.execute("SELECT COUNT(*), MAX(cams_pm2_5) FROM ml_prediction")
            self.assertEqual(cur.fetchone(), (1, 7))
            expired = now - timedelta(seconds=1)
            cur.execute(insert, ("run", "model", "kota", 1, origin, expired, 1, 1, 1, expired))
            self.assertEqual(cur.rowcount, 0)

            newer = now - timedelta(minutes=30)
            cur.execute("INSERT INTO ml_features VALUES (%s, 6, %s, %s, 0, 'Friday', 1, "
                        + ", ".join(["1"] * len(feature_columns)) + ")",
                        ("kota", newer, newer + timedelta(hours=6)))
            cur.execute(selection, (now, "run"))
            self.assertTrue(all(row[2] == origin for row in cur.fetchall()))
            for horizon in range(1, 6):
                cur.execute("INSERT INTO ml_features VALUES (%s, %s, %s, %s, 0, 'Friday', 1, "
                            + ", ".join(["1"] * len(feature_columns)) + ")",
                            ("kota", horizon, newer, newer + timedelta(hours=horizon)))
            cur.execute(selection, (now, "run-baru"))
            rows = cur.fetchall()
            self.assertEqual([row[1] for row in rows], list(range(1, 7)))
            self.assertTrue(all(row[2] == newer for row in rows))
            for row in rows:
                cur.execute(insert, ("run-baru", "baru", row[0], row[1], row[2], row[3],
                                     row[1], 1, None, row[3]))
            cur.execute("SELECT COUNT(DISTINCT horizon_hours), COUNT(DISTINCT run_id) "
                        "FROM ml_prediction WHERE run_id = 'run-baru'")
            self.assertEqual(cur.fetchone(), (6, 1))
            row = rows[0]
            cur.execute(insert, ("run-baru", "baru", row[0], row[1], row[2], row[3],
                                 5, 1, 10, row[3]))
            cur.execute("SELECT cams_pm2_5 FROM ml_prediction "
                        "WHERE run_id = 'run-baru' AND horizon_hours = 1")
            self.assertIsNone(cur.fetchone()[0])

            train = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)
                         and n.name == "train_model")
            train_query = next(n for n in ast.walk(train) if isinstance(n, ast.Call)
                               and isinstance(n.func, ast.Attribute) and n.func.attr == "execute"
                               and n.args and isinstance(n.args[0], ast.JoinedStr)
                               and "origin_at <= %s" in ast.get_source_segment(source.read_text(), n))
            self.assertEqual([n.id for n in train_query.args[1].elts], ["cutoff", "cutoff"])
            query = eval(compile(ast.Expression(train_query.args[0]), str(source), "eval"), {
                "FEATURE_COLUMNS": feature_columns,
                "PURE_FEATURES": " AND ".join(f"{name} IS NOT NULL" for name in feature_columns),
            }).replace("warehouse.ml_features", "ml_features")
            cutoff = now - timedelta(hours=48)
            for horizon in (1, 6):
                for offset in (0, 1):
                    target = cutoff + timedelta(hours=offset)
                    cur.execute("INSERT INTO ml_features VALUES (%s, %s, %s, %s, 0, "
                                "'Friday', 1, " + ", ".join(["1"] * len(feature_columns)) + ")",
                                ("latih", horizon, target - timedelta(hours=horizon), target))
            cur.execute(query, (cutoff, cutoff))
            rows = cur.fetchall()
            self.assertEqual({row[1] for row in rows}, {1, 6})
            self.assertEqual(len(rows), 2)
            self.assertTrue(all(row[3] == cutoff for row in rows))


if __name__ == "__main__":
    unittest.main()
