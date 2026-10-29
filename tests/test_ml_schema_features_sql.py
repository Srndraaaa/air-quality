"""Uji migrasi dan fitur multi-horizon dalam schema PostgreSQL sementara."""
from __future__ import annotations

import ast
import os
from datetime import datetime, timedelta
from pathlib import Path
import unittest
from uuid import uuid4

import psycopg2


source = Path(__file__).resolve().parents[1] / "airflow/dags/air_quality_ml.py"
tree = ast.parse(source.read_text(encoding="utf-8"))
constants = {node.targets[0].id: node.value.value for node in tree.body
             if isinstance(node, ast.Assign) and len(node.targets) == 1
             and isinstance(node.targets[0], ast.Name) and isinstance(node.value, ast.Constant)
             and isinstance(node.value.value, str)}


class SchemaFeaturesSQLTest(unittest.TestCase):
    def setUp(self):
        self.conn = psycopg2.connect(
            host=os.getenv("DW_HOST", "localhost"), port=os.getenv("DW_PORT", "5432"),
            dbname=os.getenv("DW_DB", "air_quality_dw"), user=os.getenv("DW_USER", "platform"),
            password=os.getenv("DW_PASSWORD", "platform123"),
        )
        self.cur = self.conn.cursor()
        self.schema = "ml_test_" + uuid4().hex
        self.cur.execute(f"CREATE SCHEMA {self.schema}")

    def tearDown(self):
        self.conn.rollback()
        self.conn.close()

    def sql(self, value):
        return value.replace("warehouse.", self.schema + ".")

    def test_migrasi_lama_dan_fresh_idempotent(self):
        ddl = constants["DDL"]
        legacy = ddl[:ddl.index("DO $$")]
        legacy = legacy.replace("    horizon_hours INT NOT NULL DEFAULT 6,\n", "")
        legacy = legacy.replace("PRIMARY KEY (city, origin_at, horizon_hours)",
                                "PRIMARY KEY (city, origin_at)")
        legacy = legacy.replace("PRIMARY KEY (model_version, city, horizon_hours)",
                                "PRIMARY KEY (model_version, city)")
        legacy = legacy.replace("run_id TEXT NOT NULL,\n    model_version TEXT,",
                                "run_id TEXT PRIMARY KEY,\n    model_version TEXT,")
        legacy = legacy.replace("updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),\n"
                                "    PRIMARY KEY (run_id, horizon_hours)",
                                "updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()")
        self.cur.execute(self.sql(legacy))
        self.cur.execute(self.sql("INSERT INTO warehouse.ml_features "
                         "(city, origin_at, target_at, target_hour, target_dow) "
                         "VALUES ('x', '2026-01-01', '2026-01-01 06:00', 6, 'Thursday')"))
        self.cur.execute(self.sql("INSERT INTO warehouse.ml_prediction "
                         "(run_id, model_version, city, origin_at, target_at) VALUES "
                         "('r', 'm', 'x', '2026-01-01', '2026-01-01 06:00'), "
                         "('r', 'm', 'x', '2026-01-01 01:00', '2026-01-01 07:00')"))
        self.cur.execute(self.sql("INSERT INTO warehouse.ml_model "
                         "(model_version, city, horizon_hours, feature_names, coefficients, "
                         "intercept, feature_means, feature_stds, ridge_lambda, train_rows) "
                         "VALUES ('m', 'x', 6, '[]', '[]', 1, '[]', '[]', 1, 24)"))
        self.cur.execute(self.sql("INSERT INTO warehouse.ml_metrics "
                         "(run_id, horizon_hours, n_predictions, n_evaluated) VALUES ('r', 6, 2, 0)"))
        for _ in range(2):
            self.cur.execute(self.sql(ddl))
        self.cur.execute(self.sql("SELECT horizon_hours FROM warehouse.ml_prediction ORDER BY target_at"))
        self.assertEqual(self.cur.fetchall(), [(6,), (6,)])
        self.cur.execute(self.sql("SELECT horizon_hours FROM warehouse.ml_features"))
        self.assertEqual(self.cur.fetchall(), [(6,)])
        self.cur.execute(self.sql("SELECT horizon_hours, intercept FROM warehouse.ml_model"))
        self.assertEqual(self.cur.fetchall(), [(6, 1)])
        self.cur.execute(self.sql("SELECT horizon_hours, n_predictions FROM warehouse.ml_metrics"))
        self.assertEqual(self.cur.fetchall(), [(6, 2)])
        for table, expected in [("ml_features", "PRIMARY KEY (city, origin_at, horizon_hours)"),
                                ("ml_model", "PRIMARY KEY (model_version, city, horizon_hours)"),
                                ("ml_metrics", "PRIMARY KEY (run_id, horizon_hours)")]:
            self.cur.execute("SELECT pg_get_constraintdef(oid) FROM pg_constraint "
                             "WHERE conrelid = %s::regclass AND contype = 'p'",
                             (f"{self.schema}.{table}",))
            self.assertEqual(self.cur.fetchone()[0], expected)
        self.cur.execute(self.sql("INSERT INTO warehouse.ml_features "
                         "(city, origin_at, horizon_hours, target_at, target_hour, target_dow) "
                         "VALUES ('x', '2026-01-01', 1, '2026-01-01 01:00', 1, 'Thursday')"))
        self.cur.execute(self.sql("INSERT INTO warehouse.ml_model "
                         "(model_version, city, horizon_hours, feature_names, coefficients, "
                         "intercept, feature_means, feature_stds, ridge_lambda, train_rows) "
                         "VALUES ('m', 'x', 1, '[]', '[]', 1, '[]', '[]', 1, 24)"))
        self.cur.execute(self.sql("INSERT INTO warehouse.ml_metrics "
                         "(run_id, horizon_hours, n_predictions, n_evaluated) VALUES ('r', 1, 1, 0)"))
        self.cur.execute(f"CREATE SCHEMA {self.schema}_fresh")
        fresh = ddl.replace("warehouse.", self.schema + "_fresh.")
        self.cur.execute(fresh)
        self.cur.execute(fresh)
        self.cur.execute("SELECT pg_get_constraintdef(oid) FROM pg_constraint "
                         "WHERE conrelid = %s::regclass AND contype = 'p'",
                         (f"{self.schema}_fresh.ml_metrics",))
        self.assertEqual(self.cur.fetchone()[0], "PRIMARY KEY (run_id, horizon_hours)")

    def test_fitur_enam_horizon_tanpa_kebocoran(self):
        self.cur.execute(self.sql(constants["DDL"]))
        self.cur.execute(self.sql("CREATE TABLE warehouse.dim_location "
                         "(location_key int PRIMARY KEY, city text)"))
        self.cur.execute(self.sql("CREATE TABLE warehouse.dim_time "
                         "(time_key bigint PRIMARY KEY, valid_at timestamp)"))
        self.cur.execute(self.sql("CREATE TABLE warehouse.fact_air_quality "
                         "(location_key int, time_key bigint, pm2_5 float, pm10 float, "
                         "PRIMARY KEY (location_key, time_key))"))
        self.cur.execute(self.sql("INSERT INTO warehouse.dim_location VALUES (1, 'x')"))
        origin = datetime(2026, 1, 1, 23)
        for delta, value in [(-6, 4), (-2, 8), (0, 12), (1, 100), (3, 200)]:
            stamp = origin + timedelta(hours=delta)
            key = int(stamp.strftime("%Y%m%d%H"))
            self.cur.execute(self.sql("INSERT INTO warehouse.dim_time VALUES (%s, %s)"), (key, stamp))
            self.cur.execute(self.sql("INSERT INTO warehouse.fact_air_quality VALUES (1, %s, %s, 9)"),
                             (key, value))
        self.cur.execute(self.sql(constants["FEATURES_SQL"]))
        self.cur.execute(self.sql("SELECT horizon_hours, target_at, target_hour, target_dow, "
                         "lag_0, lag_1, lag_2, lag_6, roll_mean_6, roll_mean_24, label_pm2_5 "
                         "FROM warehouse.ml_features WHERE origin_at = %s ORDER BY horizon_hours"),
                         (origin,))
        rows = self.cur.fetchall()
        self.assertEqual(len(rows), 6)
        for h, target, hour, day, lag0, lag1, lag2, lag6, roll6, roll24, label in rows:
            self.assertEqual((target, hour, day), (origin + timedelta(hours=h), h - 1, "Friday"))
            self.assertEqual((lag0, lag1, lag2, lag6, roll6, roll24),
                             (12, None, 8, 4, 10, 8))
            self.assertEqual(label, {1: 100, 3: 200}.get(h))
        self.cur.execute(self.sql("TRUNCATE warehouse.ml_features"))
        self.cur.execute(self.sql(constants["FEATURES_SQL"]))
        self.cur.execute(self.sql("SELECT COUNT(*) FROM warehouse.ml_features WHERE origin_at = %s"),
                         (origin,))
        self.assertEqual(self.cur.fetchone()[0], 6)


if __name__ == "__main__":
    unittest.main()
