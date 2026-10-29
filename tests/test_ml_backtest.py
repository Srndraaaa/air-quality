"""Uji simulasi walk-forward tanpa Airflow atau database."""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "airflow/dags"))
import ml_lib


class BacktestTest(unittest.TestCase):
    def setUp(self):
        self.anchor = datetime(2026, 1, 10)
        self.origin = self.anchor - timedelta(hours=6)
        self.base = {name: 1.0 for name in ml_lib.FEATURE_COLUMNS}
        self.base.update(city="x", target_hour=0, target_dow="Monday", label_pm2_5=1.0)
        self.training = [self.row(self.origin - timedelta(hours=48 + i)) for i in range(24)]
        self.candidate = self.row(self.origin)

    def row(self, origin, **changes):
        return {**self.base, "origin_at": origin, "target_at": origin + timedelta(hours=6),
                **changes}

    def simulate(self, rows):
        return ml_lib.backtest_predictions(rows, {"x": self.anchor}, 6, 48, 24, 1.0, 168)

    def test_cutoff_minimum_missing_features_and_labels(self):
        too_recent = self.row(self.origin - timedelta(hours=47), label_pm2_5=999.0)
        future_label = self.row(self.origin - timedelta(hours=50),
                                target_at=self.origin + timedelta(hours=1), label_pm2_5=999.0)
        missing_label = self.row(self.origin - timedelta(hours=80), label_pm2_5=None)
        missing_feature = self.row(self.origin - timedelta(hours=81), lag_0=None)
        with patch.object(ml_lib, "fit_ridge", wraps=ml_lib.fit_ridge) as fit:
            result = self.simulate(self.training + [self.candidate, too_recent, future_label,
                                                    missing_label, missing_feature])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][:3], ("x", self.anchor, self.origin))
        self.assertEqual(fit.call_count, 1)
        self.assertEqual(len(fit.call_args.args[1]), 24)
        self.assertEqual(fit.call_args.kwargs["weights"][0], 0.5 ** (48 / 168))
        self.assertEqual(self.simulate(self.training[1:] + [self.candidate, too_recent]), [])
        self.assertEqual(self.simulate(self.training + [{**self.candidate, "lag_0": None}]), [])
        self.assertEqual(self.simulate(self.training + [{**self.candidate,
                                                        "target_at": self.anchor + timedelta(hours=1)}]), [])

    def test_retrain_per_target_with_its_own_cutoff(self):
        candidates = [self.row(self.origin - timedelta(hours=i)) for i in range(6)]
        extra = [self.row(self.origin - timedelta(hours=i)) for i in range(72, 77)]
        with patch.object(ml_lib, "fit_ridge", wraps=ml_lib.fit_ridge) as fit:
            result = self.simulate(self.training + extra + candidates)
        self.assertEqual(len(result), 6)
        self.assertEqual(fit.call_count, 6)
        self.assertEqual([len(call.args[1]) for call in fit.call_args_list], list(range(24, 30)))
        self.assertEqual([r[1] for r in result],
                         [self.anchor - timedelta(hours=i) for i in range(5, -1, -1)])


if __name__ == "__main__":
    unittest.main()
