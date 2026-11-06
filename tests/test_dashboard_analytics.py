"""Uji perilaku analitik dashboard tanpa Streamlit atau database."""
from __future__ import annotations

import ast
from datetime import datetime, timedelta
from pathlib import Path
import unittest
from unittest.mock import MagicMock

import altair as alt

import pandas as pd


def load_analytics():
    source = (Path(__file__).resolve().parents[1] / "dashboard/app.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    functions = [node for node in tree.body if isinstance(node, ast.FunctionDef)
                 and (node.name.startswith("analytics_") or node.name in
                      {"aggregate_heatmap", "render_detailed_analytics"})]
    module = ast.Module(body=functions, type_ignores=[])
    namespace = {"pd": pd, "datetime": datetime, "timedelta": timedelta, "alt": alt}
    exec(compile(module, "<dashboard analytics>", "exec", dont_inherit=False), namespace)
    return namespace


class DashboardAnalyticsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.analytics = load_analytics()

    def setUp(self):
        self.now = datetime(2026, 2, 15, 12)

    def test_periods_are_equal_length_and_include_current_hour(self):
        periods = self.analytics["analytics_periods"]
        for days in (7, 14, 30):
            with self.subTest(days=days):
                previous, current, end = periods(self.now, days)
                self.assertEqual(end, datetime(2026, 2, 15, 13))
                self.assertEqual(end - current, timedelta(days=days))
                self.assertEqual(current - previous, timedelta(days=days))

    def test_summary_boundaries_forecast_city_and_pollutant(self):
        data = pd.DataFrame([
            ("Palu", datetime(2026, 2, 1, 12), 900, 900),
            ("Palu", datetime(2026, 2, 1, 13), 10, 100),
            ("Palu", datetime(2026, 2, 8, 12), 30, None),
            ("Palu", datetime(2026, 2, 8, 13), 20, 200),
            ("Palu", datetime(2026, 2, 15, 11), None, 400),
            ("Palu", datetime(2026, 2, 15, 12), 40, None),
            ("Palu", datetime(2026, 2, 15, 13), 999, 999),
            ("Manado", datetime(2026, 2, 15, 12), 80, 800),
            ("Kendari", datetime(2026, 2, 15, 12), 1000, 1000),
        ], columns=["city", "valid_at", "pm2_5", "pm10"])
        summary = self.analytics["analytics_summary"]
        pm25 = summary(data, ["Palu", "Manado"], "pm2_5", self.now, 7)
        rows = pm25.set_index(["city", "period"])
        self.assertEqual(set(rows.index), {("Palu", "Kini"), ("Palu", "Sebelumnya"),
                                           ("Manado", "Kini"), ("Manado", "Sebelumnya")})
        self.assertEqual(rows.loc[("Palu", "Kini"), "valid"], 2)
        self.assertEqual(rows.loc[("Palu", "Kini"), "mean"], 30)
        self.assertEqual(rows.loc[("Palu", "Sebelumnya"), "valid"], 2)
        self.assertEqual(rows.loc[("Palu", "Sebelumnya"), "mean"], 20)
        self.assertEqual(rows.loc[("Manado", "Kini"), "mean"], 80)
        self.assertEqual(rows.loc[("Manado", "Sebelumnya"), "valid"], 0)
        self.assertTrue(pd.isna(rows.loc[("Manado", "Sebelumnya"), "mean"]))
        self.assertAlmostEqual(rows.loc[("Palu", "Kini"), "coverage"], 2 / 168)
        self.assertEqual(rows.loc[("Manado", "Sebelumnya"), "coverage"], 0)
        self.assertTrue((rows["expected"] == 168).all())

        pm10 = summary(data, ["Palu"], "pm10", self.now, 7).set_index(["city", "period"])
        self.assertEqual(pm10.loc[("Palu", "Kini"), "valid"], 2)
        self.assertEqual(pm10.loc[("Palu", "Kini"), "mean"], 300)
        self.assertEqual(pm10.loc[("Palu", "Sebelumnya"), "valid"], 1)
        self.assertEqual(pm10.loc[("Palu", "Sebelumnya"), "mean"], 100)
        self.assertAlmostEqual(pm10.loc[("Palu", "Sebelumnya"), "coverage"], 1 / 168)

    def test_empty_period_keeps_selected_cities_and_expected_hours(self):
        data = pd.DataFrame(columns=["city", "valid_at", "pm2_5", "pm10"])
        summary = self.analytics["analytics_summary"]
        for days in (7, 14, 30):
            with self.subTest(days=days):
                rows = summary(data, ["Palu", "Manado"], "pm2_5", self.now, days)
                self.assertEqual(len(rows), 4)
                self.assertEqual(set(rows["city"]), {"Palu", "Manado"})
                self.assertEqual(set(rows["period"]), {"Kini", "Sebelumnya"})
                self.assertTrue((rows["valid"] == 0).all())
                self.assertTrue((rows["expected"] == 24 * days).all())
                self.assertTrue(rows["mean"].isna().all())
                self.assertTrue((rows["coverage"] == 0).all())

    def test_patterns_separate_dates_hours_cities_and_valid_values(self):
        data = pd.DataFrame([
            ("Palu", "2026-02-08 01:00", 10, 100),
            ("Palu", "2026-02-08 02:00", None, 200),
            ("Palu", "2026-02-09 01:00", 30, None),
            ("Palu", "2026-02-09 02:00", 50, 400),
            ("Manado", "2026-02-08 01:00", 90, 900),
        ], columns=["city", "valid_at", "pm2_5", "pm10"])
        data["valid_at"] = pd.to_datetime(data["valid_at"])
        patterns = self.analytics["analytics_patterns"]
        hourly, daily = patterns(data, "pm2_5")
        hourly = hourly.set_index(["city", "Jam"])
        daily = daily.set_index(["city", "Tanggal"])
        self.assertEqual(hourly.loc[("Palu", 1), "mean"], 20)
        self.assertEqual(hourly.loc[("Palu", 1), "valid"], 2)
        self.assertEqual(hourly.loc[("Palu", 2), "mean"], 50)
        self.assertEqual(hourly.loc[("Palu", 2), "valid"], 1)
        self.assertEqual(hourly.loc[("Manado", 1), "mean"], 90)
        self.assertEqual(daily.loc[("Palu", pd.Timestamp("2026-02-08")), "mean"], 10)
        self.assertEqual(daily.loc[("Palu", pd.Timestamp("2026-02-08")), "valid"], 1)
        self.assertEqual(daily.loc[("Palu", pd.Timestamp("2026-02-09")), "mean"], 40)
        self.assertEqual(daily.loc[("Palu", pd.Timestamp("2026-02-09")), "valid"], 2)
        hourly_pm10, _ = patterns(data, "pm10")
        hourly_pm10 = hourly_pm10.set_index(["city", "Jam"])
        self.assertEqual(hourly_pm10.loc[("Palu", 2), "mean"], 300)
        self.assertEqual(hourly_pm10.loc[("Palu", 2), "valid"], 2)

    def test_exceedance_uses_only_valid_hours_as_denominator(self):
        data = pd.DataFrame([
            ("Palu", "2026-02-15 00:00", 999, 999),
            ("Palu", "2026-02-15 01:00", 50, 100),
            ("Palu", "2026-02-15 02:00", 51, None),
            ("Palu", "2026-02-15 03:00", None, 0),
            ("Palu", "2026-02-15 04:00", 90, 100),
            ("Palu", "2026-02-15 05:00", 999, 999),
            ("Manado", "2026-02-15 02:00", None, None),
            ("Kendari", "2026-02-15 02:00", 999, 999),
        ], columns=["city", "valid_at", "pm2_5", "pm10"])
        data["valid_at"] = pd.to_datetime(data["valid_at"])
        exceedance = self.analytics["analytics_exceedance"]
        start, end = datetime(2026, 2, 15, 1), datetime(2026, 2, 15, 5)
        result = exceedance(data, ["Palu", "Manado"], "pm2_5", start, end, 50)
        rows = result.set_index("city")
        self.assertEqual(set(rows.index), {"Palu", "Manado"})
        self.assertEqual(rows.loc["Palu", "above"], 2)
        self.assertEqual(rows.loc["Palu", "valid"], 3)
        self.assertAlmostEqual(rows.loc["Palu", "percent"], 200 / 3)
        self.assertEqual(rows.loc["Manado", "above"], 0)
        self.assertEqual(rows.loc["Manado", "valid"], 0)
        self.assertTrue(pd.isna(rows.loc["Manado", "percent"]))
        pm10 = exceedance(data, ["Palu"], "pm10", start, end, 50).set_index("city")
        self.assertEqual(pm10.loc["Palu", "above"], 2)
        self.assertEqual(pm10.loc["Palu", "valid"], 3)
        self.assertAlmostEqual(pm10.loc["Palu", "percent"], 200 / 3)

    def test_heatmap_groups_weekdays_hours_city_and_pollutant(self):
        data = pd.DataFrame([
            ("Palu", "2026-02-09 00:00", 0, 100),
            ("Palu", "2026-02-16 00:00", 20, None),
            ("Palu", "2026-02-23 00:00", None, 300),
            ("Palu", "2026-02-10 00:00", 40, 400),
            ("Palu", "2026-02-15 23:00", 60, 600),
            ("Manado", "2026-02-09 00:00", 999, 999),
        ], columns=["city", "valid_at", "pm2_5", "pm10"])
        data["valid_at"] = pd.to_datetime(data["valid_at"])
        aggregate = self.analytics["aggregate_heatmap"]
        pm25 = aggregate(data, "Palu", "pm2_5")
        self.assertEqual(list(pm25.columns), ["day", "hour", "mean", "valid_hours"])
        self.assertEqual(len(pm25), 168)
        self.assertEqual(set(map(tuple, pm25[["day", "hour"]].to_numpy())),
                         {(day, hour) for day in range(7) for hour in range(24)})
        self.assertTrue(pd.api.types.is_integer_dtype(pm25["day"]))
        self.assertTrue(pd.api.types.is_integer_dtype(pm25["hour"]))
        rows = pm25.set_index(["day", "hour"])
        self.assertEqual(rows.loc[(0, 0), "mean"], 10)
        self.assertEqual(rows.loc[(0, 0), "valid_hours"], 2)
        self.assertEqual(rows.loc[(1, 0), "mean"], 40)
        self.assertEqual(rows.loc[(6, 23), "mean"], 60)
        self.assertTrue(pd.isna(rows.loc[(0, 1), "mean"]))
        self.assertEqual(rows.loc[(0, 1), "valid_hours"], 0)

        pm10 = aggregate(data, "Palu", "pm10").set_index(["day", "hour"])
        self.assertEqual(pm10.loc[(0, 0), "mean"], 200)
        self.assertEqual(pm10.loc[(0, 0), "valid_hours"], 2)
        manado = aggregate(data, "Manado", "pm2_5").set_index(["day", "hour"])
        self.assertEqual(manado.loc[(0, 0), "mean"], 999)
        self.assertEqual(manado.loc[(0, 0), "valid_hours"], 1)

    def test_heatmap_all_null_missing_city_and_empty_data_keep_empty_grid(self):
        data = pd.DataFrame([
            ("Palu", "2026-02-09 01:00", None, 0),
            ("Manado", "2026-02-09 02:00", 5, None),
        ], columns=["city", "valid_at", "pm2_5", "pm10"])
        data["valid_at"] = pd.to_datetime(data["valid_at"])
        aggregate = self.analytics["aggregate_heatmap"]
        for frame, city in ((data, "Palu"), (data, "Kendari"), (data.iloc[0:0], "Palu")):
            with self.subTest(city=city, size=len(frame)):
                result = aggregate(frame, city, "pm2_5")
                self.assertEqual(len(result), 168)
                self.assertEqual(set(map(tuple, result[["day", "hour"]].to_numpy())),
                                 {(day, hour) for day in range(7) for hour in range(24)})
                self.assertTrue(result["mean"].isna().all())
                self.assertTrue((result["valid_hours"] == 0).all())
        zero = aggregate(data, "Palu", "pm10").set_index(["day", "hour"])
        self.assertEqual(zero.loc[(0, 1), "mean"], 0)
        self.assertEqual(zero.loc[(0, 1), "valid_hours"], 1)

    def test_heatmap_current_period_includes_start_and_current_hour_but_not_end(self):
        data = pd.DataFrame([
            ("Palu", "2026-02-08 12:00", 900, 900),
            ("Palu", "2026-02-08 13:00", 10, 100),
            ("Palu", "2026-02-15 12:00", 20, 200),
            ("Palu", "2026-02-15 13:00", 999, 999),
        ], columns=["city", "valid_at", "pm2_5", "pm10"])
        data["valid_at"] = pd.to_datetime(data["valid_at"])
        _, start, end = self.analytics["analytics_periods"](self.now, 7)
        current = data.loc[(data["valid_at"] >= start) & (data["valid_at"] < end)]
        rows = self.analytics["aggregate_heatmap"](current, "Palu", "pm2_5")
        rows = rows.set_index(["day", "hour"])
        self.assertEqual(rows.loc[(6, 13), "mean"], 10)
        self.assertEqual(rows.loc[(6, 13), "valid_hours"], 1)
        self.assertEqual(rows.loc[(6, 12), "mean"], 20)
        self.assertEqual(rows.loc[(6, 12), "valid_hours"], 1)
        self.assertEqual(rows["valid_hours"].sum(), 2)

    def test_distribution_uses_valid_values_and_selected_pollutant(self):
        data = pd.DataFrame([
            {"city": "Palu", "pm2_5": value, "pm10": other}
            for value, other in [(0, 10), (2, None), (4, 30), (6, 50), (None, 70)]
        ] + [{"city": "Manado", "pm2_5": 99, "pm10": 99}])
        result = self.analytics["analytics_distribution"](data, ["Palu", "Mamuju"], "pm2_5")
        self.assertEqual(result["city"].tolist(), ["Palu"])
        row = result.iloc[0]
        self.assertEqual(row["valid_hours"], 4)
        self.assertEqual((row["minimum"], row["q1"], row["median"], row["q3"], row["maximum"]),
                         (0, 1.5, 3, 4.5, 6))
        pm10 = self.analytics["analytics_distribution"](data, ["Palu"], "pm10").iloc[0]
        self.assertEqual((pm10["valid_hours"], pm10["median"]), (4, 40))

    def test_distribution_single_value_and_no_valid_values(self):
        data = pd.DataFrame([
            {"city": "Palu", "pm2_5": 7},
            {"city": "Mamuju", "pm2_5": None},
        ])
        aggregate = self.analytics["analytics_distribution"]
        row = aggregate(data, ["Palu", "Mamuju", "Manado"], "pm2_5").iloc[0]
        self.assertEqual(row["valid_hours"], 1)
        self.assertTrue(all(row[key] == 7 for key in ("minimum", "q1", "median", "q3", "maximum")))
        self.assertTrue(aggregate(data, ["Mamuju", "Manado"], "pm2_5").empty)
        self.assertTrue(aggregate(data.iloc[0:0], ["Palu"], "pm2_5").empty)

    def test_distribution_fences_are_inclusive_and_whiskers_are_observations(self):
        values = [-4, -3, 0, 0, 0, 2, 2, 2, 5, 6]
        times = pd.date_range("2026-02-15 00:00", periods=len(values), freq="h")
        data = pd.DataFrame({"city": "Palu", "valid_at": times, "pm2_5": values})
        distribution = self.analytics["analytics_distribution"](data, ["Palu"], "pm2_5")
        row = distribution.iloc[0]
        self.assertEqual((row["q1"], row["median"], row["q3"]), (0, 1, 2))
        self.assertEqual((row["minimum"], row["maximum"], row["valid_hours"]), (-3, 5, 10))
        outliers = self.analytics["analytics_distribution_outliers"](
            data, distribution, "pm2_5"
        )
        self.assertEqual(
            list(outliers[["city", "valid_at", "pm2_5"]].itertuples(index=False, name=None)),
            [("Palu", times[0], -4), ("Palu", times[9], 6)],
        )

    def test_distribution_zero_iqr_keeps_each_outlier_timestamp_and_excludes_null(self):
        times = pd.date_range("2026-02-15 00:00", periods=10, freq="h")
        data = pd.DataFrame({
            "city": ["Palu"] * 10, "valid_at": times,
            "pm2_5": [7, 7, 7, 7, 7, 7, 7, 9, 9, None],
        })
        distribution = self.analytics["analytics_distribution"](data, ["Palu"], "pm2_5")
        row = distribution.iloc[0]
        self.assertEqual(
            tuple(row[key] for key in ("minimum", "q1", "median", "q3", "maximum")),
            (7, 7, 7, 7, 7),
        )
        self.assertEqual(row["valid_hours"], 9)
        outliers = self.analytics["analytics_distribution_outliers"](
            data, distribution, "pm2_5"
        )
        self.assertEqual(
            list(outliers[["city", "valid_at", "pm2_5"]].itertuples(index=False, name=None)),
            [("Palu", times[7], 9), ("Palu", times[8], 9)],
        )

    def test_distribution_current_period_separates_cities_and_pollutants(self):
        times = pd.to_datetime([
            "2026-02-08 12:00", "2026-02-08 13:00", "2026-02-09 00:00",
            "2026-02-10 00:00", "2026-02-11 00:00", "2026-02-12 00:00",
            "2026-02-13 00:00", "2026-02-14 00:00", "2026-02-15 12:00",
            "2026-02-15 13:00",
        ])
        data = pd.DataFrame({
            "city": ["Palu"] * 10,
            "valid_at": times,
            "pm2_5": [999, 1, 1, 1, 1, 1, 1, 50, None, 999],
            "pm10": [999, 10, 10, 10, 10, 10, 10, 10, 99, 999],
        })
        data = pd.concat([data, pd.DataFrame({
            "city": ["Manado", "Kendari"], "valid_at": [times[2], times[2]],
            "pm2_5": [100, 1000], "pm10": [200, 1000],
        })], ignore_index=True)
        _, start, end = self.analytics["analytics_periods"](self.now, 7)
        current = data.loc[
            data["city"].isin(["Palu", "Manado"])
            & (data["valid_at"] >= start) & (data["valid_at"] < end)
        ]
        distribution = self.analytics["analytics_distribution"]
        find_outliers = self.analytics["analytics_distribution_outliers"]
        pm25 = distribution(current, ["Palu", "Manado"], "pm2_5")
        self.assertEqual(pm25.set_index("city").loc["Palu", "valid_hours"], 7)
        outliers = find_outliers(current, pm25, "pm2_5")
        self.assertEqual(
            list(outliers[["city", "valid_at", "pm2_5"]].itertuples(index=False, name=None)),
            [("Palu", times[7], 50)],
        )
        pm10 = distribution(current, ["Palu"], "pm10")
        outliers = find_outliers(current, pm10, "pm10")
        self.assertEqual(
            list(outliers[["city", "valid_at", "pm10"]].itertuples(index=False, name=None)),
            [("Palu", times[8], 99)],
        )

    def test_boxplot_follows_pollutant_and_centers_each_city_outlier(self):
        times = pd.date_range("2026-02-10 00:00", periods=13, freq="h")
        data = pd.DataFrame([
            (city, timestamp, pm25, pm10)
            for city, pm25_values, pm10_values in (
                ("Palu", [1] * 10 + [40, 50, 50], [10] * 12 + [90]),
                ("Manado", [2] * 12 + [60], [20] * 11 + [80, 80]),
                ("Kendari", [3] * 12 + [99], [30] * 12 + [99]),
            )
            for timestamp, pm25, pm10 in zip(times, pm25_values, pm10_values)
        ], columns=["city", "valid_at", "pm2_5", "pm10"])
        selected = ["Manado", "Palu"]
        expected = {
            "PM2.5": ("pm2_5", [
                ("Palu", times[10], 40, "10/02/2026 10:00"),
                ("Palu", times[11], 50, "10/02/2026 11:00, 10/02/2026 12:00"),
                ("Palu", times[12], 50, "10/02/2026 11:00, 10/02/2026 12:00"),
                ("Manado", times[12], 60, "10/02/2026 12:00"),
            ]),
            "PM10": ("pm10", [
                ("Palu", times[12], 90, "10/02/2026 12:00"),
                ("Manado", times[11], 80, "10/02/2026 11:00, 10/02/2026 12:00"),
                ("Manado", times[12], 80, "10/02/2026 11:00, 10/02/2026 12:00"),
            ]),
        }
        for label, (pollutant, observations) in expected.items():
            with self.subTest(pollutant=label):
                st = MagicMock()
                filters = [MagicMock(), MagicMock(), MagicMock()]
                filters[0].multiselect.return_value = selected
                filters[1].selectbox.return_value = 7
                filters[2].selectbox.return_value = label
                st.columns.side_effect = [filters, [MagicMock(), MagicMock()]]
                st.selectbox.return_value = selected[0]
                st.number_input.return_value = 35.0
                self.analytics["render_detailed_analytics"].__globals__["st"] = st
                self.analytics["render_detailed_analytics"](data, selected, self.now)

                calls = [call for call in st.altair_chart.call_args_list
                         if call.kwargs.get("key") == f"distribution_{pollutant}"]
                self.assertEqual(len(calls), 1)
                chart = calls[0].args[0]
                spec = chart.to_dict()
                points = next(layer for layer in spec["layer"]
                              if layer["mark"]["type"] == "point")
                box = next(layer for layer in spec["layer"]
                           if layer["mark"]["type"] == "bar")
                self.assertEqual(points["encoding"]["x"]["field"], "city")
                self.assertEqual(points["encoding"]["x"]["sort"], selected)
                self.assertEqual(points["encoding"]["x"], box["encoding"]["x"])
                self.assertNotIn("xOffset", points["encoding"])
                self.assertEqual(points["encoding"]["y"]["field"], pollutant)
                self.assertEqual(points["encoding"]["y"]["title"], f"{label} (µg/m³)")
                tooltip = {item["field"]: item for item in points["encoding"]["tooltip"]}
                self.assertEqual(tooltip[pollutant]["title"], f"{label} (µg/m³)")
                self.assertIn("city", tooltip)
                self.assertIn("waktu_wita", tooltip)
                for layer in spec["layer"]:
                    self.assertEqual(layer["encoding"]["y"]["title"], f"{label} (µg/m³)")
                box_tooltip = {item["field"]: item for item in box["encoding"]["tooltip"]}
                for field in ("minimum", "q1", "median", "q3", "maximum"):
                    self.assertIn(label, box_tooltip[field]["title"])
                point_data = spec["datasets"][points["data"]["name"]]
                self.assertCountEqual(
                    [(row["city"], pd.Timestamp(row["valid_at"]), row[pollutant], row["waktu_wita"])
                     for row in point_data],
                    observations,
                )
                self.assertFalse(any(
                    "Kotak menunjukkan kuartil" in str(call.args[0])
                    for call in st.caption.call_args_list
                ))


if __name__ == "__main__":
    unittest.main()
