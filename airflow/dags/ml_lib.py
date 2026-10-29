"""Fungsi ML untuk DAG air_quality_ml (E4).

Model memakai scikit-learn: Ridge (regresi linear berpenalti) di atas fitur
yang distandardisasi StandardScaler. Model dilatih ulang setiap run dengan
bobot recency supaya cepat mengikuti data terbaru. File ini sengaja bebas dari
dependency Airflow dan database supaya bisa diuji langsung:

    python ml_lib.py

Modul ini dipakai bersama oleh task training dan task scoring supaya
perhitungan fitur di kedua sisi selalu identik (mencegah training/serving skew).
"""
from __future__ import annotations

import math
from datetime import datetime, timedelta

import numpy as np
from sklearn.linear_model import Ridge
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

# Urutan fitur numerik di tabel warehouse.ml_features.
FEATURE_COLUMNS = [
    "lag_0", "lag_1", "lag_2", "lag_3", "lag_6",
    "roll_mean_6", "roll_mean_24", "pm10_0",
]

# Nama hari dari dim_time (TRIM(TO_CHAR(valid_at, 'Day')) berbahasa Inggris).
DOW_INDEX = {
    "monday": 1, "tuesday": 2, "wednesday": 3, "thursday": 4,
    "friday": 5, "saturday": 6, "sunday": 7,
}

BACKTEST_ANCHORS_SQL = """
    SELECT DISTINCT ON (city) city, origin_at
    FROM warehouse.ml_prediction
    WHERE target_at > (clock_timestamp() AT TIME ZONE 'Asia/Makassar')
      AND horizon_hours = 6
    ORDER BY city, created_at DESC, prediction_id DESC
"""


def cyclic(value, period):
    """Ubah nilai waktu menjadi pasangan (sin, cos) agar jam 23 dan 0 berdekatan."""
    angle = 2 * math.pi * (value % period) / period
    return math.sin(angle), math.cos(angle)


def time_features(hour, day_of_week):
    """Fitur waktu untuk jam target: siklus jam dan siklus hari."""
    day = DOW_INDEX.get(str(day_of_week).strip().lower())
    if day is None:
        raise ValueError(f"hari tidak dikenal: {day_of_week!r}")
    hour_sin, hour_cos = cyclic(int(hour), 24)
    day_sin, day_cos = cyclic(day, 7)
    return [hour_sin, hour_cos, day_sin, day_cos]


def model_input(features, hour, day_of_week):
    """Rakit satu baris input model dari nilai fitur + waktu target."""
    values = [float(features[name]) for name in FEATURE_COLUMNS]
    return values + time_features(hour, day_of_week)


def fit_ridge(rows, labels, lam=1.0, weights=None):
    """Latih ridge scikit-learn: y = intercept + sum(b_i * (x_i - mean_i) / std_i).

    Fitur distandardisasi oleh StandardScaler di dalam Pipeline supaya penalti
    ridge (alpha=lam) adil untuk semua kolom; intercept tidak dikenai penalti.
    `weights` = bobot per baris (mis. recency). Kembaliannya dict siap disimpan
    ke JSONB dan bisa diprediksi ulang lewat predict_ridge.
    """
    if not rows or len(rows) != len(labels):
        raise ValueError("jumlah baris dan label tidak cocok")
    n = len(rows)
    width = len(rows[0])
    if n < 2 or width == 0:
        raise ValueError("butuh minimal 2 baris dan 1 fitur")
    if any(len(row) != width for row in rows):
        raise ValueError("panjang baris tidak seragam")
    if weights is not None and len(weights) != n:
        raise ValueError("jumlah bobot tidak cocok")

    pipeline = Pipeline(
        [
            ("scaler", StandardScaler()),
            ("ridge", Ridge(alpha=lam)),
        ]
    )
    if weights is None:
        pipeline.fit(rows, labels)
    else:
        pipeline.fit(rows, labels, ridge__sample_weight=weights)

    scaler = pipeline.named_steps["scaler"]
    ridge = pipeline.named_steps["ridge"]
    return {
        "intercept": float(ridge.intercept_),
        "coefficients": [float(value) for value in ridge.coef_],
        "means": [float(value) for value in scaler.mean_],
        "stds": [float(value) for value in scaler.scale_],
    }


def predict_ridge(model, row):
    """Prediksi satu baris memakai parameter dari fit_ridge.

    Objek scikit-learn direkonstruksi dari atribut tersimpan (tanpa file
    model), jadi training dan scoring memakai mesin yang sama.
    """
    scaler = StandardScaler()
    scaler.mean_ = np.array(model["means"], dtype=float)
    scaler.scale_ = np.array(model["stds"], dtype=float)
    scaler.var_ = scaler.scale_**2
    scaler.n_features_in_ = len(scaler.mean_)

    ridge = Ridge(alpha=0.0)
    ridge.coef_ = np.array(model["coefficients"], dtype=float)
    ridge.intercept_ = float(model["intercept"])
    ridge.n_features_in_ = len(ridge.coef_)

    return float(ridge.predict(scaler.transform([np.asarray(row, dtype=float)]))[0])


def backtest_predictions(
    rows: list[dict], anchors: dict[str, datetime], horizon: int, holdout: int,
    min_rows: int, lam: float, half_life: int,
) -> list[tuple]:
    """Simulasikan enam target per kota dari fitur warehouse saat ini."""
    by_city: dict[str, list[dict]] = {}
    for row in rows:
        if all(row[name] is not None for name in FEATURE_COLUMNS):
            by_city.setdefault(row["city"], []).append(row)

    predictions = []
    for city, live_origin in anchors.items():
        city_rows = by_city.get(city, [])
        by_origin = {row["origin_at"]: row for row in city_rows}
        for offset in range(5, -1, -1):
            target = live_origin - timedelta(hours=offset)
            origin = target - timedelta(hours=horizon)
            candidate = by_origin.get(origin)
            if candidate is None or candidate["target_at"] != target:
                continue
            cutoff = origin - timedelta(hours=holdout)
            training = [
                row for row in city_rows
                if row["label_pm2_5"] is not None
                and row["target_at"] <= origin and row["origin_at"] <= cutoff
            ]
            if len(training) < min_rows:
                continue
            inputs = [model_input(row, row["target_hour"], row["target_dow"]) for row in training]
            labels = [float(row["label_pm2_5"]) for row in training]
            weights = [
                0.5 ** ((origin - row["origin_at"]).total_seconds() / 3600 / half_life)
                for row in training
            ]
            model = fit_ridge(inputs, labels, lam, weights=weights)
            predicted = predict_ridge(
                model, model_input(candidate, candidate["target_hour"], candidate["target_dow"])
            )
            predictions.append((city, target, origin, predicted))
    return predictions


def _self_check():
    """Pemeriksaan cepat tanpa framework: python ml_lib.py"""
    import random

    # 1. Data persis linear: ridge dengan alpha kecil harus memulihkannya.
    random.seed(7)
    rows = [[random.uniform(-10, 10), random.uniform(-10, 10)] for _ in range(200)]
    labels = [3 * row[0] - 2 * row[1] + 5 for row in rows]
    model = fit_ridge(rows, labels, lam=1e-9)
    worst = max(
        abs(predict_ridge(model, row) - label)
        for row, label in zip(rows, labels)
    )
    assert worst < 1e-6, f"galat terlalu besar: {worst}"

    # 2. Alpha besar menekan semua koefisien: prediksi menyusut ke rata-rata.
    shrunk = fit_ridge(rows, labels, lam=1e9)
    mean_label = sum(labels) / len(labels)
    assert abs(predict_ridge(shrunk, rows[0]) - mean_label) < 1e-3

    # 3. Bobot recency: pola data terbaru mendominasi prediksi.
    weighted = fit_ridge(
        [[0.0], [0.0], [1.0], [1.0]],
        [10.0, 10.0, 20.0, 20.0],
        lam=0.0,
        weights=[1e-9, 1e-9, 1.0, 1.0],
    )
    assert abs(predict_ridge(weighted, [1.0]) - 20.0) < 1e-6

    # 4. Fitur waktu siklik: jam 0 dan 24 adalah sudut yang sama.
    assert time_features(0, "Monday") == time_features(24, "Monday")

    # 5. Baris input model punya panjang fitur + 4 kolom waktu.
    sample = {name: 1.0 for name in FEATURE_COLUMNS}
    assert len(model_input(sample, 12, "Sunday")) == len(FEATURE_COLUMNS) + 4

    # Cutoff dan minimal training diperiksa pada simulasi model sesungguhnya.
    anchor = datetime(2026, 1, 10)
    origin = anchor - timedelta(hours=6)
    sample_row = {**sample, "city": "x", "target_hour": 0, "target_dow": "Monday",
                  "label_pm2_5": 10.0}
    training = [
        {**sample_row, "origin_at": origin - timedelta(hours=48 + i),
         "target_at": origin - timedelta(hours=42 + i)}
        for i in range(24)
    ]
    candidate = {**sample_row, "origin_at": origin, "target_at": anchor}
    assert len(backtest_predictions(training + [candidate], {"x": anchor}, 6, 48, 24, 1, 168)) == 1
    assert not backtest_predictions(training[1:] + [candidate], {"x": anchor}, 6, 48, 24, 1, 168)
    assert not backtest_predictions(training + [{**candidate, "lag_0": None}],
                                    {"x": anchor}, 6, 48, 24, 1, 168)

    import ast
    from pathlib import Path

    dag = ast.parse(Path(__file__).with_name("air_quality_ml.py").read_text(encoding="utf-8"))
    feature_sql = next(node.value.value for node in dag.body if isinstance(node, ast.Assign)
                       and any(isinstance(t, ast.Name) and t.id == "FEATURES_SQL"
                               for t in node.targets))
    assert "GENERATE_SERIES(1, 6)" in feature_sql
    assert "o.origin_at + h.horizon_hours * INTERVAL '1 hour'" in feature_sql
    assert "RANGE BETWEEN INTERVAL '23 hours' PRECEDING AND CURRENT ROW" in feature_sql
    assert "horizon_hours = 6" in BACKTEST_ANCHORS_SQL

    print("[ML-LIB] Self-check lolos")


if __name__ == "__main__":
    _self_check()
