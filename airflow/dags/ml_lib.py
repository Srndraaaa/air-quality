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

    import ast
    from pathlib import Path

    dag = ast.parse(Path(__file__).with_name("air_quality_ml.py").read_text(encoding="utf-8"))
    feature_sql = next(node.value.value for node in dag.body if isinstance(node, ast.Assign)
                       and any(isinstance(t, ast.Name) and t.id == "FEATURES_SQL"
                               for t in node.targets))
    assert "GENERATE_SERIES(1, 6)" in feature_sql
    assert "o.origin_at + h.horizon_hours * INTERVAL '1 hour'" in feature_sql
    assert "RANGE BETWEEN INTERVAL '23 hours' PRECEDING AND CURRENT ROW" in feature_sql

    print("[ML-LIB] Self-check lolos")


if __name__ == "__main__":
    _self_check()
