"""Fungsi ML murni Python untuk DAG air_quality_ml (E4).

Image Airflow tidak memuat library ML (scikit-learn, numpy), jadi model dibuat
dari standard library saja: ridge regression lewat normal equations yang
diselesaikan dengan eliminasi Gauss. File ini sengaja bebas dari dependency
Airflow dan database supaya bisa diuji langsung:

    python ml_lib.py

Modul ini dipakai bersama oleh task training dan task scoring supaya
perhitungan fitur di kedua sisi selalu identik (mencegah training/serving skew).
"""
from __future__ import annotations

import math

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


def solve_linear(a, b):
    """Selesaikan a x = b dengan eliminasi Gauss + partial pivoting."""
    n = len(a)
    m = [list(row) + [b[i]] for i, row in enumerate(a)]
    for col in range(n):
        pivot = max(range(col, n), key=lambda r: abs(m[r][col]))
        m[col], m[pivot] = m[pivot], m[col]
        if abs(m[col][col]) < 1e-12:
            raise ValueError("sistem linear singular")
        for row in range(col + 1, n):
            factor = m[row][col] / m[col][col]
            if factor != 0.0:
                for c in range(col, n + 1):
                    m[row][c] -= factor * m[col][c]
    x = [0.0] * n
    for row in range(n - 1, -1, -1):
        total = m[row][n] - sum(m[row][c] * x[c] for c in range(row + 1, n))
        x[row] = total / m[row][row]
    return x


def fit_ridge(rows, labels, lam=1.0):
    """Latih ridge regression y = intercept + sum(b_i * (x_i - mean_i) / std_i).

    Fitur distandardisasi dulu supaya penalti ridge adil untuk semua kolom;
    intercept tidak dikenai penalti. Kembaliannya dict siap disimpan ke JSONB.
    """
    if not rows or len(rows) != len(labels):
        raise ValueError("jumlah baris dan label tidak cocok")
    n = len(rows)
    width = len(rows[0])
    if n < 2 or width == 0:
        raise ValueError("butuh minimal 2 baris dan 1 fitur")
    if any(len(row) != width for row in rows):
        raise ValueError("panjang baris tidak seragam")

    means = [sum(col) / n for col in zip(*rows)]
    stds = []
    for col, mean in zip(zip(*rows), means):
        variance = sum((value - mean) ** 2 for value in col) / n
        stds.append(math.sqrt(variance) or 1.0)

    standardized = [
        [(value - mean) / std for value, mean, std in zip(row, means, stds)]
        for row in rows
    ]

    # Normal equations untuk desain [1, z1..zp], penalti hanya untuk slope.
    size = width + 1
    ata = [[0.0] * size for _ in range(size)]
    aty = [0.0] * size
    for row, label in zip(standardized, labels):
        design = [1.0] + row
        for i in range(size):
            aty[i] += design[i] * label
            for j in range(size):
                ata[i][j] += design[i] * design[j]
    for i in range(1, size):
        ata[i][i] += lam

    beta = solve_linear(ata, aty)
    return {
        "intercept": beta[0],
        "coefficients": beta[1:],
        "means": means,
        "stds": stds,
    }


def predict_ridge(model, row):
    """Prediksi satu baris memakai parameter dari fit_ridge."""
    standardized = [
        (value - mean) / std
        for value, mean, std in zip(row, model["means"], model["stds"])
    ]
    return model["intercept"] + sum(
        coef * value for coef, value in zip(model["coefficients"], standardized)
    )


def _self_check():
    """Pemeriksaan cepat tanpa framework: python ml_lib.py"""
    import random

    # 1. Data persis linear: ridge dengan lambda kecil harus memulihkannya.
    random.seed(7)
    rows = [[random.uniform(-10, 10), random.uniform(-10, 10)] for _ in range(200)]
    labels = [3 * row[0] - 2 * row[1] + 5 for row in rows]
    model = fit_ridge(rows, labels, lam=1e-9)
    worst = max(
        abs(predict_ridge(model, row) - label)
        for row, label in zip(rows, labels)
    )
    assert worst < 1e-6, f"galat terlalu besar: {worst}"

    # 2. Lambda besar menekan semua koefisien: prediksi menyusut ke rata-rata.
    shrunk = fit_ridge(rows, labels, lam=1e9)
    mean_label = sum(labels) / len(labels)
    assert abs(predict_ridge(shrunk, rows[0]) - mean_label) < 1e-3

    # 3. Fitur waktu siklik: jam 0 dan 24 adalah sudut yang sama.
    assert time_features(0, "Monday") == time_features(24, "Monday")

    # 4. Baris input model punya panjang fitur + 4 kolom waktu.
    sample = {name: 1.0 for name in FEATURE_COLUMNS}
    assert len(model_input(sample, 12, "Sunday")) == len(FEATURE_COLUMNS) + 4

    print("[ML-LIB] Self-check lolos")


if __name__ == "__main__":
    _self_check()
