# AIR QUALITY ML
# DAG Airflow (E4): model prediksi PM2.5 enam jam ke depan per kota.
#
# ALUR (empat task)
# =================
# 1. build_features        : rakit tabel fitur dari fact_air_quality + dim_time.
# 2. train_model           : latih ridge regression per kota dengan split
#                            berbasis waktu, simpan parameter ke ml_model.
# 3. score_predictions     : prediksi jam target (origin + 6 jam) per kota,
#                            simpan ke ml_prediction bersama versi model dan
#                            waktu pembuatannya.
# 4. evaluate_predictions  : isi nilai aktual setelah jam target lewat, lalu
#                            hitung MAE tiga pendekatan ke ml_metrics.
#
# KEPUTUSAN PENTING
# =================
# - Nilai CAMS pada jam target TIDAK pernah menjadi fitur: itu informasi masa
#   depan sekaligus lengan pembanding. Fitur hanya memakai data yang benar-benar
#   tersedia saat prediksi dibuat (lag, rata-rata bergerak, atribut waktu).
# - Split data berbasis waktu (walk-forward), bukan acak. Pada data time series,
#   split acak menaruh jam-jam "masa depan" yang mirip ke dalam test set,
#   sehingga angka evaluasi terlihat bagus tetapi tidak mengukur kemampuan
#   memprediksi ke depan.
# - Model memakai scikit-learn (Pipeline StandardScaler + Ridge, lihat
#   ml_lib.py) yang ditambahkan ke Dockerfile.airflow; model dilatih ulang
#   tiap run dengan bobot recency supaya menyesuaikan diri dengan data baru.
# - Parameter model disimpan di tabel warehouse.ml_model, bukan file container
#   (hilang saat rebuild) dan bukan volume (tabel database sudah menjadi
#   penyimpanan bersama yang bisa dibaca semua task dan di-query siapa pun).

from __future__ import annotations

import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import psycopg2
from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowSkipException
from psycopg2.extras import Json, RealDictCursor

from ml_lib import FEATURE_COLUMNS, fit_ridge, model_input, predict_ridge

# ---------------------------------------------------------------------------
# KONFIGURASI
# ---------------------------------------------------------------------------

HORIZON_HOURS = 6             # target prediksi: enam jam setelah jam asal (origin).
HOLDOUT_HOURS = 48            # 48 jam terakhir dipakai sebagai holdout bersih.
EVAL_DELAY_HOURS = 3          # jeda agar batch memperbarui nilai terbaik CAMS.
MIN_TRAIN_ROWS = 24           # minimal pasangan (origin, target) per kota.
RIDGE_LAMBDA = 1.0            # penalti ridge (alpha) pada fitur terstandardisasi.
RECENCY_HALF_LIFE_HOURS = 168 # bobot data turun separuh tiap 168 jam (7 hari).
MODEL_TAG = "sklearn-ridge-v1"  # versi model: sklearn-ridge-v1-YYYYMMDDHHMM
WITA = ZoneInfo("Asia/Makassar")

# Syarat kelengkapan fitur numerik sebelum baris boleh masuk training/scoring.
PURE_FEATURES = " AND ".join(f"{name} IS NOT NULL" for name in FEATURE_COLUMNS)

# ---------------------------------------------------------------------------
# DDL: dijalankan ulang setiap task (aman diulang), jadi tidak butuh init file.
# ---------------------------------------------------------------------------

DDL = """
CREATE TABLE IF NOT EXISTS warehouse.ml_features (
    city TEXT NOT NULL,
    origin_at TIMESTAMP NOT NULL,
    lag_0 DOUBLE PRECISION,
    lag_1 DOUBLE PRECISION,
    lag_2 DOUBLE PRECISION,
    lag_3 DOUBLE PRECISION,
    lag_6 DOUBLE PRECISION,
    roll_mean_6 DOUBLE PRECISION,
    roll_mean_24 DOUBLE PRECISION,
    pm10_0 DOUBLE PRECISION,
    target_at TIMESTAMP NOT NULL,
    target_hour SMALLINT NOT NULL,
    target_dow TEXT NOT NULL,
    label_pm2_5 DOUBLE PRECISION,
    built_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (city, origin_at)
);

CREATE TABLE IF NOT EXISTS warehouse.ml_model (
    model_version TEXT NOT NULL,
    city TEXT NOT NULL,
    horizon_hours INT NOT NULL,
    feature_names JSONB NOT NULL,
    coefficients JSONB NOT NULL,
    intercept DOUBLE PRECISION NOT NULL,
    feature_means JSONB NOT NULL,
    feature_stds JSONB NOT NULL,
    ridge_lambda DOUBLE PRECISION NOT NULL,
    train_rows INT NOT NULL,
    train_start TIMESTAMP,
    train_end TIMESTAMP,
    holdout_rows INT NOT NULL DEFAULT 0,
    holdout_mae_model DOUBLE PRECISION,
    holdout_mae_persistence DOUBLE PRECISION,
    trained_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (model_version, city)
);

CREATE TABLE IF NOT EXISTS warehouse.ml_prediction (
    prediction_id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    model_version TEXT NOT NULL,
    city TEXT NOT NULL,
    origin_at TIMESTAMP NOT NULL,
    target_at TIMESTAMP NOT NULL,
    predicted_pm2_5 DOUBLE PRECISION,
    persistence_pm2_5 DOUBLE PRECISION,
    cams_pm2_5 DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    actual_pm2_5 DOUBLE PRECISION,
    evaluated_at TIMESTAMPTZ,
    UNIQUE (run_id, city, target_at)
);

CREATE TABLE IF NOT EXISTS warehouse.ml_metrics (
    run_id TEXT PRIMARY KEY,
    model_version TEXT,
    horizon_hours INT NOT NULL,
    n_predictions INT NOT NULL,
    n_evaluated INT NOT NULL,
    mae_model DOUBLE PRECISION,
    mae_persistence DOUBLE PRECISION,
    mae_cams DOUBLE PRECISION,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ml_prediction_target
    ON warehouse.ml_prediction(target_at);
"""

# ---------------------------------------------------------------------------
# SQL FITUR
# ---------------------------------------------------------------------------
# Semua lag diambil lewat join timestamp persis (bukan LAG berbasis baris),
# sehingga tetap benar kalau ada jam yang bolong. Rata-rata bergerak memakai
# window RANGE berbasis waktu untuk alasan yang sama.
# Fitur waktu (target_hour, target_dow) diambil dari dim_time pada jam target.
# Kolom label_pm2_5 memang nilai masa depan; kolom ini hanya dipakai training,
# tidak pernah dipakai scoring.

FEATURES_SQL = """
INSERT INTO warehouse.ml_features (
    city, origin_at,
    lag_0, lag_1, lag_2, lag_3, lag_6,
    roll_mean_6, roll_mean_24, pm10_0,
    target_at, target_hour, target_dow, label_pm2_5
)
SELECT
    l.city,
    t.valid_at,
    o.pm2_5,
    f1.pm2_5,
    f2.pm2_5,
    f3.pm2_5,
    f6l.pm2_5,
    AVG(o.pm2_5) OVER w6,
    AVG(o.pm2_5) OVER w24,
    o.pm10,
    t6.valid_at,
    t6.hour,
    t6.day_of_week,
    f6.pm2_5
FROM warehouse.fact_air_quality o
JOIN warehouse.dim_location l ON l.location_key = o.location_key
JOIN warehouse.dim_time t ON t.time_key = o.time_key
JOIN warehouse.dim_time t6 ON t6.valid_at = t.valid_at + INTERVAL '6 hours'
LEFT JOIN warehouse.fact_air_quality f1
    ON f1.location_key = o.location_key
   AND f1.time_key = TO_CHAR(t.valid_at - INTERVAL '1 hour', 'YYYYMMDDHH24')::BIGINT
LEFT JOIN warehouse.fact_air_quality f2
    ON f2.location_key = o.location_key
   AND f2.time_key = TO_CHAR(t.valid_at - INTERVAL '2 hours', 'YYYYMMDDHH24')::BIGINT
LEFT JOIN warehouse.fact_air_quality f3
    ON f3.location_key = o.location_key
   AND f3.time_key = TO_CHAR(t.valid_at - INTERVAL '3 hours', 'YYYYMMDDHH24')::BIGINT
LEFT JOIN warehouse.fact_air_quality f6l
    ON f6l.location_key = o.location_key
   AND f6l.time_key = TO_CHAR(t.valid_at - INTERVAL '6 hours', 'YYYYMMDDHH24')::BIGINT
LEFT JOIN warehouse.fact_air_quality f6
    ON f6.location_key = o.location_key
   AND f6.time_key = TO_CHAR(t.valid_at + INTERVAL '6 hours', 'YYYYMMDDHH24')::BIGINT
WHERE o.pm2_5 IS NOT NULL
WINDOW
    w6 AS (
        PARTITION BY l.city ORDER BY t.valid_at
        RANGE BETWEEN INTERVAL '5 hours' PRECEDING AND CURRENT ROW
    ),
    w24 AS (
        PARTITION BY l.city ORDER BY t.valid_at
        RANGE BETWEEN INTERVAL '23 hours' PRECEDING AND CURRENT ROW
    )
"""

# Klep versi fitur (urutan harus sama dengan ml_lib.FEATURE_COLUMNS).
ENCODED_TIME_FEATURES = ["hour_sin", "hour_cos", "dow_sin", "dow_cos"]


# ---------------------------------------------------------------------------
# HELPER
# ---------------------------------------------------------------------------

def dw_connection():
    return psycopg2.connect(
        host=os.getenv("DW_HOST", "postgres"),
        port=os.getenv("DW_PORT", "5432"),
        dbname=os.getenv("DW_DB", "air_quality_dw"),
        user=os.getenv("DW_USER", "platform"),
        password=os.getenv("DW_PASSWORD", "platform123"),
    )


def hour_floor_wita() -> datetime:
    """Jam penuh saat ini menurut WITA, tanpa informasi zona waktu."""
    return datetime.now(WITA).replace(
        tzinfo=None, minute=0, second=0, microsecond=0
    )


def pipeline_enabled(pipeline: str) -> bool:
    """True kalau jalur ini boleh jalan, False kalau dimatikan dari dashboard.

    Flag disimpan di warehouse.pipeline_settings dan ditulis dashboard
    (tab Pengaturan). Kalau tabelnya belum ada — dashboard belum pernah
    jalan — jalur dianggap ON, supaya pipeline tidak macet karena tabel.
    """
    try:
        with dw_connection() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT enabled FROM warehouse.pipeline_settings WHERE pipeline = %s",
                (pipeline,),
            )
            row = cur.fetchone()
    except psycopg2.Error:
        # Tabel belum ada -> biarkan jalur jalan.
        return True

    # Baris tidak ada -> pipeline itu belum pernah terdaftar -> default ON.
    return True if row is None else bool(row[0])


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

@dag(
    dag_id="air_quality_ml",
    description="Prediksi PM2.5 +6 jam per kota: fitur, training, scoring, evaluasi",
    schedule="10 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    # Satu run aktif saja: run yang bertumpuk (mis. catch-up setelah scheduler
    # lama mati) pernah membuat dua write bersamaan ke tabel ML dan deadlock.
    max_active_runs=1,
    tags=["big-data", "air-quality", "ml", "spark"],
)
def air_quality_ml():

    # -----------------------------------------------------------------------
    # TASK 1 - FEATURE
    # -----------------------------------------------------------------------
    @task
    def build_features() -> dict:
        """Rakit ulang warehouse.ml_features dari fact dan dimensi."""
        # Jalur ML bisa dimatikan dari dashboard (tab Pengaturan). Kalau
        # dimatikan, task ini di-skip dan seluruh DAG ter-lewati, jadi
        # ml_features tidak di-truncate dan tidak ada prediksi baru.
        if not pipeline_enabled("ml"):
            raise AirflowSkipException(
                "Jalur ML dimatikan lewat warehouse.pipeline_settings"
            )

        with dw_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(DDL)
            cur.execute("TRUNCATE warehouse.ml_features")
            cur.execute(FEATURES_SQL)
            cur.execute("SELECT COUNT(*) AS total FROM warehouse.ml_features")
            rows = cur.fetchone()["total"]
        print(f"[ML] ml_features dibangun ulang: {rows} baris origin")
        return {"feature_rows": rows}

    # -----------------------------------------------------------------------
    # TASK 2 - TRAINING
    # -----------------------------------------------------------------------
    @task
    def train_model(features_info: dict) -> dict:
        """Latih satu model ridge per kota, split berbasis waktu.

        Train   : origin <= sekarang - 48 jam, label sudah terbentuk.
        Holdout : origin 48 jam terakhir, dipakai hanya untuk laporan bersih
                  (model tidak dilatih pada rentang ini).
        """
        now_floor = hour_floor_wita()
        cutoff = now_floor - timedelta(hours=HOLDOUT_HOURS)
        model_version = f"{MODEL_TAG}-{now_floor:%Y%m%d%H%M}"

        trained = []
        skipped = []

        with dw_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(DDL)
            cur.execute(
                f"""
                SELECT city, origin_at, target_at, target_hour, target_dow,
                       {", ".join(FEATURE_COLUMNS)}, label_pm2_5
                FROM warehouse.ml_features
                WHERE {PURE_FEATURES}
                  AND label_pm2_5 IS NOT NULL
                  AND target_at <= %s
                  AND origin_at <= %s
                """,
                (now_floor, cutoff),
            )
            per_city: dict[str, list] = {}
            for row in cur.fetchall():
                per_city.setdefault(row["city"], []).append(row)

            for city, rows in sorted(per_city.items()):
                if len(rows) < MIN_TRAIN_ROWS:
                    skipped.append(city)
                    print(f"[ML] {city}: data training kurang ({len(rows)} < {MIN_TRAIN_ROWS}), dilewati")
                    continue

                x_train = [
                    model_input(row, row["target_hour"], row["target_dow"])
                    for row in rows
                ]
                y_train = [float(row["label_pm2_5"]) for row in rows]

                # Bobot recency: baris yang makin tua bobotnya turun separuh
                # tiap RECENCY_HALF_LIFE_HOURS jam, supaya model gesit mengikuti
                # rezim terbaru tanpa membuang riwayat lama.
                weights = [
                    0.5 ** ((now_floor - row["origin_at"]).total_seconds()
                            / 3600.0 / RECENCY_HALF_LIFE_HOURS)
                    for row in rows
                ]
                model = fit_ridge(x_train, y_train, RIDGE_LAMBDA, weights=weights)

                # Holdout bersih untuk laporan: MAE model vs persistence.
                cur.execute(
                    f"""
                    SELECT target_hour, target_dow, {", ".join(FEATURE_COLUMNS)}, label_pm2_5
                    FROM warehouse.ml_features
                    WHERE city = %s AND {PURE_FEATURES}
                      AND label_pm2_5 IS NOT NULL
                      AND target_at <= %s
                      AND origin_at > %s
                    """,
                    (city, now_floor, cutoff),
                )
                holdout = cur.fetchall()
                mae_model = mae_persistence = None
                if holdout:
                    errors_model = []
                    errors_persistence = []
                    for hrow in holdout:
                        label = float(hrow["label_pm2_5"])
                        predicted = predict_ridge(
                            model,
                            model_input(hrow, hrow["target_hour"], hrow["target_dow"]),
                        )
                        errors_model.append(abs(predicted - label))
                        errors_persistence.append(abs(float(hrow["lag_0"]) - label))
                    mae_model = sum(errors_model) / len(errors_model)
                    mae_persistence = sum(errors_persistence) / len(errors_persistence)

                cur.execute(
                    """
                    INSERT INTO warehouse.ml_model (
                        model_version, city, horizon_hours, feature_names,
                        coefficients, intercept, feature_means, feature_stds,
                        ridge_lambda, train_rows, train_start, train_end,
                        holdout_rows, holdout_mae_model, holdout_mae_persistence,
                        trained_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (model_version, city) DO UPDATE SET
                        feature_names = EXCLUDED.feature_names,
                        coefficients = EXCLUDED.coefficients,
                        intercept = EXCLUDED.intercept,
                        feature_means = EXCLUDED.feature_means,
                        feature_stds = EXCLUDED.feature_stds,
                        train_rows = EXCLUDED.train_rows,
                        train_start = EXCLUDED.train_start,
                        train_end = EXCLUDED.train_end,
                        holdout_rows = EXCLUDED.holdout_rows,
                        holdout_mae_model = EXCLUDED.holdout_mae_model,
                        holdout_mae_persistence = EXCLUDED.holdout_mae_persistence,
                        trained_at = NOW()
                    """,
                    (
                        model_version,
                        city,
                        HORIZON_HOURS,
                        Json(FEATURE_COLUMNS + ENCODED_TIME_FEATURES),
                        Json(model["coefficients"]),
                        model["intercept"],
                        Json(model["means"]),
                        Json(model["stds"]),
                        RIDGE_LAMBDA,
                        len(rows),
                        min(r["origin_at"] for r in rows),
                        max(r["origin_at"] for r in rows),
                        len(holdout),
                        mae_model,
                        mae_persistence,
                    ),
                )
                trained.append(city)
                report = (
                    f"MAE holdout model={mae_model:.2f} persistence={mae_persistence:.2f}"
                    if holdout else "holdout kosong"
                )
                print(f"[ML] {city}: dilatih pada {len(rows)} baris ({report})")

        # DAG tidak boleh hijau kalau tidak ada satu pun model yang dilatih:
        # score_predictions akan memakai model lama/kosong tanpa peringatan.
        if not trained:
            raise ValueError(
                "ML gagal: tidak ada kota yang punya cukup data training "
                f"(minimal {MIN_TRAIN_ROWS} baris per kota; "
                f"dilewati: {skipped or '-'})"
            )

        print(f"[ML] Versi model {model_version}: dilatih {trained or '-'}, dilewati {skipped or '-'}")
        return {"model_version": model_version, "trained": trained, "skipped": skipped}

    # -----------------------------------------------------------------------
    # TASK 3 - SCORING
    # -----------------------------------------------------------------------
    @task
    def score_predictions(train_info: dict) -> dict:
        """Prediksi setiap kota untuk jam target (origin + 6 jam).

        Tiga nilai disimpan berdampingan pada saat yang sama:
        - predicted_pm2_5  : model ridge milik kita.
        - persistence_pm2_5: baseline paling naif (nilai terakhir = prediksi).
        - cams_pm2_5       : nilai CAMS/Open-Meteo untuk jam target pada saat
                             scoring berjalan (forecast pihak ketiga, dibekukan
                             sebelum jam target tiba; bukan fitur model).
        """
        now_floor = hour_floor_wita()
        run_id = f"ml-{now_floor:%Y%m%d%H%M}"
        scored = 0

        with dw_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(DDL)
            cur.execute(
                """
                SELECT DISTINCT ON (city)
                    city, model_version, intercept, coefficients,
                    feature_means, feature_stds
                FROM warehouse.ml_model
                ORDER BY city, trained_at DESC, model_version DESC
                """
            )
            models = {}
            for row in cur.fetchall():
                models[row["city"]] = {
                    "model_version": row["model_version"],
                    "intercept": float(row["intercept"]),
                    "coefficients": [float(v) for v in row["coefficients"]],
                    "means": [float(v) for v in row["feature_means"]],
                    "stds": [float(v) for v in row["feature_stds"]],
                }
            if not models:
                print("[ML] Belum ada model; scoring dilewati")
                return {"run_id": run_id, "scored": 0}

            cur.execute(
                f"""
                SELECT DISTINCT ON (city)
                    city, origin_at, target_at, target_hour, target_dow,
                    {", ".join(FEATURE_COLUMNS)}
                FROM warehouse.ml_features
                WHERE origin_at <= %s AND {PURE_FEATURES}
                ORDER BY city, origin_at DESC
                """,
                (now_floor,),
            )
            for row in cur.fetchall():
                model = models.get(row["city"])
                if model is None:
                    continue

                predicted = predict_ridge(
                    model,
                    model_input(row, row["target_hour"], row["target_dow"]),
                )
                persistence = float(row["lag_0"])

                # Snapshot CAMS untuk jam target pada saat scoring berjalan.
                cur.execute(
                    """
                    SELECT f.pm2_5
                    FROM warehouse.fact_air_quality f
                    JOIN warehouse.dim_location l ON l.location_key = f.location_key
                    JOIN warehouse.dim_time t ON t.time_key = f.time_key
                    WHERE l.city = %s AND t.valid_at = %s
                    """,
                    (row["city"], row["target_at"]),
                )
                cams_row = cur.fetchone()
                cams = (
                    float(cams_row["pm2_5"])
                    if cams_row and cams_row["pm2_5"] is not None
                    else None
                )

                cur.execute(
                    """
                    INSERT INTO warehouse.ml_prediction (
                        run_id, model_version, city, origin_at, target_at,
                        predicted_pm2_5, persistence_pm2_5, cams_pm2_5, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (run_id, city, target_at) DO UPDATE SET
                        model_version = EXCLUDED.model_version,
                        predicted_pm2_5 = EXCLUDED.predicted_pm2_5,
                        persistence_pm2_5 = EXCLUDED.persistence_pm2_5,
                        cams_pm2_5 = EXCLUDED.cams_pm2_5
                    """,
                    (
                        run_id,
                        model["model_version"],
                        row["city"],
                        row["origin_at"],
                        row["target_at"],
                        predicted,
                        persistence,
                        cams,
                    ),
                )
                scored += 1
                print(
                    f"[ML] {row['city']}: prediksi {predicted:.1f} vs "
                    f"persistence {persistence:.1f} vs CAMS {cams if cams is None else round(cams, 1)} "
                    f"untuk {row['target_at']:%Y-%m-%d %H:%M}"
                )

        return {"run_id": run_id, "scored": scored}

    # -----------------------------------------------------------------------
    # TASK 4 - EVALUASI
    # -----------------------------------------------------------------------
    @task
    def evaluate_predictions(score_info: dict) -> dict:
        """Isi nilai aktual prediksi yang sudah matang, lalu hitung MAE.

        Prediksi boleh dinilai hanya setelah jam target lewat plus jeda tiga jam
        (memberi kesempatan batch menulis ulang nilai terbaik CAMS). Evaluasi
        dihitung ulang dari nol setiap run supaya idempoten: hasil akhirnya sama
        siapa pun yang menonton kapan pun.
        """
        now_floor = hour_floor_wita()
        mature_before = now_floor - timedelta(hours=EVAL_DELAY_HOURS)

        with dw_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(DDL)

            # 1. Tempelkan nilai aktual (nilai terbaru di fact pada jam target).
            cur.execute(
                """
                UPDATE warehouse.ml_prediction p
                SET actual_pm2_5 = f.pm2_5,
                    evaluated_at = NOW()
                FROM warehouse.fact_air_quality f
                JOIN warehouse.dim_location l ON l.location_key = f.location_key
                JOIN warehouse.dim_time t ON t.time_key = f.time_key
                WHERE l.city = p.city
                  AND t.valid_at = p.target_at
                  AND p.target_at <= %s
                """,
                (mature_before,),
            )
            updated = cur.rowcount

            # 2. Hitung ulang MAE tiap run dari seluruh prediksi yang sudah
            #    matang. Filter lengkap memastikan semua lengan dinilai pada
            #    himpunan baris yang sama (perbandingan adil).
            cur.execute(
                """
                INSERT INTO warehouse.ml_metrics (
                    run_id, model_version, horizon_hours,
                    n_predictions, n_evaluated,
                    mae_model, mae_persistence, mae_cams, updated_at
                )
                SELECT
                    p.run_id,
                    MIN(p.model_version),
                    %s,
                    (SELECT COUNT(*) FROM warehouse.ml_prediction q WHERE q.run_id = p.run_id),
                    COUNT(*),
                    AVG(ABS(p.predicted_pm2_5 - p.actual_pm2_5)),
                    AVG(ABS(p.persistence_pm2_5 - p.actual_pm2_5)),
                    AVG(ABS(p.cams_pm2_5 - p.actual_pm2_5)),
                    NOW()
                FROM warehouse.ml_prediction p
                WHERE p.actual_pm2_5 IS NOT NULL
                  AND p.predicted_pm2_5 IS NOT NULL
                  AND p.persistence_pm2_5 IS NOT NULL
                  AND p.cams_pm2_5 IS NOT NULL
                GROUP BY p.run_id
                ON CONFLICT (run_id) DO UPDATE SET
                    model_version = EXCLUDED.model_version,
                    horizon_hours = EXCLUDED.horizon_hours,
                    n_predictions = EXCLUDED.n_predictions,
                    n_evaluated = EXCLUDED.n_evaluated,
                    mae_model = EXCLUDED.mae_model,
                    mae_persistence = EXCLUDED.mae_persistence,
                    mae_cams = EXCLUDED.mae_cams,
                    updated_at = NOW()
                """,
                (HORIZON_HOURS,),
            )

            run_id = score_info.get("run_id")
            cur.execute(
                """
                SELECT n_predictions, n_evaluated, mae_model, mae_persistence, mae_cams
                FROM warehouse.ml_metrics WHERE run_id = %s
                """,
                (run_id,),
            )
            own = cur.fetchone()

        print(f"[ML] Aktual diperbarui untuk {updated} prediksi")
        if own:
            print(
                f"[ML] Metrik kumulatif {run_id}: n={own['n_evaluated']}, "
                f"MAE model={own['mae_model']:.2f}, "
                f"persistence={own['mae_persistence']:.2f}, "
                f"CAMS={own['mae_cams']:.2f}"
            )
        else:
            print(f"[ML] {run_id}: belum ada prediksi matang (tunggu {EVAL_DELAY_HOURS} jam)")
        return {"run_id": run_id, "updated": updated}

    # -----------------------------------------------------------------------
    # URUTAN TASK
    # -----------------------------------------------------------------------
    features_info = build_features()
    train_info = train_model(features_info)
    score_info = score_predictions(train_info)
    evaluate_predictions(score_info)


air_quality_ml()
