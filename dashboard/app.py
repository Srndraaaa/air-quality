"""Dashboard kelas: seluruh angka berasal dari fact dan dimension warehouse.

Empat tab:
- Live (streaming): data sensor simulasi, diperbarui otomatis tiap 5 detik.
- Per jam (batch): data Open-Meteo yang dimuat DAG Airflow.
- Prediksi (ML): hasil model ridge (E4) vs baseline persistence dan CAMS.
- Pengaturan: tombol on/off untuk tiap jalur pipeline.
"""

import os
from contextlib import closing

import pandas as pd
import psycopg2
import streamlit as st


def connect():
    """Koneksi ke PostgreSQL warehouse."""
    return psycopg2.connect(
        host=os.getenv("DW_HOST", "postgres"),
        port=os.getenv("DW_PORT", "5432"),
        dbname=os.getenv("DW_DB", "air_quality_dw"),
        user=os.getenv("DW_USER", "platform"),
        password=os.getenv("DW_PASSWORD", "platform123"),
        connect_timeout=5,
    )


def query(sql: str) -> pd.DataFrame:
    with closing(connect()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            columns = [column[0] for column in cur.description]
    return pd.DataFrame(rows, columns=columns)


def execute(sql: str, params: tuple = ()) -> int:
    """Jalankan perintah tulis lalu commit.

    Dipakai dashboard untuk menulis flag on/off. Semua perintah yang
    dipakai di sini idempotent, jadi aman dijalankan ulang.
    """
    with closing(connect()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            conn.commit()
            return cur.rowcount


def read_fact() -> pd.DataFrame:
    return query("""
        SELECT l.city, t.valid_at, f.pm2_5, f.pm10, f.european_aqi
        FROM warehouse.fact_air_quality AS f
        JOIN warehouse.dim_location AS l ON l.location_key = f.location_key
        JOIN warehouse.dim_time AS t ON t.time_key = f.time_key
        ORDER BY t.valid_at DESC, l.city
    """)


# ---------------------------------------------------------------- jalur streaming

RULE_LABELS = {
    "normalized_city": "Nama kota dibakukan",
    "fixed_format": "Koma diganti titik",
    "nulled_unparseable": "Teks bukan angka → NULL",
    "nulled_negative": "Nilai negatif → NULL",
    "nulled_outlier": "Nilai tidak wajar → NULL",
    "rejected_city": "Ditolak: kota tak dikenal",
    "rejected_time": "Ditolak: waktu tidak wajar",
    "rejected_empty": "Ditolak: semua nilai kosong",
    "duplicates": "Dibuang: duplikat",
}


@st.fragment(run_every="5s")
def render_live() -> None:
    try:
        summary = query("""
            SELECT
                COALESCE(SUM(rows_in), 0) AS rows_in,
                COALESCE(SUM(rows_loaded), 0) AS rows_loaded,
                COALESCE(SUM(normalized_city), 0) AS normalized_city,
                COALESCE(SUM(fixed_format), 0) AS fixed_format,
                COALESCE(SUM(nulled_unparseable), 0) AS nulled_unparseable,
                COALESCE(SUM(nulled_negative), 0) AS nulled_negative,
                COALESCE(SUM(nulled_outlier), 0) AS nulled_outlier,
                COALESCE(SUM(rejected_city), 0) AS rejected_city,
                COALESCE(SUM(rejected_time), 0) AS rejected_time,
                COALESCE(SUM(rejected_empty), 0) AS rejected_empty,
                COALESCE(SUM(duplicates), 0) AS duplicates
            FROM staging.stream_quality_log
            WHERE processed_at >= NOW() - INTERVAL '5 minutes'
        """).iloc[0]
        lag = query("""
            SELECT EXTRACT(EPOCH FROM (NOW() AT TIME ZONE 'Asia/Makassar') - MAX(reading_at)) AS seconds
            FROM warehouse.fact_sensor_live
        """).iloc[0]["seconds"]
        live = query("""
            SELECT f.reading_at, l.city, f.pm2_5
            FROM warehouse.fact_sensor_live f
            JOIN warehouse.dim_location l USING (location_key)
            WHERE f.reading_at >= (NOW() AT TIME ZONE 'Asia/Makassar') - INTERVAL '10 minutes'
            ORDER BY f.reading_at
        """)
        raw = query("""
            SELECT received_at AT TIME ZONE 'Asia/Makassar' AS diterima, dirty_type AS jenis,
                   city AS kota, reading_at AS jam, pm2_5, pm10, ozone
            FROM staging.sensor_raw
            ORDER BY raw_id DESC
            LIMIT 15
        """)
    except (psycopg2.Error, OSError) as exc:
        st.warning("Jalur streaming belum siap. Pastikan service sensor-simulator dan spark-streaming menyala.")
        st.code(str(exc))
        return

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Pesan masuk (5 menit)", f"{int(summary['rows_in']):,}")
    k2.metric("Masuk ke warehouse", f"{int(summary['rows_loaded']):,}")
    k3.metric("Ditolak / duplikat", f"{int(summary['rows_in'] - summary['rows_loaded']):,}")
    k4.metric("Jeda data terakhir", f"{lag:.0f} detik" if pd.notna(lag) else "—")

    left, right = st.columns([3, 2])
    with left:
        st.subheader("PM2.5 live, 10 menit terakhir")
        if live.empty:
            st.info("Belum ada data live. Tunggu beberapa detik setelah simulator menyala.")
        else:
            live["reading_at"] = pd.to_datetime(live["reading_at"])
            trend = live.pivot_table(index="reading_at", columns="city", values="pm2_5", aggfunc="mean")
            st.line_chart(trend, x_label="Waktu (WITA)", y_label="PM2.5 (µg/m³)")
    with right:
        st.subheader("Yang dibersihkan Spark (5 menit)")
        rules = pd.DataFrame(
            {"Jumlah": [int(summary[key]) for key in RULE_LABELS]},
            index=list(RULE_LABELS.values()),
        )
        st.bar_chart(rules, horizontal=True, x_label="Jumlah", y_label="")

    st.subheader("15 pesan mentah terbaru dari sensor")
    st.caption("Baris merah = pesan yang sengaja dibuat kotor oleh simulator. Kolom 'jenis' hanya ada untuk keperluan pembelajaran; sensor sungguhan tidak memberikan label seperti ini.")
    styled = raw.style.apply(
        lambda row: ["background-color: rgba(220, 60, 50, 0.15)" if row["jenis"] != "bersih" else "" for _ in row],
        axis=1,
    )
    st.dataframe(styled, width="stretch", hide_index=True)


# ---------------------------------------------------------------- jalur batch

def render_batch() -> None:
    if st.button("Muat ulang data"):
        st.rerun()

    try:
        data = read_fact()
    except (psycopg2.Error, OSError) as exc:
        st.error("Database belum siap. Periksa status PostgreSQL dan jalankan pipeline Airflow.")
        st.code(str(exc))
        return

    if data.empty:
        st.info("Belum ada baris di fact table. Jalankan DAG air_quality_end_to_end di Airflow, tunggu empat task berhasil, lalu klik Muat ulang data.")
        return

    data["valid_at"] = pd.to_datetime(data["valid_at"])
    cities = sorted(data["city"].unique().tolist())
    selected = st.multiselect("Pilih kota", cities, default=cities)
    if not selected:
        st.info("Pilih sedikitnya satu kota untuk menampilkan grafik.")
        return

    view = data.loc[data["city"].isin(selected)].copy()
    recent = view.loc[view["valid_at"] >= view["valid_at"].max() - pd.Timedelta(days=7)]

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Kota terpilih", len(selected))
    k2.metric("Baris fakta", f"{len(view):,}")
    k3.metric("PM2.5 rerata (µg/m³)", f"{view['pm2_5'].mean():.1f}" if view["pm2_5"].notna().any() else "—")
    k4.metric("Jam data terakhir (WITA)", view["valid_at"].max().strftime("%d/%m/%Y %H:%M"))

    st.subheader("PM2.5 per jam dalam tujuh hari terakhir")
    trend = recent.pivot_table(index="valid_at", columns="city", values="pm2_5", aggfunc="mean")
    if trend.empty:
        st.info("Tidak ada nilai PM2.5 pada rentang ini.")
    else:
        st.line_chart(trend, x_label="Waktu (WITA)", y_label="PM2.5 (µg/m³)")

    st.subheader("Rerata polutan per kota dalam tujuh hari terakhir")
    means = recent.groupby("city", as_index=True)[["pm2_5", "pm10"]].mean()
    st.bar_chart(means, x_label="Kota", y_label="Konsentrasi (µg/m³)")

    st.subheader("50 catatan terbaru")
    st.dataframe(view.head(50), width="stretch", hide_index=True)
    st.caption("Ini data model kualitas udara dari Open-Meteo/CAMS, bukan pembacaan sensor lapangan. Nilai kosong tetap kosong; jangan ditafsirkan sebagai nol. Indeks AQI Eropa hanya ditampilkan jika tersedia.")


# ---------------------------------------------------------------- jalur ML (E4)

def render_ml() -> None:
    if st.button("Muat ulang data", key="ml_reload"):
        st.rerun()

    try:
        evaluated = query("""
            SELECT run_id, n_evaluated, mae_model, mae_persistence, mae_cams
            FROM warehouse.ml_metrics
            WHERE n_evaluated > 0
            ORDER BY updated_at DESC
            LIMIT 1
        """)
        holdout = query("""
            SELECT DISTINCT ON (city)
                city, model_version, train_rows, holdout_rows,
                holdout_mae_model, holdout_mae_persistence, trained_at
            FROM warehouse.ml_model
            ORDER BY city, trained_at DESC
        """)
        preds = query("""
            SELECT target_at, city, predicted_pm2_5, persistence_pm2_5, cams_pm2_5, actual_pm2_5
            FROM warehouse.ml_prediction
            WHERE target_at >= (NOW() AT TIME ZONE 'Asia/Makassar') - INTERVAL '48 hours'
            ORDER BY target_at
        """)
        history = query("""
            SELECT run_id, model_version, n_evaluated, mae_model, mae_persistence, mae_cams
            FROM warehouse.ml_metrics
            WHERE n_evaluated > 0
            ORDER BY updated_at DESC
            LIMIT 10
        """)
        recent = query("""
            SELECT created_at, city, target_at, predicted_pm2_5, persistence_pm2_5, cams_pm2_5, actual_pm2_5
            FROM warehouse.ml_prediction
            ORDER BY created_at DESC, target_at
            LIMIT 20
        """)
        observations = query("""
            SELECT t.valid_at, l.city, f.pm2_5, f.pm10, f.european_aqi
            FROM warehouse.fact_air_quality f
            JOIN warehouse.dim_location l USING (location_key)
            JOIN warehouse.dim_time t USING (time_key)
            ORDER BY t.valid_at DESC, l.city
            LIMIT 20
        """)
        volume = query("""
            SELECT COUNT(*) AS total_rows,
                   COUNT(DISTINCT t.valid_at::DATE) AS total_days
            FROM warehouse.fact_air_quality f
            JOIN warehouse.dim_time t USING (time_key)
        """).iloc[0]
    except (psycopg2.Error, OSError) as exc:
        st.warning("Tabel ML belum siap. Pastikan DAG air_quality_ml sudah pernah berjalan.")
        st.code(str(exc))
        return

    if preds.empty and holdout.empty:
        st.info("Belum ada hasil ML. Jalankan DAG air_quality_ml di Airflow (butuh riwayat batch), lalu klik Muat ulang data.")
        return

    def mae(value) -> str:
        return "—" if pd.isna(value) else f"{value:.2f}"

    # Berapa data yang sudah masuk database dan berapa lama rentangnya.
    # Ini memberitahu apakah riwayat cukup untuk evaluasi walk-forward.
    volume_label = (
        f"{int(volume['total_rows']):,} baris ({int(volume['total_days'])} hari)"
    )

    if not evaluated.empty:
        row = evaluated.iloc[0]
        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Prediksi sudah dinilai", f"{int(row['n_evaluated']):,}")
        k2.metric("MAE model (µg/m³)", mae(row["mae_model"]))
        k3.metric("MAE persistence", mae(row["mae_persistence"]))
        k4.metric("MAE CAMS", mae(row["mae_cams"]))
        k5.metric("Data di warehouse", volume_label)
        st.caption(f"MAE walk-forward kumulatif run `{row['run_id']}`: model, baseline persistence, dan forecast CAMS dinilai pada himpunan jam target yang sama setelah targetnya lewat.")
    else:
        valid = holdout.dropna(subset=["holdout_mae_model"])
        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Kota termodel", len(holdout))
        k2.metric("MAE holdout model", mae(valid["holdout_mae_model"].mean()) if not valid.empty else "—")
        k3.metric("MAE holdout persistence", mae(valid["holdout_mae_persistence"].mean()) if not valid.empty else "—")
        trained = pd.to_datetime(holdout["trained_at"]).max() if not holdout.empty else None
        k4.metric("Training terakhir (WITA)", trained.tz_convert("Asia/Makassar").strftime("%d/%m %H:%M") if trained is not None else "—")
        k5.metric("Data di warehouse", volume_label)
        st.caption("Belum ada prediksi matang untuk dinilai (butuh ±9 jam sejak run pertama). Angka di atas = MAE holdout 48 jam terakhir, rata-rata kota.")

    st.subheader("Prediksi vs aktual vs persistence vs CAMS")
    if preds.empty:
        st.info("Belum ada prediksi tersimpan dalam 48 jam terakhir.")
    else:
        city = st.selectbox("Pilih kota", sorted(preds["city"].unique().tolist()), key="ml_city")
        view = preds.loc[preds["city"] == city].copy()
        view["target_at"] = pd.to_datetime(view["target_at"])
        chart = view.pivot_table(
            index="target_at",
            values=["predicted_pm2_5", "persistence_pm2_5", "cams_pm2_5", "actual_pm2_5"],
            aggfunc="mean",
        ).rename(columns={
            "predicted_pm2_5": "Prediksi model",
            "persistence_pm2_5": "Persistence",
            "cams_pm2_5": "CAMS",
            "actual_pm2_5": "Aktual",
        })
        st.line_chart(chart, x_label="Waktu (WITA)", y_label="PM2.5 (µg/m³)")
        st.caption("48 jam terakhir sampai 6 jam ke depan. Garis aktual berhenti di jam sekarang; tiga garis lain adalah ramalan yang dibuat enam jam sebelum jam target.")

    st.subheader("20 prediksi terbaru")
    if recent.empty:
        st.info("Belum ada prediksi tersimpan.")
    else:
        recent["created_at"] = pd.to_datetime(recent["created_at"]).dt.tz_convert("Asia/Makassar")
        st.dataframe(recent, width="stretch", hide_index=True)

    st.subheader("Riwayat MAE per run evaluasi")
    if history.empty:
        st.info("Belum ada run yang selesai dievaluasi. Evaluasi walk-forward butuh ±9 jam sejak prediksi pertama.")
    else:
        history[["mae_model", "mae_persistence", "mae_cams"]] = history[["mae_model", "mae_persistence", "mae_cams"]].round(2)
        st.dataframe(history, width="stretch", hide_index=True)

    st.subheader("20 observasi terbaru yang sudah masuk database")
    if observations.empty:
        st.info("Belum ada observasi di fact table. Jalankan DAG batch lebih dulu.")
    else:
        observations["valid_at"] = pd.to_datetime(observations["valid_at"])
        st.dataframe(observations, width="stretch", hide_index=True)
        st.caption("Baris faktual yang dipakai sebagai bahan latih dan sebagai pembanding 'aktual' di grafik di atas. Nilai kosong tetap kosong, jangan ditafsirkan sebagai nol.")


# ---------------------------------------------------------------- pengaturan jalur

# Tabel flag on/off. Dibuat dashboard karena dashboard satu-satunya penulisnya.
# Init SQL Postgres hanya berjalan di volume baru, jadi tabel dibuat ulang
# saat dashboard start (CREATE TABLE IF NOT EXISTS aman diulang).
SETTINGS_DDL = """
CREATE TABLE IF NOT EXISTS warehouse.pipeline_settings (
    pipeline TEXT PRIMARY KEY,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""

SETTINGS_SEED = """
INSERT INTO warehouse.pipeline_settings (pipeline, enabled)
VALUES ('batch', TRUE), ('streaming', TRUE), ('ml', TRUE)
ON CONFLICT (pipeline) DO NOTHING
"""

# Tiga jalur yang bisa dimatikan, plus keterangan efeknya.
PIPELINES = {
    "batch": {
        "label": "Jalur batch",
        "detail": "DAG air_quality_end_to_end, jadwal tiap 3 jam.",
        "efek": "Dimatikan: run DAG berikutnya dilewati seluruhnya — tidak ada extract, tidak ada Spark, tidak ada data baru.",
        "status_sql": """
            SELECT MAX(t.valid_at) AS last_at
            FROM warehouse.fact_air_quality f
            JOIN warehouse.dim_time t USING (time_key)
        """,
        "status_label": "Data batch terakhir",
    },
    "streaming": {
        "label": "Jalur streaming",
        "detail": "sensor-simulator → Spark Structured Streaming → warehouse.",
        "efek": "Dimatikan: simulator berhenti menulis file dan job Spark menghentikan query. Tidak ada data baru yang masuk dan folder incoming tidak menumpuk.",
        "status_sql": "SELECT MAX(reading_at) AS last_at FROM warehouse.fact_sensor_live",
        "status_label": "Data streaming terakhir",
    },
    "ml": {
        "label": "Jalur ML (prediksi)",
        "detail": "DAG air_quality_ml, jadwal tiap jam.",
        "efek": "Dimatikan: run DAG berikutnya dilewati — features, training, scoring, dan evaluasi tidak dijalankan.",
        "status_sql": "SELECT MAX(target_at) AS last_at FROM warehouse.ml_prediction",
        "status_label": "Prediksi terakhir",
    },
}


def read_settings() -> pd.DataFrame:
    """Baca semua flag on/off yang tersimpan."""
    return query("""
        SELECT pipeline, enabled, updated_at
        FROM warehouse.pipeline_settings
        ORDER BY pipeline
    """)


def set_enabled(pipeline: str, enabled: bool) -> None:
    """Simpan status on/off satu jalur."""
    execute(
        """
        UPDATE warehouse.pipeline_settings
        SET enabled = %s, updated_at = NOW()
        WHERE pipeline = %s
        """,
        (enabled, pipeline),
    )


def last_status(meta: dict) -> str:
    """Jam data terakhir dari sebuah jalur, atau '-' kalau tabelnya belum ada."""
    try:
        value = query(meta["status_sql"]).iloc[0]["last_at"]
    except (psycopg2.Error, OSError, IndexError, KeyError):
        return "—"
    if pd.isna(value):
        return "—"
    return pd.to_datetime(value).strftime("%d/%m/%Y %H:%M")


def render_settings() -> None:
    st.caption("Perubahan berlaku pada run berikutnya. Tidak perlu restart container.")

    # Tabel flag dibuat di sini supaya dashboard tidak bergantung pada
    # DAG yang pernah jalan lebih dulu.
    try:
        try:
            execute(SETTINGS_DDL)
        except psycopg2.Error:
            # Tabelnya sudah ada — CREATE TABLE IF NOT EXISTS bisa kalah
            # race kalau dua sesi dashboard	start bersamaan.
            pass
        execute(SETTINGS_SEED)
        settings = read_settings()
    except (psycopg2.Error, OSError) as exc:
        st.error("Tabel pengaturan belum bisa disiapkan. Pastikan PostgreSQL jalan.")
        st.code(str(exc))
        return

    flags = {row["pipeline"]: row for _, row in settings.iterrows()}

    for key, meta in PIPELINES.items():
        row = flags.get(key)
        enabled = bool(row["enabled"]) if row is not None else True
        updated = row["updated_at"] if row is not None else None

        with st.container(border=True):
            left, right = st.columns([3, 2])
            with left:
                new_state = st.toggle(meta["label"], value=enabled, key=f"toggle_{key}")
            with right:
                st.caption(meta["detail"])

            st.caption(meta["efek"])

            # Widget Streamlit selalu kirim nilainya, jadi perbandingannya
            # dengan nilai di database itu yang membedakan "tidak diubah"
            # dari "baru saja diklik".
            if new_state != enabled:
                set_enabled(key, new_state)
                settings = read_settings()
                flags = {r["pipeline"]: r for _, r in settings.iterrows()}
                if key in flags:
                    updated = flags[key]["updated_at"]
                st.success(
                    f"{meta['label']} {'dinyalakan' if new_state else 'dimatikan'}."
                )

            status_line = f"{meta['status_label']}: {last_status(meta)}"
            if updated is not None:
                diubah = pd.to_datetime(updated)
                if diubah.tzinfo is not None:
                    diubah = diubah.tz_convert("Asia/Makassar")
                status_line += f" · diubah {diubah.strftime('%d/%m/%Y %H:%M')}"
            st.caption(status_line)


st.set_page_config(page_title="Kualitas Udara | Praktikum", layout="wide")
st.title("Kualitas Udara Empat Kota")
st.caption("Praktikum Big Data · waktu Asia/Makassar (WITA)")

tab_live, tab_batch, tab_ml, tab_settings = st.tabs(
    ["Live (streaming)", "Per jam (batch)", "Prediksi (ML)", "Pengaturan"]
)
with tab_live:
    st.caption("Jalur streaming: sensor simulasi → Spark Structured Streaming → warehouse. Diperbarui otomatis tiap 5 detik.")
    render_live()
with tab_batch:
    st.caption("Jalur batch: Open-Meteo → Airflow → Spark → warehouse. Diperbarui tiap run DAG (tiap 3 jam).")
    render_batch()
with tab_ml:
    st.caption("Jalur ML (E4): model ridge per kota → prediksi PM2.5 enam jam ke depan, dibandingkan baseline persistence dan forecast CAMS. Diperbarui tiap run DAG air_quality_ml (tiap jam).")
    render_ml()
with tab_settings:
    st.caption("Kontrol on/off tiap jalur pipeline. Perubahan berlaku pada run berikutnya.")
    render_settings()
