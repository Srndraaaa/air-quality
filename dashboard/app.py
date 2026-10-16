"""Dashboard kelas: seluruh angka berasal dari fact dan dimension warehouse.

Dua tab, satu per jalur:
- Live (streaming): data sensor simulasi, diperbarui otomatis tiap 5 detik.
- Per jam (batch): data Open-Meteo yang dimuat DAG Airflow.
"""

import os
from contextlib import closing

import pandas as pd
import psycopg2
import streamlit as st


def query(sql: str) -> pd.DataFrame:
    with closing(psycopg2.connect(
        host=os.getenv("DW_HOST", "postgres"),
        port=os.getenv("DW_PORT", "5432"),
        dbname=os.getenv("DW_DB", "air_quality_dw"),
        user=os.getenv("DW_USER", "platform"),
        password=os.getenv("DW_PASSWORD", "platform123"),
        connect_timeout=5,
    )) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            columns = [column[0] for column in cur.description]
    return pd.DataFrame(rows, columns=columns)


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
            SELECT EXTRACT(EPOCH FROM (NOW() AT TIME ZONE 'Asia/Jakarta') - MAX(reading_at)) AS seconds
            FROM warehouse.fact_sensor_live
        """).iloc[0]["seconds"]
        live = query("""
            SELECT f.reading_at, l.city, f.pm2_5
            FROM warehouse.fact_sensor_live f
            JOIN warehouse.dim_location l USING (location_key)
            WHERE f.reading_at >= (NOW() AT TIME ZONE 'Asia/Jakarta') - INTERVAL '10 minutes'
            ORDER BY f.reading_at
        """)
        raw = query("""
            SELECT received_at AT TIME ZONE 'Asia/Jakarta' AS diterima, dirty_type AS jenis,
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
            st.line_chart(trend, x_label="Waktu (WIB)", y_label="PM2.5 (µg/m³)")
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
    k4.metric("Jam data terakhir (WIB)", view["valid_at"].max().strftime("%d/%m/%Y %H:%M"))

    st.subheader("PM2.5 per jam dalam tujuh hari terakhir")
    trend = recent.pivot_table(index="valid_at", columns="city", values="pm2_5", aggfunc="mean")
    if trend.empty:
        st.info("Tidak ada nilai PM2.5 pada rentang ini.")
    else:
        st.line_chart(trend, x_label="Waktu (WIB)", y_label="PM2.5 (µg/m³)")

    st.subheader("Rerata polutan per kota dalam tujuh hari terakhir")
    means = recent.groupby("city", as_index=True)[["pm2_5", "pm10"]].mean()
    st.bar_chart(means, x_label="Kota", y_label="Konsentrasi (µg/m³)")

    st.subheader("50 catatan terbaru")
    st.dataframe(view.head(50), width="stretch", hide_index=True)
    st.caption("Ini data model kualitas udara dari Open-Meteo/CAMS, bukan pembacaan sensor lapangan. Nilai kosong tetap kosong; jangan ditafsirkan sebagai nol. Indeks AQI Eropa hanya ditampilkan jika tersedia.")


st.set_page_config(page_title="Kualitas Udara | Praktikum", layout="wide")
st.title("Kualitas Udara Lima Kota")
st.caption("Praktikum Big Data · waktu Asia/Jakarta")

tab_live, tab_batch = st.tabs(["Live (streaming)", "Per jam (batch)"])
with tab_live:
    st.caption("Jalur streaming: sensor simulasi → Spark Structured Streaming → warehouse. Diperbarui otomatis tiap 5 detik.")
    render_live()
with tab_batch:
    st.caption("Jalur batch: Open-Meteo → Airflow → Spark → warehouse. Diperbarui tiap run DAG (tiap 3 jam).")
    render_batch()
