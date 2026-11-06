"""Dashboard kelas: seluruh angka berasal dari fact dan dimension warehouse.

Lima tab:
- Live (streaming): data sensor simulasi, diperbarui otomatis tiap 5 detik.
- Per jam (batch): data Open-Meteo yang dimuat DAG Airflow.
- Analitik (batch): peta status per wilayah + ringkasan PM2.5 per kota.
- Prediksi (ML): hasil model ridge (E4) vs baseline persistence dan CAMS.
- Pengaturan: tombol on/off untuk tiap jalur pipeline.
"""

import json
import os
from contextlib import closing
from pathlib import Path

import altair as alt
import pandas as pd
import psycopg2
import pydeck as pdk
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


def read_cities() -> list[str]:
    """Daftar kota dari dimension, supaya card tetap tampil meski fact kosong."""
    return query("SELECT city FROM warehouse.dim_location ORDER BY city")["city"].tolist()


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
        last_load = query("""
            SELECT MAX(loaded_at) AT TIME ZONE 'Asia/Makassar' AS last_at
            FROM warehouse.fact_air_quality
        """).iloc[0]["last_at"]
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
    k4.metric(
        "Warehouse terakhir dimuat (WITA)",
        pd.to_datetime(last_load).strftime("%d/%m/%Y %H:%M") if pd.notna(last_load)
        else "Belum ada",
    )

    st.subheader("PM2.5 per jam dalam tujuh hari terakhir")
    trend = recent.pivot_table(index="valid_at", columns="city", values="pm2_5", aggfunc="mean")
    if trend.empty:
        st.info("Tidak ada nilai PM2.5 pada rentang ini.")
    else:
        st.line_chart(trend, x_label="Waktu (WITA)", y_label="PM2.5 (µg/m³)")

    st.subheader("Rerata polutan per kota dalam tujuh hari terakhir")
    means = recent.groupby("city", as_index=True)[["pm2_5", "pm10"]].mean()
    st.bar_chart(means, x_label="Kota", y_label="Konsentrasi (µg/m³)")

    st.caption("Ini data model kualitas udara dari Open-Meteo/CAMS, bukan pembacaan sensor lapangan. Nilai kosong tetap kosong; jangan ditafsirkan sebagai nol.")


# ---------------------------------------------------------------- analitik per kota

# Ambang konsentrasi PM2.5 (µg/m³) yang dipakai ISPU. Ini bukan nilai
# indeks AQI, hanya klasifikasi konsentrasi supaya card mudah dibaca.
PM25_BANDS = (
    (15.5, "Baik"),
    (55.4, "Sedang"),
    (150.4, "Tidak sehat"),
    (250.4, "Sangat tidak sehat"),
)


def pm25_status(value: float | None) -> str:
    """Label status dari konsentrasi PM2.5, atau 'Data belum tersedia'."""
    if value is None or pd.isna(value):
        return "Data belum tersedia"
    for upper, label in PM25_BANDS:
        if value <= upper:
            return label
    return "Berbahaya"


# Warna badge markdown dan isi polygon peta per status, supaya card dan
# peta tidak pernah menampilkan kategori yang berbeda.
STATUS_STYLE = {
    "Baik": ("green", [46, 158, 91]),
    "Sedang": ("blue", [59, 130, 196]),
    "Tidak sehat": ("yellow", [227, 181, 5]),
    "Sangat tidak sehat": ("orange", [224, 123, 26]),
    "Berbahaya": ("red", [192, 57, 43]),
    "Data belum tersedia": ("gray", [195, 199, 204]),
}

# Kota di dim_location adalah ibu kota provinsi. Pasangan ini memakai
# shapeISO supaya tidak bergantung pada nama Inggris di GeoJSON.
CITY_PROVINCE = {
    "Palu": "ID-ST",
    "Mamuju": "ID-SR",
    "Gorontalo": "ID-GO",
    "Manado": "ID-SA",
}

GEOJSON_PATH = Path(__file__).with_name("assets") / "sulawesi.geojson"


def load_boundaries() -> dict | None:
    """Batas provinsi dari aset lokal; None kalau aset tidak terbaca."""
    try:
        with GEOJSON_PATH.open(encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, ValueError):
        return None


def summarize_city(
    city: str,
    city_data: pd.DataFrame,
    now_floor: pd.Timestamp,
) -> dict:
    """Angka satu kota; dipakai card dan peta supaya tidak dihitung dua kali."""
    valid = city_data.dropna(subset=["pm2_5"]).sort_values("valid_at")
    latest = valid.iloc[-1] if not valid.empty else None

    last7d = city_data[city_data["valid_at"] > now_floor - pd.Timedelta(days=7)]
    avg7 = last7d["pm2_5"].mean()

    return {
        "city": city,
        "value": None if latest is None else float(latest["pm2_5"]),
        "value_at": None if latest is None else latest["valid_at"],
        "avg7": None if pd.isna(avg7) else float(avg7),
        "n7": int(last7d["pm2_5"].notna().sum()),
        "last_row_at": city_data["valid_at"].max() if not city_data.empty else None,
        "status": pm25_status(None if latest is None else latest["pm2_5"]),
    }


def render_map(summaries: dict[str, dict], boundaries: dict) -> None:
    """Choropleth: warna tiap provinsi mengikuti status kotanya."""
    iso_status = {
        CITY_PROVINCE[city]: item["status"]
        for city, item in summaries.items()
        if city in CITY_PROVINCE
    }
    features = boundaries.get("features", [])

    # Satu layer per status supaya warna tetap konstan tanpa accessor string.
    layers = []
    for status, (_, color) in STATUS_STYLE.items():
        subset = [
            feature
            for feature in features
            if iso_status.get(
                feature["properties"].get("shapeISO"), "Data belum tersedia"
            )
            == status
        ]
        if not subset:
            continue
        layers.append(
            pdk.Layer(
                "GeoJsonLayer",
                data={"type": "FeatureCollection", "features": subset},
                get_fill_color=color,
                get_line_color=[255, 255, 255],
                line_width_min_pixels=1,
                pickable=True,
                auto_highlight=True,
            )
        )

    deck = pdk.Deck(
        layers=layers,
        initial_view_state=pdk.ViewState(latitude=-2.0, longitude=120.5, zoom=6),
        # Basemap terang tanpa label: polygon tetap dominan, tile butuh internet.
        map_style=pdk.map_styles.CARTO_LIGHT_NO_LABELS,
    )
    legend = "  ".join(
        f":{color}-badge[{label}]" for label, (color, _) in STATUS_STYLE.items()
    )

    with st.container(border=True):
        st.markdown("#### Peta status per wilayah")
        st.pydeck_chart(deck, height=420)
        st.markdown(legend)
        st.caption(
            "Warna = status PM2.5 kota; provinsi tanpa kota pada dataset "
            "berwarna abu-abu. Batas wilayah: geoBoundaries (ODbL 1.0)."
        )


def render_city_card(summary: dict, now_floor: pd.Timestamp) -> None:
    """Card ringkas: status menempel di nama kota, dua angka sejajar."""
    with st.container(border=True):
        color = STATUS_STYLE[summary["status"]][0]
        st.markdown(
            f"**{summary['city']}** &nbsp; "
            f":{color}-badge[{summary['status']}]"
        )

        left, right = st.columns(2)
        left.metric(
            "PM2.5 terbaru",
            "—" if summary["value"] is None else f"{summary['value']:.1f} µg/m³",
        )
        right.metric(
            "Rerata 7 hari",
            "—" if summary["avg7"] is None else f"{summary['avg7']:.1f} µg/m³",
        )

        notes = []
        if summary["value_at"] is not None:
            notes.append(f"Data {summary['value_at']:%d/%m %H:%M} WITA")
        if summary["n7"]:
            notes.append(f"{summary['n7']} jam berisi")
        if (
            summary["value_at"] is not None
            and summary["last_row_at"] is not None
            and summary["value_at"] < summary["last_row_at"]
        ):
            notes.append(
                f"baris {summary['last_row_at']:%d/%m %H:%M} belum berisi PM2.5"
            )
        if (
            summary["last_row_at"] is not None
            and summary["last_row_at"] < now_floor - pd.Timedelta(hours=6)
        ):
            lag = int(
                (now_floor - summary["last_row_at"]).total_seconds() // 3600
            )
            notes.append(f"⚠️ batch tertunda {lag} jam")
        if notes:
            st.caption(" · ".join(notes))


def render_analytics() -> None:
    """Ringkasan PM2.5 per kota dari jalur batch."""
    if st.button("Muat ulang data", key="analytics_reload"):
        st.rerun()

    try:
        cities = read_cities()
        data = read_fact()
    except (psycopg2.Error, OSError) as exc:
        st.error(
            "Database belum siap. Periksa status PostgreSQL dan "
            "jalankan pipeline Airflow."
        )
        st.code(str(exc))
        return

    if not cities:
        st.info(
            "Belum ada lokasi di warehouse. Jalankan DAG "
            "air_quality_end_to_end lebih dulu."
        )
        return
    if data.empty:
        st.info(
            "Belum ada baris di fact table. Jalankan DAG "
            "air_quality_end_to_end lalu klik Muat ulang data."
        )
        return

    data["valid_at"] = pd.to_datetime(data["valid_at"])

    # Jam sekarang WITA, naive seperti kolom valid_at. Baris forecast
    # (valid_at di masa depan) tidak boleh dihitung sebagai observasi.
    now_floor = pd.Timestamp.now(tz="Asia/Makassar").tz_localize(None).floor("h")
    observed = data[data["valid_at"] <= now_floor]

    summaries = {
        city: summarize_city(city, observed[observed["city"] == city], now_floor)
        for city in cities
    }

    boundaries = load_boundaries()
    if boundaries is None:
        st.warning(
            "Aset batas wilayah tidak terbaca (dashboard/assets/sulawesi.geojson); "
            "peta dilewati."
        )
    else:
        render_map(summaries, boundaries)

    for start in range(0, len(cities), 2):
        cols = st.columns(2)
        for col, city in zip(cols, cities[start:start + 2]):
            with col:
                render_city_card(summaries[city], now_floor)

    render_detailed_analytics(data, cities, now_floor)


def analytics_periods(now_floor: pd.Timestamp, days: int) -> tuple:
    """Batas dua periode jam yang berdampingan, akhir tidak termasuk."""
    end = now_floor + pd.Timedelta(hours=1)
    current_start = end - pd.Timedelta(days=days)
    return current_start - pd.Timedelta(days=days), current_start, end


def analytics_summary(
    data: pd.DataFrame, cities: list[str], pollutant: str,
    now_floor: pd.Timestamp, days: int,
) -> pd.DataFrame:
    """Rata-rata dan cakupan nilai valid per kota dan periode."""
    previous_start, current_start, end = analytics_periods(now_floor, days)
    rows = []
    for city in cities:
        for period, start, stop in (
            ("Kini", current_start, end),
            ("Sebelumnya", previous_start, current_start),
        ):
            values = data.loc[
                (data["city"] == city) & (data["valid_at"] >= start)
                & (data["valid_at"] < stop), pollutant
            ].dropna()
            expected = days * 24
            rows.append({
                "city": city, "period": period, "valid": len(values),
                "expected": expected, "mean": values.mean() if len(values) else None,
                "coverage": len(values) / expected,
            })
    return pd.DataFrame(rows, columns=["city", "period", "valid", "expected", "mean", "coverage"])


def analytics_patterns(data: pd.DataFrame, pollutant: str) -> tuple:
    """Rerata dan jumlah nilai valid menurut jam serta tanggal WITA."""
    valid = data.dropna(subset=[pollutant]).copy()
    valid["Jam"] = valid["valid_at"].dt.hour
    valid["Tanggal"] = valid["valid_at"].dt.normalize()
    hourly = valid.groupby(["city", "Jam"], as_index=False).agg(
        mean=(pollutant, "mean"), valid=(pollutant, "count")
    )
    daily = valid.groupby(["city", "Tanggal"], as_index=False).agg(
        mean=(pollutant, "mean"), valid=(pollutant, "count")
    )
    return hourly, daily


def aggregate_heatmap(data: pd.DataFrame, city: str, pollutant: str) -> pd.DataFrame:
    """Rerata dan jumlah jam valid per hari dan jam untuk satu kota."""
    valid = data.loc[
        (data["city"] == city) & data["valid_at"].notna() & data[pollutant].notna(),
        ["valid_at", pollutant],
    ].copy()
    valid["day"] = valid["valid_at"].dt.dayofweek
    valid["hour"] = valid["valid_at"].dt.hour
    grouped = valid.groupby(["day", "hour"])[pollutant].agg(
        mean="mean", valid_hours="count"
    )
    grid = pd.MultiIndex.from_product([range(7), range(24)], names=["day", "hour"])
    result = grouped.reindex(grid).reset_index()
    result["valid_hours"] = result["valid_hours"].fillna(0).astype(int)
    return result


def analytics_distribution(
    data: pd.DataFrame, cities: list[str], pollutant: str,
) -> pd.DataFrame:
    """Ringkasan sebaran nilai valid per kota pada data periode terpilih."""
    rows = []
    for city in cities:
        values = data.loc[data["city"] == city, pollutant].dropna()
        if values.empty:
            continue
        q1 = values.quantile(0.25)
        q3 = values.quantile(0.75)
        iqr = q3 - q1
        inside = values[(values >= q1 - 1.5 * iqr) & (values <= q3 + 1.5 * iqr)]
        rows.append({
            "city": city, "minimum": inside.min(), "q1": q1,
            "median": values.median(), "q3": q3,
            "maximum": inside.max(), "valid_hours": len(values),
        })
    return pd.DataFrame(
        rows, columns=["city", "minimum", "q1", "median", "q3", "maximum", "valid_hours"]
    )


def analytics_distribution_outliers(
    data: pd.DataFrame, distribution: pd.DataFrame, pollutant: str,
) -> pd.DataFrame:
    """Observasi di luar pagar 1,5 IQR beserta waktu dan kotanya."""
    values = data.loc[
        data[pollutant].notna() & data["valid_at"].notna(),
        ["city", "valid_at", pollutant],
    ].merge(distribution[["city", "q1", "q3"]], on="city", how="inner")
    iqr = values["q3"] - values["q1"]
    outliers = (values[pollutant] < values["q1"] - 1.5 * iqr) | (
        values[pollutant] > values["q3"] + 1.5 * iqr
    )
    return values.loc[outliers, ["city", "valid_at", pollutant]].copy()


def analytics_exceedance(
    data: pd.DataFrame, cities: list[str], pollutant: str,
    start: pd.Timestamp, end: pd.Timestamp, threshold: float,
) -> pd.DataFrame:
    """Jumlah dan persentase nilai valid yang melampaui ambang per kota."""
    rows = []
    for city in cities:
        values = data.loc[
            (data["city"] == city) & (data["valid_at"] >= start)
            & (data["valid_at"] < end), pollutant
        ].dropna()
        count = len(values)
        above = int((values > threshold).sum())
        rows.append({"city": city, "above": above, "valid": count,
                     "percent": 100 * above / count if count else None})
    return pd.DataFrame(rows, columns=["city", "above", "valid", "percent"])


def render_detailed_analytics(data: pd.DataFrame, cities: list[str], now_floor: pd.Timestamp) -> None:
    st.divider()
    st.subheader("Analisis perbandingan")
    filters = st.columns(3)
    selected = filters[0].multiselect("Kota", cities, default=cities, key="detail_cities")
    days = filters[1].selectbox("Periode", [7, 14, 30], format_func=lambda n: f"{n} hari")
    label = filters[2].selectbox("Polutan", ["PM2.5", "PM10"])
    pollutant = {"PM2.5": "pm2_5", "PM10": "pm10"}[label]
    threshold = st.number_input(f"Ambang {label} (µg/m³)", min_value=0.0, value=35.0)
    if not selected:
        st.info("Pilih setidaknya satu kota untuk melihat analisis.")
        return

    previous_start, current_start, end = analytics_periods(now_floor, days)
    summary = analytics_summary(data, selected, pollutant, now_floor, days)
    comparison = []
    for city in selected:
        city_rows = summary[summary["city"] == city].set_index("period")
        current, previous = city_rows.loc["Kini"], city_rows.loc["Sebelumnya"]
        change = current["mean"] - previous["mean"] if current["valid"] and previous["valid"] else None
        comparison.append({
            "Kota": city, "Rerata kini": current["mean"],
            "Rerata sebelumnya": previous["mean"],
            "Perubahan": change,
        })
    st.markdown(f"**Perbandingan {label} (µg/m³)**")
    st.dataframe(pd.DataFrame(comparison), hide_index=True, width="stretch")
    st.caption("Perubahan = kini − sebelumnya.")
    if (summary["valid"] < summary["expected"]).any():
        st.warning("Cakupan belum lengkap; perbandingan dapat dipengaruhi jam yang hilang.")

    current = data.loc[
        data["city"].isin(selected) & (data["valid_at"] >= current_start)
        & (data["valid_at"] <= now_floor), ["city", "valid_at", pollutant]
    ].dropna(subset=[pollutant]).copy()
    if current.empty:
        st.info("Tidak ada nilai valid pada periode kini; pilih kota atau periode lain.")
        return
    city_means = current.groupby("city")[pollutant].mean().sort_values(ascending=False)
    if len(city_means) > 1:
        st.write(f"Rerata tertinggi: **{city_means.index[0]}** ({city_means.iloc[0]:.1f} µg/m³).")

    hourly, daily = analytics_patterns(current, pollutant)
    st.markdown("**Pola rata-rata menurut jam dan tanggal**")
    st.caption("Rata-rata nilai valid pada periode kini; jumlah jam tiap kelompok dapat berbeda.")
    charts = st.columns(2)
    for column, frame, x in (
        (charts[0], hourly, "Jam:Q"),
        (charts[1], daily, "Tanggal:T"),
    ):
        chart = alt.Chart(frame).mark_line(point=True).encode(
            x=alt.X(x, title=x.split(":")[0]),
            y=alt.Y("mean:Q", title=f"{label} (µg/m³)"),
            color=alt.Color("city:N", title="Kota"),
            tooltip=["city:N", x, alt.Tooltip("mean:Q", title="Rerata", format=".1f"),
                     alt.Tooltip("valid:Q", title="Jam valid")],
        )
        column.altair_chart(chart, use_container_width=True)

    st.markdown("**Pola hari dan jam**")
    st.caption("Warna menunjukkan rerata; sel kosong berarti tidak ada jam valid.")
    heatmap_city = st.selectbox("Kota untuk heatmap", selected, key="heatmap_city")
    heatmap = aggregate_heatmap(current, heatmap_city, pollutant)
    if heatmap["valid_hours"].sum() == 0:
        st.info("Tidak ada nilai valid untuk kota ini; pilih kota atau periode lain.")
    else:
        days_of_week = ["Senin", "Selasa", "Rabu", "Kamis", "Jumat", "Sabtu", "Minggu"]
        heatmap["Hari"] = heatmap["day"].map(dict(enumerate(days_of_week)))
        chart = alt.Chart(heatmap).mark_rect().encode(
            x=alt.X("hour:O", title="Jam WITA", scale=alt.Scale(domain=list(range(24)))),
            y=alt.Y("Hari:N", title="Hari", scale=alt.Scale(domain=days_of_week)),
            color=alt.Color("mean:Q", title=f"{label} (µg/m³)", scale=alt.Scale(scheme="blues")),
            tooltip=[
                alt.Tooltip("Hari:N", title="Hari"),
                alt.Tooltip("hour:O", title="Jam WITA"),
                alt.Tooltip("mean:Q", title="Rerata (µg/m³)", format=".1f"),
                alt.Tooltip("valid_hours:Q", title="Jam valid"),
            ],
        ).transform_filter("isValid(datum.mean)")
        st.altair_chart(chart, use_container_width=True)

    st.markdown(f"**Sebaran {label} per kota**")
    distribution = analytics_distribution(current, selected, pollutant)
    missing = [city for city in selected if city not in distribution["city"].values]
    if missing:
        st.info(f"Tidak ada nilai valid untuk: {', '.join(missing)}.")
    tooltip = [
        alt.Tooltip("city:N", title="Kota"),
        alt.Tooltip("minimum:Q", title=f"Whisker bawah {label} (µg/m³)", format=".1f"),
        alt.Tooltip("q1:Q", title=f"Kuartil 1 {label} (µg/m³)", format=".1f"),
        alt.Tooltip("median:Q", title=f"Median {label} (µg/m³)", format=".1f"),
        alt.Tooltip("q3:Q", title=f"Kuartil 3 {label} (µg/m³)", format=".1f"),
        alt.Tooltip("maximum:Q", title=f"Whisker atas {label} (µg/m³)", format=".1f"),
        alt.Tooltip("valid_hours:Q", title="Jam valid"),
    ]
    base = alt.Chart(distribution).encode(
        x=alt.X("city:N", title="Kota", sort=selected),
        tooltip=tooltip,
    )
    axis = alt.Y("minimum:Q", title=f"{label} (µg/m³)")
    whisker = base.mark_rule().encode(y=axis, y2="maximum:Q")
    box = base.mark_bar(size=30).encode(
        y=alt.Y("q1:Q", title=f"{label} (µg/m³)"), y2="q3:Q"
    )
    median = base.mark_tick(color="white", thickness=2, size=30).encode(
        y=alt.Y("median:Q", title=f"{label} (µg/m³)")
    )
    outliers = analytics_distribution_outliers(current, distribution, pollutant)
    outliers["waktu_wita"] = outliers["valid_at"].dt.strftime("%d/%m/%Y %H:%M")
    outliers["waktu_wita"] = outliers.groupby(["city", pollutant])["waktu_wita"].transform(
        lambda times: ", ".join(times)
    )
    points = alt.Chart(outliers).mark_point(filled=True, size=55).encode(
        x=alt.X("city:N", title="Kota", sort=selected),
        y=alt.Y(f"{pollutant}:Q", title=f"{label} (µg/m³)"),
        tooltip=[
            alt.Tooltip("city:N", title="Kota"),
            alt.Tooltip("waktu_wita:N", title="Tanggal dan jam WITA"),
            alt.Tooltip(f"{pollutant}:Q", title=f"{label} (µg/m³)", format=".2f"),
        ],
    )
    st.altair_chart(
        whisker + box + median + points,
        use_container_width=True,
        key=f"distribution_{pollutant}",
    )

    st.markdown(f"**Jam di atas {threshold:g} µg/m³**")
    exceedance = analytics_exceedance(data, selected, pollutant, current_start, end, threshold)
    st.caption("Persentase = jam di atas ambang / jam dengan nilai valid pada periode kini.")
    st.dataframe(
        exceedance[["city", "above", "percent"]].rename(
            columns={"city": "Kota", "above": "Jam di atas ambang", "percent": "Persen (%)"}
        ),
        hide_index=True, width="stretch",
    )


# ---------------------------------------------------------------- jalur ML (E4)

def chart_series(values: pd.DataFrame) -> pd.DataFrame:
    return pd.concat([
        pd.DataFrame({
            "waktu": values.index, "nilai": values[label].values, "seri": label,
            "segmen": values[label].isna().cumsum().values,
        })
        for label in values.columns
    ], ignore_index=True).dropna(subset=["nilai"])


def render_ml() -> None:
    if st.button("Muat ulang data", key="ml_reload"):
        st.rerun()

    metric_horizon = st.selectbox(
        "Horizon metrik", range(1, 7), index=5, format_func=lambda h: f"+{h} jam"
    )

    try:
        evaluated = query(f"""
            SELECT run_id, horizon_hours, n_evaluated, mae_model, mae_persistence, mae_cams
            FROM warehouse.ml_metrics
            WHERE n_evaluated > 0 AND horizon_hours = {metric_horizon}
            ORDER BY updated_at DESC
            LIMIT 1
        """)
        holdout = query(f"""
            SELECT DISTINCT ON (city)
                city, horizon_hours, model_version, train_rows, holdout_rows,
                holdout_mae_model, holdout_mae_persistence, trained_at
            FROM warehouse.ml_model
            WHERE horizon_hours = {metric_horizon}
            ORDER BY city, trained_at DESC
        """)
        preds = query("""
            SELECT p.city, p.origin_at, p.run_id, hours.valid_at,
                   f.pm2_5, h.predicted_pm2_5 AS historical_model,
                   h.cams_pm2_5 AS historical_cams,
                   future.predicted_pm2_5, future.cams_pm2_5
            FROM (
                SELECT DISTINCT ON (city) city, origin_at, run_id
                FROM warehouse.ml_prediction
                ORDER BY city, created_at DESC, prediction_id DESC
            ) p
            CROSS JOIN LATERAL generate_series(
                p.origin_at - INTERVAL '5 hours', p.origin_at + INTERVAL '6 hours',
                INTERVAL '1 hour'
            ) AS hours(valid_at)
            LEFT JOIN warehouse.dim_location l ON l.city = p.city
            LEFT JOIN warehouse.dim_time t
                ON t.valid_at = hours.valid_at
                AND t.valid_at <= (NOW() AT TIME ZONE 'Asia/Makassar')
            LEFT JOIN warehouse.fact_air_quality f
                ON f.location_key = l.location_key AND f.time_key = t.time_key
            LEFT JOIN warehouse.ml_prediction future
                ON future.city = p.city AND future.run_id = p.run_id
                AND future.origin_at = p.origin_at AND future.target_at = hours.valid_at
                AND future.target_at > p.origin_at
            LEFT JOIN LATERAL (
                SELECT predicted_pm2_5, cams_pm2_5
                FROM warehouse.ml_prediction h
                WHERE h.city = p.city AND h.target_at = hours.valid_at
                  AND h.target_at = h.origin_at + INTERVAL '6 hours'
                  AND (h.created_at AT TIME ZONE 'Asia/Makassar') < h.target_at
                ORDER BY h.created_at DESC, h.prediction_id DESC
                LIMIT 1
            ) h ON TRUE
            ORDER BY p.city, hours.valid_at
        """)
        cities = read_cities()
        history = query(f"""
            SELECT run_id, horizon_hours, model_version, n_evaluated,
                   mae_model, mae_persistence, mae_cams
            FROM warehouse.ml_metrics
            WHERE n_evaluated > 0 AND horizon_hours = {metric_horizon}
            ORDER BY updated_at DESC
            LIMIT 10
        """)
        recent = query("""
            SELECT created_at, city, origin_at, target_at, horizon_hours,
                   predicted_pm2_5, persistence_pm2_5, cams_pm2_5, actual_pm2_5
            FROM warehouse.ml_prediction
            ORDER BY created_at DESC, target_at
            LIMIT 20
        """)
        volume = query("""
            SELECT COUNT(*) AS total_rows,
                   COUNT(DISTINCT t.valid_at::DATE) AS total_days
            FROM warehouse.fact_air_quality f
            JOIN warehouse.dim_time t USING (time_key)
        """).iloc[0]
    except (psycopg2.Error, OSError) as exc:
        st.error("Gagal membaca data ML dari warehouse. Periksa detail berikut.")
        st.code(str(exc))
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
        st.caption(f"MAE walk-forward horizon +{metric_horizon} jam, run `{row['run_id']}`: model, persistence, dan CAMS dinilai pada himpunan target yang sama setelah target lewat.")
    else:
        valid = holdout.dropna(subset=["holdout_mae_model"])
        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Kota termodel", len(holdout))
        k2.metric("MAE holdout model", mae(valid["holdout_mae_model"].mean()) if not valid.empty else "—")
        k3.metric("MAE holdout persistence", mae(valid["holdout_mae_persistence"].mean()) if not valid.empty else "—")
        trained = pd.to_datetime(holdout["trained_at"]).max() if not holdout.empty else None
        k4.metric("Training terakhir (WITA)", trained.tz_convert("Asia/Makassar").strftime("%d/%m %H:%M") if trained is not None else "—")
        k5.metric("Data di warehouse", volume_label)
        st.caption(f"Belum ada prediksi horizon +{metric_horizon} jam yang matang untuk dinilai (target ditambah jeda 3 jam). Angka di atas = MAE holdout 48 jam terakhir, rata-rata kota.")

    st.subheader("PM2.5 enam jam historis dan prediksi enam jam ke depan")
    if not cities:
        st.info("Belum ada kota di warehouse. Jalankan DAG batch, lalu muat ulang data.")
    else:
        city = st.selectbox("Pilih kota", cities, key="ml_city")
        view = preds.loc[preds["city"] == city]
        if view.empty:
            st.info(
                "Belum ada prediksi untuk kota ini. "
                "Periksa data batch dan jalankan DAG air_quality_ml, lalu muat ulang."
            )
        else:
            prediction = view.iloc[0]
            origin = pd.Timestamp(prediction["origin_at"])
            values = view.set_index("valid_at")[
                ["pm2_5", "historical_model", "historical_cams",
                 "predicted_pm2_5", "cams_pm2_5"]
            ]
            values.index = pd.to_datetime(values.index)
            values = values.reindex(pd.date_range(start=origin - pd.Timedelta(hours=5),
                                                  periods=12, freq="h"))
            values.loc[values.index > origin,
                       ["pm2_5", "historical_model", "historical_cams"]] = None
            values.loc[values.index <= origin, ["predicted_pm2_5", "cams_pm2_5"]] = None
            values.columns = [
                "Data aktual", "Model historis operasional +6", "CAMS historis +6",
                "Prediksi model", "CAMS",
            ]
            for label in values.columns:
                column = values[label]
                count = column.notna().sum()
                if count < 6 and label not in ("Model historis operasional +6", "CAMS historis +6"):
                    st.info(f"{label}: {count} dari 6 jam tersedia; jam kosong tidak diisi.")
            if pd.notna(values.loc[origin, "Data aktual"]):
                for label in ("Prediksi model", "CAMS"):
                    if pd.notna(values.loc[origin + pd.Timedelta(hours=1), label]):
                        values.loc[origin, label] = values.loc[origin, "Data aktual"]
            chart = chart_series(values)
            if not chart.empty:
                plot = alt.Chart(chart).mark_line(point=True).encode(
                    x=alt.X("waktu:T", title="Waktu (WITA)"),
                    y=alt.Y("nilai:Q", title="PM2.5 (µg/m³)"),
                    color=alt.Color("seri:N", title="Seri"),
                    detail="segmen:N",
                )
                st.altair_chart(plot, use_container_width=True)
            else:
                st.info("Tidak ada nilai historis atau prediksi yang dapat digambar untuk run ini.")
            st.caption(
                f"Run `{prediction['run_id']}`; asal: {origin:%d/%m/%Y %H:%M} WITA; "
                "target +1 sampai +6 jam. Fakta historis adalah Open-Meteo/CAMS, bukan sensor lapangan. "
                "Model historis operasional dan CAMS historis adalah snapshot horizon +6 terbaru "
                "sebelum tiap target; titik dapat berasal dari run berbeda. "
                "Garis prediksi dimulai dari data aktual "
                "pada waktu asal; celah berarti nilai tidak tersedia."
            )

    st.subheader("20 prediksi terbaru")
    if recent.empty:
        st.info("Belum ada prediksi tersimpan.")
    else:
        recent["created_at"] = pd.to_datetime(recent["created_at"]).dt.tz_convert("Asia/Makassar")
        st.dataframe(recent, width="stretch", hide_index=True)

    st.subheader("Riwayat MAE per run evaluasi")
    if history.empty:
        st.info(f"Belum ada evaluasi horizon +{metric_horizon} jam. Evaluasi memerlukan target yang telah lewat dan jeda 3 jam.")
    else:
        history[["mae_model", "mae_persistence", "mae_cams"]] = history[["mae_model", "mae_persistence", "mae_cams"]].round(2)
        st.dataframe(history, width="stretch", hide_index=True)



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
            SELECT MAX(loaded_at) AT TIME ZONE 'Asia/Makassar' AS last_at
            FROM warehouse.fact_air_quality
        """,
        "status_label": "Warehouse terakhir dimuat (WITA)",
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
    """Waktu terakhir jalur, atau keterangan jika kosong atau gagal dibaca."""
    try:
        value = query(meta["status_sql"]).iloc[0]["last_at"]
    except (psycopg2.Error, OSError, IndexError, KeyError):
        return "Gagal membaca status; periksa PostgreSQL"
    if pd.isna(value):
        return "Belum ada data"
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

tab_live, tab_batch, tab_analytics, tab_ml, tab_settings = st.tabs(
    [
        "Live (streaming)",
        "Per jam (batch)",
        "Analitik",
        "Prediksi (ML)",
        "Pengaturan",
    ]
)
with tab_live:
    st.caption("Jalur streaming: sensor simulasi → Spark Structured Streaming → warehouse. Diperbarui otomatis tiap 5 detik.")
    render_live()
with tab_batch:
    st.caption("Jalur batch: Open-Meteo → Airflow → Spark → warehouse. Diperbarui tiap run DAG (tiap 3 jam).")
    render_batch()
with tab_analytics:
    st.caption(
        "Ringkasan batch per kota. Nilai dari model Open-Meteo/CAMS, bukan "
        "sensor lapangan. Kategori memakai ambang PM2.5 ISPU, bukan AQI resmi."
    )
    render_analytics()
with tab_ml:
    st.caption("Jalur ML (E4): model ridge per kota dan horizon +1 sampai +6 jam, dibandingkan baseline persistence dan forecast CAMS. Diperbarui tiap run DAG air_quality_ml (tiap jam).")
    render_ml()
with tab_settings:
    st.caption("Kontrol on/off tiap jalur pipeline. Perubahan berlaku pada run berikutnya.")
    render_settings()
