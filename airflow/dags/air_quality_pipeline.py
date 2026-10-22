# AIR QUALITY ANALYTICS PLATFORM
# DAG Airflow: Open-Meteo -> Raw Staging -> Spark -> Data Warehouse -> Data Quality
#
# CARA MEMBACA FILE INI
# =====================
# Baca kode dari atas ke bawah dalam lima lapisan:
#
# 1. Import library
#    Menyiapkan fungsi Python, koneksi PostgreSQL, HTTP request, Airflow, dan Spark.
#
# 2. Konfigurasi global
#    Menentukan API yang dipakai, kota yang diambil, dan variabel kualitas udara.
#
# 3. Helper function
#    - dw_connection()      : membuka koneksi ke PostgreSQL.
#    - safe_value()         : mengambil nilai array API dengan aman.
#    - fetch_hourly_rows()  : ambil deret jam dari Open-Meteo.
#    - upsert_raw_rows()    : tulis ke raw staging (idempotent).
#
# 4. Definisi DAG dan task
#    - backfill_historical()  : isi jam bolong dengan data historis.
#    - extract_api_to_raw()   : Extract + raw staging.
#    - spark_clean_transform  : Transform dengan Spark.
#    - load_data_warehouse()  : Load ke dimension/fact.
#    - quality_check()        : Validasi hasil pipeline.
#
# 5. Dependency DAG
#    backfilled >> extracted >> spark_clean_transform >> warehouse_loaded >> dq
#
#    Tanda >> dibaca "setelah task di sebelah kiri selesai,
#    jalankan task di sebelah kanan".

# ---------------------------------------------------------------------------
# 1. IMPORT LIBRARY
# ---------------------------------------------------------------------------

# Meminta Python menggunakan perilaku annotation/type hint yang lebih modern.
# Pada file ini berguna karena ada type hint seperti -> int, -> dict,
# dan parameter seperti hourly: dict.
from __future__ import annotations

# os dipakai untuk membaca environment variable, misalnya DW_HOST dan DW_USER.
# Environment variable membuat konfigurasi database tidak harus ditulis
# secara permanen di kode.
import os

# datetime dipakai untuk menentukan start_date DAG.
# timedelta dipakai untuk menentukan jeda retry ketika task gagal.
from datetime import datetime, timedelta

# psycopg2 adalah driver PostgreSQL untuk Python.
# Library ini dipakai saat Airflow perlu membaca/menulis data ke PostgreSQL.
import psycopg2

# requests dipakai untuk melakukan HTTP request ke Open-Meteo Air Quality API.
import requests

# execute_values mempercepat INSERT banyak baris sekaligus ke PostgreSQL.
# Jadi data tidak dimasukkan satu per satu dengan INSERT terpisah.
from psycopg2.extras import execute_values

# dag dan task adalah decorator Airflow.
# @dag mengubah fungsi Python menjadi DAG.
# @task mengubah fungsi Python menjadi task Airflow.
from airflow.sdk import dag, task

# SparkSubmitOperator dipakai Airflow untuk menjalankan file Python Spark.
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# batch_lib berisi helper murni jalur batch: konvensi jam WITA dan pencarian
# jam bolong. Modul itu sengaja tidak mengimpor Airflow supaya bisa diuji
# tanpa stack (pola yang sama dengan ml_lib.py, design.md D-015).
from batch_lib import count_missing_hours, hour_floor_wita, missing_windows


# ---------------------------------------------------------------------------
# 2. KONFIGURASI SUMBER DATA
# ---------------------------------------------------------------------------

# URL endpoint Open-Meteo Air Quality API.
# Nanti requests.get() akan mengirim permintaan HTTP GET ke alamat ini.
API_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"


# Daftar kota yang akan diproses.
# Setiap elemen berupa dictionary berisi nama kota, latitude, dan longitude.
# Koordinat dipakai sebagai parameter ketika memanggil API.
CITIES = [
    {"city": "Palu", "latitude": -0.9083, "longitude": 119.8708},
    {"city": "Mamuju", "latitude": -2.6806, "longitude": 118.8861},
    {"city": "Gorontalo", "latitude": 0.5375, "longitude": 123.0625},
    {"city": "Manado", "latitude": 1.4822, "longitude": 124.8489},
]


# String ini berisi nama variabel hourly yang diminta dari Open-Meteo.
#
# Python otomatis menggabungkan dua string yang berdampingan di dalam
# tanda kurung, sehingga hasil akhirnya menjadi satu string panjang:
#
# pm2_5,pm10,carbon_monoxide,nitrogen_dioxide,
# sulphur_dioxide,ozone,european_aqi
#
# Parameter ini nanti dikirim sebagai:
#   "hourly": HOURLY_VARIABLES
HOURLY_VARIABLES = (
    "pm2_5,pm10,carbon_monoxide,nitrogen_dioxide,"
    "sulphur_dioxide,ozone,european_aqi"
)


# Batas ke belakang pipeline boleh menarik data historis, dalam hari.
#
# Angka 92 adalah nilai maksimum parameter past_days Open-Meteo, jadi
# dengan nilai ini gap yang bisa ditutup tidak pernah lebih tua dari arsip
# CAMS global (data tersedia sejak sekitar September 2022).
#
# Nilai ini bisa diubah lewat environment variable BACKFILL_MAX_DAYS
# tanpa menyentuh kode.
BACKFILL_MAX_DAYS = int(os.getenv("BACKFILL_MAX_DAYS", "92"))


# ---------------------------------------------------------------------------
# 3. HELPER: KONEKSI KE DATA WAREHOUSE POSTGRESQL
# ---------------------------------------------------------------------------

def dw_connection():
    # Membuka koneksi ke PostgreSQL.
    #
    # Cara membaca:
    # os.getenv("DW_HOST", "postgres")
    # berarti: baca environment variable DW_HOST.
    # Jika tidak ada, gunakan nilai default "postgres".
    #
    # Pola yang sama berlaku untuk port, database, user, dan password.

    # psycopg2.connect(...) membuat satu koneksi baru ke PostgreSQL.
    return psycopg2.connect(
        # Host PostgreSQL.
        # Dalam Docker Compose, "postgres" biasanya adalah nama service.
        host=os.getenv("DW_HOST", "postgres"),

        # Port standar PostgreSQL adalah 5432.
        port=os.getenv("DW_PORT", "5432"),

        # Nama database yang akan digunakan.
        dbname=os.getenv("DW_DB", "air_quality_dw"),

        # Username PostgreSQL.
        user=os.getenv("DW_USER", "platform"),

        # Password PostgreSQL.
        # Untuk produksi sebaiknya password hanya berasal dari secret/env,
        # bukan menggunakan default password di source code.
        password=os.getenv("DW_PASSWORD", "platform123"),
    )


# ---------------------------------------------------------------------------
# 4. HELPER: MENGAMBIL NILAI API DENGAN AMAN
# ---------------------------------------------------------------------------

def safe_value(hourly: dict, name: str, index: int):
    # Mengambil elemen dari dictionary hourly secara aman.
    #
    # Contoh:
    # safe_value(hourly, "pm2_5", 3)
    #
    # Dibaca:
    # "Ambil array hourly['pm2_5'], lalu ambil elemen indeks ke-3.
    # Jika array tidak ada atau indeks di luar batas, kembalikan None."

    # hourly.get(name) mengambil nilai berdasarkan key.
    # Berbeda dengan hourly[name], .get() tidak langsung error jika key tidak ada.
    values = hourly.get(name)

    # Dua kondisi dicek:
    # 1. not values       -> data tidak ada / kosong.
    # 2. index >= len(...) -> indeks yang diminta melewati panjang array.
    if not values or index >= len(values):
        # None nantinya dapat diterjemahkan menjadi NULL saat masuk database.
        return None

    # Jika data tersedia dan indeks valid, kembalikan nilainya.
    return values[index]


# ---------------------------------------------------------------------------
# 4B. HELPER: AMBIL DERET JAM DARI API
# ---------------------------------------------------------------------------

def fetch_hourly_rows(location: dict, time_params: dict) -> list[tuple]:
    """Ambil deret jam dari Open-Meteo untuk satu kota, siap di-insert.

    `time_params` menentukan rentang waktu yang diminta:
    - {"past_days": 30, "forecast_days": 1} -> data terkini plus riwayat 30 hari.
    - {"start_hour": "...", "end_hour": "..."} -> window historis yang persis,
      dipakai task backfill_historical untuk hanya meminta jam yang bolong.

    Baris dikembalikan apa adanya. Kalau API menyisipkan jam tanpa nilai
    polutan, nilainya tetap NULL dan jam itu tetap dihitung sebagai jam yang
    terisi. Kalau baris seperti itu dibuang, pipeline akan meminta jam yang
    sama pada setiap run dan quality_check akan selalu gagal.
    """
    # requests.get(...) mengirim HTTP GET request ke Open-Meteo.
    response = requests.get(
        # Endpoint API.
        API_URL,

        # params akan diubah requests menjadi query string URL.
        params={
            # Latitude kota yang sedang diproses.
            "latitude": location["latitude"],

            # Longitude kota yang sedang diproses.
            "longitude": location["longitude"],

            # Variabel kualitas udara per jam.
            "hourly": HOURLY_VARIABLES,

            # Waktu hasil API memakai zona Asia/Makassar (WITA).
            "timezone": "Asia/Makassar",

            # Rentang waktu, ditentukan oleh pemanggil.
            **time_params,
        },

        # Jika server tidak merespons dalam 30 detik, request dianggap gagal.
        timeout=30,
    )

    # Status HTTP error (4xx/5xx) membuat task gagal sehingga retry berjalan.
    response.raise_for_status()

    # Ambil bagian "hourly" dari response.
    hourly = response.json()["hourly"]

    # Wadah sementara untuk semua baris kota ini.
    rows = []

    # Satu iterasi = satu jam. Indeks i dipakai agar semua polutan diambil
    # pada jam yang sama dengan timestamp-nya.
    for i, valid_at in enumerate(hourly["time"]):
        rows.append(
            (
                # Nama kota.
                location["city"],

                # Latitude kota.
                location["latitude"],

                # Longitude kota.
                location["longitude"],

                # Waktu data kualitas udara berlaku.
                valid_at,

                # Ambil PM2.5 pada indeks jam yang sama.
                safe_value(hourly, "pm2_5", i),

                # Ambil PM10.
                safe_value(hourly, "pm10", i),

                # Ambil carbon monoxide / CO.
                safe_value(hourly, "carbon_monoxide", i),

                # Ambil nitrogen dioxide / NO2.
                safe_value(hourly, "nitrogen_dioxide", i),

                # Ambil sulphur dioxide / SO2.
                safe_value(hourly, "sulphur_dioxide", i),

                # Ambil ozone / O3.
                safe_value(hourly, "ozone", i),

                # Ambil European AQI.
                safe_value(hourly, "european_aqi", i),
            )
        )

    return rows


# ---------------------------------------------------------------------------
# 4C. HELPER: UPSERT KE RAW STAGING
# ---------------------------------------------------------------------------

def upsert_raw_rows(rows: list[tuple]) -> int:
    """Upsert baris ke staging.air_quality_raw. Aman dijalankan ulang.

    Kombinasi city + valid_at adalah identitas data, jadi baris yang sama
    boleh ditulis berkali-kali tanpa menggandakan isi (design.md D-003).
    """
    # Tidak ada baris -> tidak perlu buka koneksi.
    if not rows:
        return 0

    upsert_sql = """
        INSERT INTO staging.air_quality_raw (
            city, latitude, longitude, valid_at,
            pm2_5, pm10, carbon_monoxide, nitrogen_dioxide,
            sulphur_dioxide, ozone, european_aqi
        ) VALUES %s

        ON CONFLICT (city, valid_at) DO UPDATE SET
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            pm2_5 = EXCLUDED.pm2_5,
            pm10 = EXCLUDED.pm10,
            carbon_monoxide = EXCLUDED.carbon_monoxide,
            nitrogen_dioxide = EXCLUDED.nitrogen_dioxide,
            sulphur_dioxide = EXCLUDED.sulphur_dioxide,
            ozone = EXCLUDED.ozone,
            european_aqi = EXCLUDED.european_aqi,
            ingested_at = NOW();
    """

    # "with" dipakai agar koneksi otomatis ditutup setelah blok selesai.
    with dw_connection() as conn:

        # Membuat cursor SQL.
        with conn.cursor() as cur:

            # execute_values mengirim banyak record sekaligus.
            execute_values(cur, upsert_sql, rows, page_size=500)

    return len(rows)


# ---------------------------------------------------------------------------
# 5. DEFINISI DAG AIRFLOW
# ---------------------------------------------------------------------------

# @dag(...) adalah decorator.
# Artinya fungsi air_quality_pipeline() di bawah tidak sekadar fungsi Python,
# tetapi didefinisikan sebagai sebuah DAG Airflow.
@dag(
    # ID unik DAG yang tampil di Airflow UI.
    dag_id="air_quality_end_to_end",

    # Deskripsi singkat alur pipeline.
    description="Open-Meteo -> Airflow -> Spark -> PostgreSQL Data Warehouse",

    # Cron expression: "0 */3 * * *"
    # Dibaca: jalankan pada menit ke-0 setiap 3 jam.
    # Contoh: 00:00, 03:00, 06:00, 09:00, dan seterusnya.
    schedule="0 */3 * * *",

    # Airflow mulai menganggap DAG berlaku sejak tanggal ini.
    start_date=datetime(2026, 1, 1),

    # catchup=False berarti Airflow tidak otomatis menjalankan semua jadwal
    # historis yang terlewat sejak start_date.
    catchup=False,

    # Satu run aktif saja: run bertumpuk (mis. catch-up) bisa saling menimpa
    # staging dan warehouse.
    max_active_runs=1,

    # Tags hanya untuk memudahkan pencarian/pengelompokan di Airflow UI.
    tags=["big-data", "air-quality", "spark", "postgresql"],
)
def air_quality_pipeline():
    # Fungsi utama yang mendefinisikan seluruh task dan urutan pipeline.
    #
    # Penting:
    # Isi fungsi ini membangun struktur DAG.
    # Bukan berarti semua task langsung dijalankan ketika Python membaca fungsi.
    # Airflow akan mengelola kapan task dijalankan berdasarkan dependency.

    # -----------------------------------------------------------------------
    # 5A. TASK 1 — BACKFILL HISTORIS (MENUTUPI JAM BOLONG)
    # -----------------------------------------------------------------------

    # Task ini berjalan lebih dulu, sebelum extract.
    #
    # Kalau di gudang ada jam-hour yang kosong — misalnya pipeline mati
    # beberapa hari, atau ada kota yang datanya baru mulai masuk — jam-jam itu
    # diambil dari arsip Open-Meteo memakai parameter start_hour/end_hour.
    # Berbeda dengan extract yang selalu meminta 30 hari penuh, di sini API
    # hanya mengembalikan jam-jam yang benar-benar bolong.
    #
    # retries=2 + execution_timeout:
    # task ini menyentuh jaringan, jadi wajib punya keduanya (AGENTS.md).
    @task(
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=15),
    )
    def backfill_historical() -> dict:
        """Isi jam-hour yang kosong di raw staging dengan data historis."""

        # Jam penuh sekarang menurut WITA, tanpa informasi zona waktu.
        # Kolom valid_at adalah TIMESTAMP naive WITA, jadi perbandingan
        # "sekarang" dengan kolom itu harus dilakukan di jam WITA juga
        # (design.md D-001).
        now_floor = hour_floor_wita()

        # Batas paling tua yang boleh dicari. Jam yang lebih tua dari ini
        # dianggap di luar jangkauan backfill.
        oldest = now_floor - timedelta(days=BACKFILL_MAX_DAYS)

        # Ringkasan per kota, dikembalikan sebagai XCom.
        summary = {}

        # Loop setiap kota pada CITIES.
        for location in CITIES:

            # Jam-jam yang sudah ada di raw staging untuk kota ini.
            with dw_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT valid_at
                        FROM staging.air_quality_raw
                        WHERE city = %s AND valid_at BETWEEN %s AND %s
                        ORDER BY valid_at
                        """,
                        (location["city"], oldest, now_floor),
                    )
                    existing = [row[0] for row in cur.fetchall()]

            # Jam yang kosong, dikelompokkan jadi window berurutan supaya
            # API hanya dipanggil sekali per lubuk.
            windows = missing_windows(existing, oldest, now_floor)

            # Kumpulkan baris dari semua window yang bolong.
            rows = []
            for gap_start, gap_end in windows:
                rows.extend(
                    fetch_hourly_rows(
                        location,
                        {
                            # Open-Meteo menerima waktu ISO8601 dan
                            # mengembalikan rentang itu inklusif di kedua
                            # ujungnya.
                            "start_hour": gap_start.strftime("%Y-%m-%dT%H:%M"),
                            "end_hour": gap_end.strftime("%Y-%m-%dT%H:%M"),
                        },
                    )
                )

            # Tulis ke raw staging. Idempotent, jadi aman saat task di-retry.
            upsert_raw_rows(rows)

            gap_hours = count_missing_hours(existing, oldest, now_floor)
            summary[location["city"]] = {
                "windows": len(windows),
                "gap_hours": gap_hours,
                "rows": len(rows),
            }

            print(
                f"[AIRFLOW] Backfill {location['city']}: "
                f"{len(windows)} window, {gap_hours} jam bolong, {len(rows)} baris"
            )

        return summary

    # -----------------------------------------------------------------------
    # 5B. TASK 2 — EXTRACT API -> RAW STAGING
    # -----------------------------------------------------------------------

    # @task menjadikan fungsi berikut sebagai Airflow task.
    #
    # retries=2:
    # jika task gagal, Airflow boleh mencoba ulang maksimum 2 kali.
    #
    # retry_delay=timedelta(minutes=2):
    # jeda antarpercobaan ulang adalah 2 menit.
    @task(retries=2, retry_delay=timedelta(minutes=2))
    def extract_api_to_raw() -> int:
        """Call Open-Meteo and upsert hourly air-quality rows into raw staging."""

        # Cara membaca fungsi ini:
        # mengambil data kualitas udara dari Open-Meteo,
        # lalu melakukan INSERT/UPDATE ke staging.air_quality_raw.
        #
        # Return:
        # int -> jumlah baris yang berhasil disiapkan untuk upsert.

        # rows menjadi wadah sementara seluruh data dari semua kota.
        # Bentuk akhirnya adalah list berisi banyak tuple.
        rows = []

        # Loop setiap kota pada CITIES.
        # Pada iterasi pertama location berisi Palu,
        # kemudian Mamuju, Gorontalo, dan Manado.
        for location in CITIES:

            # Ambil deret jam dari Open-Meteo untuk kota ini.
            #
            # Sertakan riwayat 30 hari ke belakang supaya model ML (E4)
            # punya cukup data training, dan tetap aman di-upsert ulang.
            # Sertakan forecast satu hari ke depan.
            rows.extend(
                fetch_hourly_rows(
                    location,
                    {"past_days": 30, "forecast_days": 1},
                )
            )

        # Tulis sekaligus ke raw staging.
        upsert_raw_rows(rows)

        # Log ini muncul pada log task Airflow.
        print(f"[AIRFLOW] Extracted/upserted {len(rows)} rows")

        # Mengembalikan jumlah record.
        # Karena task memakai @task, nilai ini juga dapat disimpan Airflow
        # melalui mekanisme XCom.
        return len(rows)

    # -----------------------------------------------------------------------
    # 5C. TASK 3 — TRANSFORMASI DENGAN APACHE SPARK
    # -----------------------------------------------------------------------

    # Berbeda dari task pertama yang berupa fungsi @task,
    # task ini menggunakan SparkSubmitOperator.
    #
    # Artinya Airflow akan memanggil spark-submit untuk menjalankan
    # file transform_air_quality.py.
    spark_clean_transform = SparkSubmitOperator(
        # ID task yang tampil pada DAG Airflow.
        task_id="spark_clean_transform",

        # Lokasi script Spark di dalam environment/container Airflow.
        application="/opt/airflow/spark_jobs/transform_air_quality.py",

        # Nama Airflow Connection untuk koneksi ke Spark.
        conn_id="spark_default",

        # JDBC driver PostgreSQL.
        # Diperlukan jika Spark membaca/menulis PostgreSQL melalui JDBC.
        jars="/usr/share/java/postgresql.jar",

        # Konfigurasi resource dan network Spark.
        conf={
            # Memory untuk Spark driver.
            "spark.driver.memory": "768m",

            # Memory untuk Spark executor.
            "spark.executor.memory": "768m",

            # Host driver Spark.
            # Nama "airflow-scheduler" menunjukkan komunikasi antarkontainer
            # menggunakan nama service/container pada network Docker.
            "spark.driver.host": "airflow-scheduler",

            # Driver menerima koneksi pada seluruh interface di container.
            "spark.driver.bindAddress": "0.0.0.0",
        },

        # Environment variable yang diberikan ke proses Spark.
        # Dengan ini script transform_air_quality.py mengetahui
        # lokasi dan kredensial database.
        env_vars={
            # Host database.
            "DW_HOST": "postgres",

            # Port PostgreSQL.
            "DW_PORT": "5432",

            # Nama database. Jika env DW_DB tidak ada, gunakan default.
            "DW_DB": os.getenv("DW_DB", "air_quality_dw"),

            # Username database.
            "DW_USER": os.getenv("DW_USER", "platform"),

            # Password database.
            "DW_PASSWORD": os.getenv("DW_PASSWORD", "platform123"),
        },

        # verbose=True membuat log spark-submit lebih detail.
        verbose=True,
    )

    # Secara konseptual task Spark ini berada di tengah pipeline:
    #
    # staging.air_quality_raw
    #          |
    #          v
    # transform_air_quality.py
    #          |
    #          v
    # staging.air_quality_clean
    #
    # Detail transformasinya sendiri tidak ada di file DAG ini,
    # tetapi berada pada:
    # /opt/airflow/spark_jobs/transform_air_quality.py

    # -----------------------------------------------------------------------
    # 5D. TASK 4 — LOAD CLEAN STAGING -> DATA WAREHOUSE
    # -----------------------------------------------------------------------

    # @task tanpa parameter tambahan berarti task Python biasa
    # dengan konfigurasi default Airflow.
    @task
    def load_data_warehouse() -> dict:
        """Load/update dimensions and fact table from Spark-cleaned staging data."""

        # Cara membaca fungsi ini:
        # mengisi:
        # 1. warehouse.dim_location
        # 2. warehouse.dim_time
        # 3. warehouse.fact_air_quality
        #
        # Sumbernya adalah staging.air_quality_clean,
        # yaitu hasil transformasi Spark.

        # Satu string SQL ini berisi tiga operasi INSERT.
        sql = """
        INSERT INTO warehouse.dim_location (city, latitude, longitude)
        SELECT DISTINCT city, latitude, longitude
        FROM staging.air_quality_clean
        ON CONFLICT (city) DO UPDATE SET
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            updated_at = NOW();


        INSERT INTO warehouse.dim_time (
            time_key, valid_at, full_date, year, month, day, hour, day_of_week
        )
        SELECT DISTINCT
            TO_CHAR(valid_at, 'YYYYMMDDHH24')::BIGINT AS time_key,
            valid_at,
            valid_at::DATE,
            EXTRACT(YEAR FROM valid_at)::SMALLINT,
            EXTRACT(MONTH FROM valid_at)::SMALLINT,
            EXTRACT(DAY FROM valid_at)::SMALLINT,
            EXTRACT(HOUR FROM valid_at)::SMALLINT,
            TRIM(TO_CHAR(valid_at, 'Day'))
        FROM staging.air_quality_clean
        ON CONFLICT (time_key) DO NOTHING;


        INSERT INTO warehouse.fact_air_quality (
            location_key, time_key,
            pm2_5, pm10, carbon_monoxide, nitrogen_dioxide,
            sulphur_dioxide, ozone, european_aqi
        )
        SELECT
            l.location_key,
            t.time_key,
            c.pm2_5,
            c.pm10,
            c.carbon_monoxide,
            c.nitrogen_dioxide,
            c.sulphur_dioxide,
            c.ozone,
            c.european_aqi
        FROM staging.air_quality_clean c
        JOIN warehouse.dim_location l
          ON l.city = c.city
        JOIN warehouse.dim_time t
          ON t.valid_at = c.valid_at
        ON CONFLICT (location_key, time_key) DO UPDATE SET
            pm2_5 = EXCLUDED.pm2_5,
            pm10 = EXCLUDED.pm10,
            carbon_monoxide = EXCLUDED.carbon_monoxide,
            nitrogen_dioxide = EXCLUDED.nitrogen_dioxide,
            sulphur_dioxide = EXCLUDED.sulphur_dioxide,
            ozone = EXCLUDED.ozone,
            european_aqi = EXCLUDED.european_aqi,
            loaded_at = NOW();
        """

        # -------------------------------------------------------------------
        # Cara membaca SQL pertama: warehouse.dim_location
        # -------------------------------------------------------------------
        #
        # INSERT INTO warehouse.dim_location (city, latitude, longitude)
        #   -> target adalah dimension lokasi.
        #
        # SELECT DISTINCT city, latitude, longitude
        # FROM staging.air_quality_clean
        #   -> ambil kombinasi lokasi unik dari clean staging.
        #
        # ON CONFLICT (city) DO UPDATE
        #   -> jika kota sudah ada, jangan membuat kota duplikat;
        #      cukup update koordinat dan updated_at.

        # -------------------------------------------------------------------
        # Cara membaca SQL kedua: warehouse.dim_time
        # -------------------------------------------------------------------
        #
        # time_key dibuat dari waktu dengan format:
        # YYYYMMDDHH24
        #
        # Contoh:
        # 2026-09-27 14:00
        # menjadi:
        # 2026092714
        #
        # valid_at::DATE mengambil hanya tanggal.
        #
        # EXTRACT(YEAR/MONTH/DAY/HOUR FROM valid_at)
        # memecah timestamp menjadi atribut waktu yang cocok untuk analitik.
        #
        # TRIM(TO_CHAR(valid_at, 'Day'))
        # menghasilkan nama hari dan menghapus spasi ekstra.
        #
        # ON CONFLICT (time_key) DO NOTHING
        # berarti dimension waktu yang sudah ada tidak perlu dimasukkan ulang.

        # -------------------------------------------------------------------
        # Cara membaca SQL ketiga: warehouse.fact_air_quality
        # -------------------------------------------------------------------
        #
        # Fact table tidak menyimpan city langsung sebagai key utama dimensi.
        # Ia memakai:
        # - location_key
        # - time_key
        #
        # JOIN warehouse.dim_location l ON l.city = c.city
        #   -> mencari surrogate key lokasi untuk setiap record clean.
        #
        # JOIN warehouse.dim_time t ON t.valid_at = c.valid_at
        #   -> mencari surrogate/time key untuk waktu record.
        #
        # Nilai pengukuran seperti pm2_5, pm10, CO, NO2, SO2, O3, dan AQI
        # disimpan sebagai measure pada fact table.
        #
        # ON CONFLICT (location_key, time_key) DO UPDATE
        #   -> satu lokasi pada satu waktu hanya memiliki satu record fact.
        #      Jika sudah ada, nilainya diperbarui.

        # Buka koneksi PostgreSQL.
        with dw_connection() as conn:

            # Buat cursor untuk menjalankan SQL.
            with conn.cursor() as cur:

                # Jalankan ketiga statement SQL di atas.
                cur.execute(sql)

                # Setelah load selesai, hitung total record fact table.
                cur.execute("SELECT COUNT(*) FROM warehouse.fact_air_quality")

                # fetchone() mengambil satu row hasil SELECT.
                # [0] mengambil kolom pertama, yaitu COUNT(*).
                fact_rows = cur.fetchone()[0]

        # Tulis jumlah fact rows ke log Airflow.
        print(f"[AIRFLOW] Warehouse fact rows: {fact_rows}")

        # Kembalikan dictionary.
        # Karena ini task Airflow, return value dapat disimpan sebagai XCom.
        return {"fact_rows": fact_rows}

    # -----------------------------------------------------------------------
    # 5E. TASK 5 — DATA QUALITY CHECK
    # -----------------------------------------------------------------------

    @task
    def quality_check() -> dict:
        """Fail the DAG if critical warehouse quality rules are violated."""

        # Cara membaca fungsi ini:
        # memeriksa apakah pipeline menghasilkan data yang masuk akal.
        #
        # Rule yang diperiksa:
        # 1. raw staging tidak boleh kosong.
        # 2. clean staging tidak boleh kosong.
        # 3. fact table tidak boleh kosong.
        # 4. PM2.5, PM10, dan European AQI tidak boleh negatif.
        # 5. tidak boleh ada jam bolong di fact table (lihat task backfill).
        #
        # Jika rule gagal, raise ValueError membuat task gagal.

        # Buka koneksi database.
        with dw_connection() as conn:

            # Buat cursor.
            with conn.cursor() as cur:

                # Hitung jumlah record raw staging.
                cur.execute("SELECT COUNT(*) FROM staging.air_quality_raw")

                # Ambil hasil COUNT(*).
                raw_rows = cur.fetchone()[0]

                # Hitung jumlah record clean staging.
                cur.execute("SELECT COUNT(*) FROM staging.air_quality_clean")

                # Ambil hasil COUNT(*).
                clean_rows = cur.fetchone()[0]

                # Hitung jumlah record fact table.
                cur.execute("SELECT COUNT(*) FROM warehouse.fact_air_quality")

                # Ambil hasil COUNT(*).
                fact_rows = cur.fetchone()[0]

                # Cari record yang memiliki nilai negatif pada indikator
                # yang secara logis tidak boleh negatif.
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM warehouse.fact_air_quality
                    WHERE pm2_5 < 0 OR pm10 < 0 OR european_aqi < 0
                    """
                )

                # Jumlah record invalid.
                invalid_rows = cur.fetchone()[0]

        # Jika raw table kosong, hentikan task sebagai failure.
        if raw_rows == 0:
            raise ValueError("DQ failed: staging.air_quality_raw is empty")

        # Jika hasil transformasi Spark kosong, hentikan task.
        if clean_rows == 0:
            raise ValueError("DQ failed: staging.air_quality_clean is empty")

        # Jika fact table kosong, hentikan task.
        if fact_rows == 0:
            raise ValueError("DQ failed: warehouse.fact_air_quality is empty")

        # Jika ada nilai negatif yang tidak valid, hentikan task.
        if invalid_rows > 0:
            raise ValueError(f"DQ failed: {invalid_rows} invalid negative rows")

        # -------------------------------------------------------------------
        # RULE 5 — TIDAK BOLEH ADA JAM BOLONG DI FACT TABLE
        # -------------------------------------------------------------------

        # Task backfill_historical sudah mencoba menutup setiap jam yang
        # kosong. Kalau masih ada jam bolong, berarti jendela itu tidak bisa
        # diisi dari arsip Open-Meteo, jadi pipeline harus gagal supaya
        # masalahnya terlihat, bukan diam-diam terbawa ke model ML.
        #
        # Yang dicek adalah fact table, bukan raw staging, karena yang
        # dipakai downstream (ML dan dashboard) adalah fact table.
        now_floor = hour_floor_wita()
        oldest = now_floor - timedelta(days=BACKFILL_MAX_DAYS)

        # Total jam bolong di semua kota.
        gap_hours = 0

        # Rincian per kota, dipakai di pesan error.
        gap_detail = []

        for location in CITIES:
            with dw_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT t.valid_at
                        FROM warehouse.fact_air_quality f
                        JOIN warehouse.dim_location l
                          ON l.location_key = f.location_key
                        JOIN warehouse.dim_time t
                          ON t.time_key = f.time_key
                        WHERE l.city = %s AND t.valid_at BETWEEN %s AND %s
                        ORDER BY t.valid_at
                        """,
                        (location["city"], oldest, now_floor),
                    )
                    existing = [row[0] for row in cur.fetchall()]

            missing = count_missing_hours(existing, oldest, now_floor)

            if missing > 0:
                gap_hours += missing
                gap_detail.append(f"{location['city']}={missing}")

        # Jika masih ada jam bolong, hentikan task.
        if gap_hours > 0:
            raise ValueError(
                f"DQ failed: {gap_hours} jam bolong di warehouse.fact_air_quality "
                f"({', '.join(gap_detail)})"
            )

        # Jika semua pemeriksaan lolos, susun ringkasan hasil quality check.
        result = {
            "raw_rows": raw_rows,
            "clean_rows": clean_rows,
            "fact_rows": fact_rows,
            "invalid_rows": invalid_rows,
            "gap_hours": gap_hours,
            "status": "PASS",
        }

        # Tampilkan hasil quality check di log Airflow.
        print(f"[AIRFLOW] Data quality: {result}")

        # Return dictionary hasil quality check.
        return result

    # -----------------------------------------------------------------------
    # 6. MEMBUAT INSTANCE TASK
    # -----------------------------------------------------------------------

    # Memanggil fungsi task tidak berarti menjalankannya saat file dibaca.
    # Dalam konteks Airflow TaskFlow, pemanggilan ini membuat node/task
    # pada graph DAG.

    # Membuat task backfill_historical.
    backfilled = backfill_historical()

    # Membuat task extract_api_to_raw.
    extracted = extract_api_to_raw()

    # Membuat task load_data_warehouse.
    warehouse_loaded = load_data_warehouse()

    # Membuat task quality_check.
    dq = quality_check()

    # -----------------------------------------------------------------------
    # 7. MENENTUKAN URUTAN / DEPENDENCY TASK
    # -----------------------------------------------------------------------

    # Operator >> dibaca "kemudian".
    #
    # backfilled
    #     >>
    # extracted
    #     >>
    # spark_clean_transform
    #     >>
    # warehouse_loaded
    #     >>
    # dq
    #
    # Artinya:
    #
    # 1. backfill_historical menutup jam bolong lebih dulu.
    # 2. extract_api_to_raw mengambil data terkini, harus sukses lebih dahulu.
    # 3. setelah itu Spark membersihkan/mentransformasi data.
    # 4. setelah Spark sukses, data dimuat ke data warehouse.
    # 5. terakhir, quality_check memvalidasi hasil.
    #
    # Jika task di tengah gagal, task setelahnya tidak akan berjalan
    # dengan trigger rule default Airflow.
    backfilled >> extracted >> spark_clean_transform >> warehouse_loaded >> dq


# ---------------------------------------------------------------------------
# 8. REGISTRASI DAG
# ---------------------------------------------------------------------------

# Baris ini memanggil fungsi yang sudah diberi decorator @dag.
# Hasilnya adalah objek DAG yang didaftarkan Airflow dari file ini.
air_quality_pipeline()


# ---------------------------------------------------------------------------
# RINGKASAN ALUR DATA
# ---------------------------------------------------------------------------
#
# Open-Meteo API
#      |
#      | HTTP GET (start_hour/end_hour per window bolong)
#      v
# backfill_historical()
#      |
#      | UPSERT
#      v
# staging.air_quality_raw <-----------.
#      |                             |
#      | HTTP GET (past_days=30)      | UPSERT
#      v                             |
# extract_api_to_raw() ---------------'
#      |
#      | dibaca dan ditransformasi oleh Spark
#      v
# spark_clean_transform
#      |
#      v
# staging.air_quality_clean
#      |
#      | SQL load_data_warehouse()
#      v
# +--------------------------+
# | warehouse.dim_location   |
# | warehouse.dim_time       |
# | warehouse.fact_air_quality|
# +--------------------------+
#      |
#      v
# quality_check()
#      |
#      +--> PASS  : DAG selesai
#      |
#      +--> FAIL  : task melempar ValueError
#
#
# URUTAN TASK DI AIRFLOW
# ----------------------
#
# backfill_historical
#        >>
# extract_api_to_raw
#        >>
# spark_clean_transform
#        >>
# load_data_warehouse
#        >>
# quality_check
#
#
# CARA MEMBACA PIPELINE SECARA SINGKAT
# ------------------------------------
#
# "Setiap tiga jam Airflow menutup dulu jam-jam yang bolong di gudang dengan
# menarik data historis Open-Meteo, lalu mengambil data terkini untuk empat
# kota. Data per jam dikumpulkan dan di-upsert ke raw staging.
# Setelah extract selesai, Airflow menjalankan Spark untuk membersihkan data.
# Hasil clean staging kemudian dimuat ke tabel dimensi dan fact PostgreSQL.
# Terakhir Airflow memeriksa bahwa tabel tidak kosong, nilai utama tidak
# mengandung angka negatif, dan tidak ada jam yang bolong di fact table."
