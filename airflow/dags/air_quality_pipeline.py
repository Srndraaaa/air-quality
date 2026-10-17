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
#    - dw_connection() : membuka koneksi ke PostgreSQL.
#    - safe_value()    : mengambil nilai array API dengan aman.
#
# 4. Definisi DAG dan task
#    - extract_api_to_raw()     : Extract + raw staging.
#    - spark_clean_transform    : Transform dengan Spark.
#    - load_data_warehouse()    : Load ke dimension/fact.
#    - quality_check()          : Validasi hasil pipeline.
#
# 5. Dependency DAG
#    extracted >> spark_clean_transform >> warehouse_loaded >> dq
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
    # 5A. TASK 1 — EXTRACT API -> RAW STAGING
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

                    # Meminta variabel kualitas udara per jam.
                    "hourly": HOURLY_VARIABLES,

                    # Meminta waktu hasil API menggunakan zona Asia/Makassar (WITA).
                    "timezone": "Asia/Makassar",

                    # Sertakan riwayat 30 hari ke belakang supaya model ML (E4)
                    # punya cukup data training, dan tetap aman di-upsert ulang.
                    "past_days": 30,

                    # Sertakan forecast satu hari ke depan.
                    "forecast_days": 1,
                },

                # Jika server tidak merespons dalam 30 detik,
                # requests akan menghentikan request dan menganggapnya gagal.
                timeout=30,
            )

            # Jika status HTTP adalah error, misalnya 4xx atau 5xx,
            # baris ini akan melempar exception.
            # Exception tersebut membuat task Airflow gagal dan mekanisme
            # retry dapat bekerja.
            response.raise_for_status()

            # response.json() mengubah body JSON menjadi object Python
            # (biasanya dictionary).
            payload = response.json()

            # Dari seluruh response API, kita hanya mengambil bagian "hourly".
            hourly = payload["hourly"]

            # hourly["time"] berisi daftar timestamp per jam.
            #
            # enumerate(...) memberi dua nilai sekaligus:
            # - i        = nomor indeks: 0, 1, 2, ...
            # - valid_at = timestamp pada indeks tersebut.
            #
            # Indeks i dipakai agar PM2.5, PM10, CO, NO2, SO2, O3, dan AQI
            # diambil pada jam yang sama.
            for i, valid_at in enumerate(hourly["time"]):

                # Tambahkan satu record ke list rows.
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

        # -------------------------------------------------------------------
        # SQL UPSERT KE RAW STAGING
        # -------------------------------------------------------------------

        # Triple quote """...""" memungkinkan SQL ditulis multi-baris.
        #
        # Istilah UPSERT = INSERT + UPDATE.
        # Jika data baru -> INSERT.
        # Jika city + valid_at sudah ada -> UPDATE.
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

        # Cara membaca SQL di atas:
        #
        # INSERT INTO staging.air_quality_raw (...)
        #   -> masukkan data ke tabel raw staging.
        #
        # VALUES %s
        #   -> placeholder untuk sekumpulan rows yang nanti diisi
        #      oleh execute_values().
        #
        # ON CONFLICT (city, valid_at)
        #   -> anggap kombinasi city + valid_at sebagai identitas data.
        #
        # DO UPDATE SET ...
        #   -> jika kombinasi itu sudah ada, perbarui nilainya.
        #
        # EXCLUDED.pm2_5 dan seterusnya
        #   -> "nilai baru yang tadinya hendak di-INSERT".
        #
        # ingested_at = NOW()
        #   -> catat waktu ketika record diperbarui.

        # "with" dipakai agar koneksi otomatis ditutup setelah blok selesai.
        with dw_connection() as conn:

            # Membuat cursor SQL.
            # Cursor adalah object untuk menjalankan perintah SQL.
            with conn.cursor() as cur:

                # execute_values mengirim banyak record sekaligus.
                #
                # cur        = cursor PostgreSQL.
                # upsert_sql = SQL yang sudah dibuat.
                # rows       = seluruh tuple hasil API.
                # page_size=500 = kirim maksimal 500 record per batch.
                execute_values(cur, upsert_sql, rows, page_size=500)

        # Log ini muncul pada log task Airflow.
        print(f"[AIRFLOW] Extracted/upserted {len(rows)} rows")

        # Mengembalikan jumlah record.
        # Karena task memakai @task, nilai ini juga dapat disimpan Airflow
        # melalui mekanisme XCom.
        return len(rows)

    # -----------------------------------------------------------------------
    # 5B. TASK 2 — TRANSFORMASI DENGAN APACHE SPARK
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
    # 5C. TASK 3 — LOAD CLEAN STAGING -> DATA WAREHOUSE
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
    # 5D. TASK 4 — DATA QUALITY CHECK
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

        # Jika semua pemeriksaan lolos, susun ringkasan hasil quality check.
        result = {
            "raw_rows": raw_rows,
            "clean_rows": clean_rows,
            "fact_rows": fact_rows,
            "invalid_rows": invalid_rows,
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
    # 1. extract_api_to_raw harus sukses lebih dahulu.
    # 2. setelah itu Spark membersihkan/mentransformasi data.
    # 3. setelah Spark sukses, data dimuat ke data warehouse.
    # 4. terakhir, quality_check memvalidasi hasil.
    #
    # Jika task di tengah gagal, task setelahnya tidak akan berjalan
    # dengan trigger rule default Airflow.
    extracted >> spark_clean_transform >> warehouse_loaded >> dq


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
#      | HTTP GET oleh requests
#      v
# extract_api_to_raw()
#      |
#      | UPSERT
#      v
# staging.air_quality_raw
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
# "Setiap tiga jam Airflow memanggil Open-Meteo untuk empat kota.
# Data per jam dikumpulkan dan di-upsert ke raw staging.
# Setelah extract selesai, Airflow menjalankan Spark untuk membersihkan data.
# Hasil clean staging kemudian dimuat ke tabel dimensi dan fact PostgreSQL.
# Terakhir Airflow memeriksa bahwa tabel tidak kosong dan nilai utama
# tidak mengandung angka negatif."
