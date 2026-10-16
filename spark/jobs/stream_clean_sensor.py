"""Jalur streaming: bersihkan data sensor simulasi dan muat ke Data Warehouse.

Spark Structured Streaming memantau folder /opt/stream/incoming. Setiap
5 detik, file baru dibaca sebagai satu micro-batch lalu diproses fungsi
process_batch: simpan mentah, bersihkan, muat ke fakta, catat kualitas.
"""
from __future__ import annotations

import os

import psycopg2
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType, StructField, StructType

INCOMING_DIR = "/opt/stream/incoming"
CHECKPOINT_DIR = "/opt/stream/checkpoint"
DDL_FILE = "/opt/sql/03-streaming.sql"


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


DB = {
    "host": env("DW_HOST", "postgres"),
    "port": env("DW_PORT", "5432"),
    "dbname": env("DW_DB", "air_quality_dw"),
    "user": env("DW_USER", "platform"),
    "password": env("DW_PASSWORD", "platform123"),
}
JDBC_URL = f"jdbc:postgresql://{DB['host']}:{DB['port']}/{DB['dbname']}"
JDBC_OPTIONS = {"user": DB["user"], "password": DB["password"], "driver": "org.postgresql.Driver"}

POLLUTANTS = [
    "pm2_5",
    "pm10",
    "carbon_monoxide",
    "nitrogen_dioxide",
    "sulphur_dioxide",
    "ozone",
]

# Batas atas yang masih masuk akal (µg/m³). Di atas ini dianggap sensor error.
UPPER_LIMIT = {
    "pm2_5": 1000,
    "pm10": 1500,
    "carbon_monoxide": 50000,
    "nitrogen_dioxide": 2000,
    "sulphur_dioxide": 2000,
    "ozone": 1000,
}

# Semua variasi nama kota (huruf kecil, tanpa spasi di ujung) ke nama baku.
CITY_ALIASES = {
    "semarang": "Semarang", "kota semarang": "Semarang", "smg": "Semarang",
    "jakarta": "Jakarta", "dki jakarta": "Jakarta", "jkt": "Jakarta",
    "bandung": "Bandung", "kota bandung": "Bandung", "bdg": "Bandung",
    "surabaya": "Surabaya", "kota surabaya": "Surabaya", "sby": "Surabaya",
    "yogyakarta": "Yogyakarta", "kota yogyakarta": "Yogyakarta",
    "jogja": "Yogyakarta", "yogya": "Yogyakarta",
}

# Koordinat baku, sama dengan CITIES di DAG jalur batch.
COORDINATES = {
    "Semarang": (-6.9667, 110.4167),
    "Jakarta": (-6.2088, 106.8456),
    "Bandung": (-6.9175, 107.6191),
    "Surabaya": (-7.2575, 112.7521),
    "Yogyakarta": (-7.7956, 110.3695),
}

# Pesan kotor bisa berisi apa saja, jadi semua kolom dibaca sebagai teks dulu.
SCHEMA = StructType(
    [StructField(name, StringType()) for name in ["reading_id", "city", "reading_at", *POLLUTANTS, "dirty_type"]]
)

MERGE_SQL = """
    INSERT INTO warehouse.dim_location (city, latitude, longitude)
    SELECT DISTINCT city, latitude, longitude FROM staging.sensor_clean_batch
    ON CONFLICT (city) DO NOTHING;

    INSERT INTO warehouse.dim_time (
        time_key, valid_at, full_date, year, month, day, hour, day_of_week
    )
    SELECT DISTINCT
        TO_CHAR(h, 'YYYYMMDDHH24')::BIGINT, h, h::DATE,
        EXTRACT(YEAR FROM h)::SMALLINT, EXTRACT(MONTH FROM h)::SMALLINT,
        EXTRACT(DAY FROM h)::SMALLINT, EXTRACT(HOUR FROM h)::SMALLINT,
        TRIM(TO_CHAR(h, 'Day'))
    FROM (SELECT DATE_TRUNC('hour', reading_at::TIMESTAMP) AS h FROM staging.sensor_clean_batch) jam
    ON CONFLICT (time_key) DO NOTHING;
"""

FACT_SQL = """
    INSERT INTO warehouse.fact_sensor_live (
        reading_id, location_key, time_key, reading_at,
        pm2_5, pm10, carbon_monoxide, nitrogen_dioxide, sulphur_dioxide, ozone, batch_id
    )
    SELECT
        c.reading_id,
        l.location_key,
        TO_CHAR(DATE_TRUNC('hour', c.reading_at::TIMESTAMP), 'YYYYMMDDHH24')::BIGINT,
        c.reading_at::TIMESTAMP,
        c.pm2_5, c.pm10, c.carbon_monoxide, c.nitrogen_dioxide, c.sulphur_dioxide, c.ozone,
        %s
    FROM staging.sensor_clean_batch c
    JOIN warehouse.dim_location l ON l.city = c.city
    ON CONFLICT (reading_id) DO NOTHING;
"""


def run_ddl() -> None:
    """Buat tabel jalur streaming kalau belum ada. Baris perintah psql (diawali \\) dilewati."""
    with open(DDL_FILE, encoding="utf-8") as handle:
        sql = "\n".join(line for line in handle if not line.lstrip().startswith("\\"))
    with psycopg2.connect(**DB) as conn, conn.cursor() as cur:
        cur.execute(sql)
    print("[STREAM] Tabel jalur streaming siap", flush=True)


def process_batch(batch, batch_id: int) -> None:
    if batch.isEmpty():
        return
    batch = batch.cache()

    # 1. Simpan semua pesan apa adanya ke staging (bukti asli, termasuk yang kotor).
    (
        batch.withColumn("batch_id", F.lit(batch_id))
        .write.mode("append")
        .jdbc(JDBC_URL, "staging.sensor_raw", properties=JDBC_OPTIONS)
    )

    # 2. Nama kota: rapikan spasi dan huruf, lalu cocokkan ke nama baku.
    alias_map = F.create_map(*[F.lit(x) for pair in CITY_ALIASES.items() for x in pair])
    df = batch.withColumn("city_clean", alias_map[F.lower(F.trim(F.col("city")))])

    # 3. Waktu: harus terbaca, tidak di masa depan (> 5 menit), tidak terlambat (> 60 menit).
    #    Jam pesan adalah jam WIB, jadi dibandingkan dengan "sekarang" versi WIB.
    now_wib = F.from_utc_timestamp(F.current_timestamp(), "Asia/Jakarta")
    ts = F.try_to_timestamp(F.col("reading_at"), F.lit("yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("ts", ts).withColumn(
        "time_ok",
        F.col("ts").isNotNull()
        & (F.col("ts") <= now_wib + F.expr("INTERVAL 5 MINUTES"))
        & (F.col("ts") >= now_wib - F.expr("INTERVAL 60 MINUTES")),
    )

    # 4. Angka: koma jadi titik, teks yang bukan angka jadi NULL,
    #    nilai negatif dan nilai di atas batas wajar juga jadi NULL.
    for name in POLLUTANTS:
        text = F.trim(F.col(name))
        number = F.regexp_replace(text, ",", ".").try_cast("double")
        df = (
            df.withColumn(f"{name}_fixed", text.contains(",") & number.isNotNull())
            .withColumn(f"{name}_unparseable", (text != "") & text.isNotNull() & number.isNull())
            .withColumn(f"{name}_negative", number < 0)
            .withColumn(f"{name}_outlier", number > UPPER_LIMIT[name])
            .withColumn(
                f"{name}_clean",
                F.when((number < 0) | (number > UPPER_LIMIT[name]), None).otherwise(number),
            )
        )
    all_empty = F.lit(True)
    for name in POLLUTANTS:
        all_empty = all_empty & F.col(f"{name}_clean").isNull()
    df = df.withColumn("all_empty", all_empty)

    # 5. Hitung dampak tiap aturan untuk log kualitas.
    def count_true(expr):
        return F.sum(F.when(expr, 1).otherwise(0))

    def any_of(suffix):
        cond = F.lit(False)
        for name in POLLUTANTS:
            cond = cond | F.coalesce(F.col(f"{name}_{suffix}"), F.lit(False))
        return cond

    def total_of(suffix):
        total = F.lit(0)
        for name in POLLUTANTS:
            total = total + F.when(F.coalesce(F.col(f"{name}_{suffix}"), F.lit(False)), 1).otherwise(0)
        return F.sum(total)

    stats = df.agg(
        F.count(F.lit(1)).alias("rows_in"),
        count_true(F.col("city_clean").isNull()).alias("rejected_city"),
        count_true(F.col("city_clean").isNotNull() & ~F.col("time_ok")).alias("rejected_time"),
        count_true(F.col("city_clean").isNotNull() & F.col("time_ok") & F.col("all_empty")).alias("rejected_empty"),
        count_true(F.col("city_clean").isNotNull() & (F.col("city") != F.col("city_clean"))).alias("normalized_city"),
        count_true(any_of("fixed")).alias("fixed_format"),
        total_of("unparseable").alias("nulled_unparseable"),
        total_of("negative").alias("nulled_negative"),
        total_of("outlier").alias("nulled_outlier"),
    ).first().asDict()

    # 6. Buang baris yang ditolak, lalu buang duplikat di dalam batch ini.
    accepted = df.filter(F.col("city_clean").isNotNull() & F.col("time_ok") & ~F.col("all_empty"))
    before_dedup = accepted.count()
    accepted = accepted.dropDuplicates(["reading_id"])
    duplicates_in_batch = before_dedup - accepted.count()

    lat_map = F.create_map(*[F.lit(x) for city, (lat, _) in COORDINATES.items() for x in (city, lat)])
    lon_map = F.create_map(*[F.lit(x) for city, (_, lon) in COORDINATES.items() for x in (city, lon)])
    clean = accepted.select(
        "reading_id",
        F.col("city_clean").alias("city"),
        lat_map[F.col("city_clean")].alias("latitude"),
        lon_map[F.col("city_clean")].alias("longitude"),
        F.date_format("ts", "yyyy-MM-dd HH:mm:ss").alias("reading_at"),
        *[F.col(f"{name}_clean").alias(name) for name in POLLUTANTS],
    )

    # 7. Tulis hasil bersih ke staging, lalu muat ke dimensi dan fakta dengan SQL.
    (
        clean.write.mode("overwrite")
        .option("truncate", "true")
        .jdbc(JDBC_URL, "staging.sensor_clean_batch", properties=JDBC_OPTIONS)
    )
    with psycopg2.connect(**DB) as conn, conn.cursor() as cur:
        cur.execute(MERGE_SQL)
        cur.execute(FACT_SQL, (batch_id,))
        loaded = cur.rowcount
        # Duplikat yang sudah masuk di batch sebelumnya ditolak primary key fakta.
        duplicates = duplicates_in_batch + (before_dedup - duplicates_in_batch - loaded)
        cur.execute(
            """
            INSERT INTO staging.stream_quality_log (
                batch_id, rows_in, rows_loaded, rejected_city, rejected_time, rejected_empty,
                duplicates, normalized_city, fixed_format, nulled_unparseable,
                nulled_negative, nulled_outlier
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (batch_id) DO NOTHING
            """,
            (
                batch_id, stats["rows_in"], loaded, stats["rejected_city"], stats["rejected_time"],
                stats["rejected_empty"], duplicates, stats["normalized_city"], stats["fixed_format"],
                stats["nulled_unparseable"], stats["nulled_negative"], stats["nulled_outlier"],
            ),
        )

    print(
        f"[STREAM] batch {batch_id}: masuk {stats['rows_in']}, dimuat {loaded}, "
        f"ditolak kota {stats['rejected_city']} / waktu {stats['rejected_time']} / kosong {stats['rejected_empty']}, "
        f"duplikat {duplicates}",
        flush=True,
    )
    batch.unpersist()


def main() -> None:
    run_ddl()
    spark = SparkSession.builder.appName("AirQualitySensorStream").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    stream = (
        spark.readStream.schema(SCHEMA)
        .option("maxFilesPerTrigger", 100)
        .option("cleanSource", "delete")
        .json(INCOMING_DIR)
        .withColumn("source_file", F.input_file_name())
    )
    query = (
        stream.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="5 seconds")
        .start()
    )
    print("[STREAM] Menunggu file baru di " + INCOMING_DIR, flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()
