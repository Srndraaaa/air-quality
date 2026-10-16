from __future__ import annotations

import os
from pyspark.sql import SparkSession, functions as F


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


spark = (
    SparkSession.builder
    .appName("AirQualityCleanTransform")
    .getOrCreate()
)

jdbc_url = (
    f"jdbc:postgresql://{env('DW_HOST', 'postgres')}:"
    f"{env('DW_PORT', '5432')}/{env('DW_DB', 'air_quality_dw')}"
)

jdbc_options = {
    "user": env("DW_USER", "platform"),
    "password": env("DW_PASSWORD", "platform123"),
    "driver": "org.postgresql.Driver",
}

print("[SPARK] Reading staging.air_quality_raw ...")
raw = spark.read.jdbc(
    url=jdbc_url,
    table="staging.air_quality_raw",
    properties=jdbc_options,
)

print(f"[SPARK] Raw rows: {raw.count()}")

pollutants = [
    "pm2_5",
    "pm10",
    "carbon_monoxide",
    "nitrogen_dioxide",
    "sulphur_dioxide",
    "ozone",
    "european_aqi",
]

clean = raw.filter(F.col("valid_at").isNotNull())

# Nilai konsentrasi/AQI negatif tidak masuk akal untuk pipeline ini.
for column_name in pollutants:
    clean = clean.withColumn(
        column_name,
        F.when(F.col(column_name) < 0, F.lit(None)).otherwise(F.col(column_name)),
    )

clean = (
    clean
    .dropDuplicates(["city", "valid_at"])
    .withColumn("processed_at", F.current_timestamp())
    .select(
        "city",
        "latitude",
        "longitude",
        "valid_at",
        *pollutants,
        "source_name",
        "ingested_at",
        "processed_at",
    )
)

clean_count = clean.count()
print(f"[SPARK] Clean rows: {clean_count}")

# Tabel clean adalah tabel kerja. Setiap run dibangun ulang dari raw staging.
(
    clean.write
    .mode("overwrite")
    .jdbc(
        url=jdbc_url,
        table="staging.air_quality_clean",
        properties=jdbc_options,
    )
)

print("[SPARK] staging.air_quality_clean written successfully")
spark.stop()
