-- Jalur streaming: tabel untuk data sensor simulasi.
-- File ini dijalankan PostgreSQL saat volume baru dibuat, dan juga oleh job
-- spark-streaming setiap kali menyala (semua perintah aman diulang).
\connect air_quality_dw;

-- Staging: setiap pesan sensor disimpan apa adanya, termasuk yang kotor.
-- Semua kolom nilai bertipe TEXT karena pesan kotor bisa berisi "38,5" atau "N/A".
CREATE TABLE IF NOT EXISTS staging.sensor_raw (
    raw_id BIGSERIAL PRIMARY KEY,
    reading_id TEXT,
    city TEXT,
    reading_at TEXT,
    pm2_5 TEXT,
    pm10 TEXT,
    carbon_monoxide TEXT,
    nitrogen_dioxide TEXT,
    sulphur_dioxide TEXT,
    ozone TEXT,
    dirty_type TEXT,
    source_file TEXT,
    batch_id BIGINT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Staging: hasil bersih satu micro-batch, dikosongkan ulang setiap batch.
CREATE TABLE IF NOT EXISTS staging.sensor_clean_batch (
    reading_id TEXT,
    city TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    reading_at TEXT,
    pm2_5 DOUBLE PRECISION,
    pm10 DOUBLE PRECISION,
    carbon_monoxide DOUBLE PRECISION,
    nitrogen_dioxide DOUBLE PRECISION,
    sulphur_dioxide DOUBLE PRECISION,
    ozone DOUBLE PRECISION
);

-- Catatan kualitas: satu baris per micro-batch, berisi hitungan tiap aturan pembersihan.
CREATE TABLE IF NOT EXISTS staging.stream_quality_log (
    batch_id BIGINT PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    rows_in INT NOT NULL,
    rows_loaded INT NOT NULL,
    rejected_city INT NOT NULL,
    rejected_time INT NOT NULL,
    rejected_empty INT NOT NULL,
    duplicates INT NOT NULL,
    normalized_city INT NOT NULL,
    fixed_format INT NOT NULL,
    nulled_unparseable INT NOT NULL,
    nulled_negative INT NOT NULL,
    nulled_outlier INT NOT NULL
);

-- Fact: bacaan sensor yang sudah bersih. Memakai dimensi yang sama dengan jalur batch.
CREATE TABLE IF NOT EXISTS warehouse.fact_sensor_live (
    reading_id TEXT PRIMARY KEY,
    location_key BIGINT NOT NULL REFERENCES warehouse.dim_location(location_key),
    time_key BIGINT NOT NULL REFERENCES warehouse.dim_time(time_key),
    reading_at TIMESTAMP NOT NULL,
    pm2_5 DOUBLE PRECISION,
    pm10 DOUBLE PRECISION,
    carbon_monoxide DOUBLE PRECISION,
    nitrogen_dioxide DOUBLE PRECISION,
    sulphur_dioxide DOUBLE PRECISION,
    ozone DOUBLE PRECISION,
    batch_id BIGINT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_sensor_live_reading_at
    ON warehouse.fact_sensor_live(reading_at);
CREATE INDEX IF NOT EXISTS idx_sensor_raw_received_at
    ON staging.sensor_raw(received_at);
