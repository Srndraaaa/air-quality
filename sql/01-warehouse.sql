\connect air_quality_dw;

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;

-- Landing/staging: data dari API disimpan hampir apa adanya.
CREATE TABLE IF NOT EXISTS staging.air_quality_raw (
    source_id BIGSERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    valid_at TIMESTAMP NOT NULL,
    pm2_5 DOUBLE PRECISION,
    pm10 DOUBLE PRECISION,
    carbon_monoxide DOUBLE PRECISION,
    nitrogen_dioxide DOUBLE PRECISION,
    sulphur_dioxide DOUBLE PRECISION,
    ozone DOUBLE PRECISION,
    european_aqi DOUBLE PRECISION,
    -- Jumlah percobaan backfill untuk jam yang pm2_5/pm10-nya masih NULL.
    backfill_attempts SMALLINT NOT NULL DEFAULT 0,
    last_backfill_at TIMESTAMPTZ,
    source_name VARCHAR(50) NOT NULL DEFAULT 'Open-Meteo',
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (city, valid_at)
);

-- Log kualitas per run batch: metrik per kota, bukan hanya status pass/fail.
CREATE TABLE IF NOT EXISTS staging.batch_quality_log (
    run_id VARCHAR(200) NOT NULL,
    city VARCHAR(100) NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    hours_present INTEGER NOT NULL,
    hours_usable INTEGER NOT NULL,
    hours_all_null INTEGER NOT NULL,
    hours_partial INTEGER NOT NULL,
    hours_missing INTEGER NOT NULL,
    known_unavailable INTEGER NOT NULL,
    -- Selalu dihitung dari 30 hari terakhir, bukan dari window_start.
    usable_30d INTEGER NOT NULL,
    status VARCHAR(10) NOT NULL,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, city)
);

-- Dimension: satu baris untuk satu kota/lokasi.
CREATE TABLE IF NOT EXISTS warehouse.dim_location (
    location_key BIGSERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL UNIQUE,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Dimension: satu baris untuk satu jam.
CREATE TABLE IF NOT EXISTS warehouse.dim_time (
    time_key BIGINT PRIMARY KEY,
    valid_at TIMESTAMP NOT NULL UNIQUE,
    full_date DATE NOT NULL,
    year SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    day SMALLINT NOT NULL,
    hour SMALLINT NOT NULL,
    day_of_week VARCHAR(12) NOT NULL
);

-- Fact: nilai kualitas udara per lokasi per jam.
CREATE TABLE IF NOT EXISTS warehouse.fact_air_quality (
    location_key BIGINT NOT NULL REFERENCES warehouse.dim_location(location_key),
    time_key BIGINT NOT NULL REFERENCES warehouse.dim_time(time_key),
    pm2_5 DOUBLE PRECISION,
    pm10 DOUBLE PRECISION,
    carbon_monoxide DOUBLE PRECISION,
    nitrogen_dioxide DOUBLE PRECISION,
    sulphur_dioxide DOUBLE PRECISION,
    ozone DOUBLE PRECISION,
    european_aqi DOUBLE PRECISION,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (location_key, time_key)
);

CREATE INDEX IF NOT EXISTS idx_fact_air_quality_time
    ON warehouse.fact_air_quality(time_key);
CREATE INDEX IF NOT EXISTS idx_fact_air_quality_location
    ON warehouse.fact_air_quality(location_key);
