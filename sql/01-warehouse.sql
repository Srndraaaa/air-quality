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
    source_name VARCHAR(50) NOT NULL DEFAULT 'Open-Meteo',
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (city, valid_at)
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
