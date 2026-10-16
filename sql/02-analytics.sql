-- 1. Pastikan data sudah sampai ke setiap layer.
SELECT 'raw' AS layer, COUNT(*) AS rows FROM staging.air_quality_raw
UNION ALL
SELECT 'clean', COUNT(*) FROM staging.air_quality_clean
UNION ALL
SELECT 'fact', COUNT(*) FROM warehouse.fact_air_quality;

-- 2. Rata-rata PM2.5 dan PM10 per kota.
SELECT
    l.city,
    ROUND(AVG(f.pm2_5)::numeric, 2) AS avg_pm2_5,
    ROUND(AVG(f.pm10)::numeric, 2) AS avg_pm10,
    ROUND(AVG(f.european_aqi)::numeric, 2) AS avg_european_aqi
FROM warehouse.fact_air_quality f
JOIN warehouse.dim_location l USING (location_key)
GROUP BY l.city
ORDER BY avg_pm2_5 DESC NULLS LAST;

-- 3. Tren PM2.5 per kota dan tanggal.
SELECT
    l.city,
    t.full_date,
    ROUND(AVG(f.pm2_5)::numeric, 2) AS avg_pm2_5
FROM warehouse.fact_air_quality f
JOIN warehouse.dim_location l USING (location_key)
JOIN warehouse.dim_time t USING (time_key)
GROUP BY l.city, t.full_date
ORDER BY t.full_date, l.city;

-- 4. Jam dengan PM2.5 tertinggi.
SELECT
    l.city,
    t.valid_at,
    f.pm2_5,
    f.pm10,
    f.european_aqi
FROM warehouse.fact_air_quality f
JOIN warehouse.dim_location l USING (location_key)
JOIN warehouse.dim_time t USING (time_key)
WHERE f.pm2_5 IS NOT NULL
ORDER BY f.pm2_5 DESC
LIMIT 10;
