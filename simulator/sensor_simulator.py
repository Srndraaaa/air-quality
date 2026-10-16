"""Simulator sensor kualitas udara untuk jalur streaming.

Setiap SIM_INTERVAL_SECONDS detik, simulator menulis satu file JSON Lines berisi
satu bacaan per kota ke /opt/stream/incoming. Nilainya berpijak pada data asli
jalur batch (tabel fakta), ditambah fluktuasi kecil. Sebagian pesan sengaja
dibuat kotor supaya aturan pembersihan di Spark terlihat bekerja.
"""
from __future__ import annotations

import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import psycopg2

WIB = ZoneInfo("Asia/Jakarta")
INCOMING_DIR = "/opt/stream/incoming"
TMP_DIR = "/opt/stream/tmp"
INTERVAL = float(os.getenv("SIM_INTERVAL_SECONDS", "3"))
DIRTY_RATE = float(os.getenv("SIM_DIRTY_RATE", "0.25"))
BASELINE_REFRESH_SECONDS = 60

POLLUTANTS = [
    "pm2_5",
    "pm10",
    "carbon_monoxide",
    "nitrogen_dioxide",
    "sulphur_dioxide",
    "ozone",
]

# Nilai cadangan kalau tabel fakta jalur batch masih kosong.
FALLBACK = {
    "Semarang": [35, 38, 300, 8, 20, 120],
    "Jakarta": [60, 62, 450, 25, 15, 110],
    "Bandung": [55, 58, 400, 12, 10, 100],
    "Surabaya": [20, 23, 280, 10, 18, 115],
    "Yogyakarta": [30, 32, 290, 7, 9, 125],
}

# Variasi penulisan nama kota yang biasa muncul dari perangkat berbeda.
CITY_VARIANTS = {
    "Semarang": [" semarang ", "KOTA SEMARANG", "Smg"],
    "Jakarta": ["jakarta", "DKI Jakarta", "Jkt"],
    "Bandung": ["BANDUNG", "Kota Bandung", "Bdg"],
    "Surabaya": ["surabaya ", "Kota Surabaya", "Sby"],
    "Yogyakarta": ["Jogja", "yogya", "Kota Yogyakarta"],
}

# Jenis data kotor dan bobot kemunculannya.
DIRTY_TYPES = {
    "negatif": 3,
    "outlier": 2,
    "format_koma": 2,
    "teks_bukan_angka": 2,
    "field_hilang": 2,
    "nama_kota_beda": 3,
    "kota_tak_dikenal": 1,
    "waktu_masa_depan": 1,
    "waktu_terlambat": 1,
    "semua_kosong": 1,
    "duplikat": 2,
}


def dw_connection():
    return psycopg2.connect(
        host=os.getenv("DW_HOST", "postgres"),
        port=os.getenv("DW_PORT", "5432"),
        dbname=os.getenv("DW_DB", "air_quality_dw"),
        user=os.getenv("DW_USER", "platform"),
        password=os.getenv("DW_PASSWORD", "platform123"),
        connect_timeout=5,
    )


def load_baseline() -> dict:
    """Ambil nilai jam sekarang (atau jam terdekat) per kota dari fakta jalur batch."""
    query = """
        SELECT DISTINCT ON (l.city)
            l.city, f.pm2_5, f.pm10, f.carbon_monoxide,
            f.nitrogen_dioxide, f.sulphur_dioxide, f.ozone
        FROM warehouse.fact_air_quality f
        JOIN warehouse.dim_location l USING (location_key)
        JOIN warehouse.dim_time t USING (time_key)
        ORDER BY l.city, ABS(EXTRACT(EPOCH FROM (t.valid_at - %s)))
    """
    baseline = {city: list(values) for city, values in FALLBACK.items()}
    now_wib = datetime.now(WIB).replace(tzinfo=None)
    try:
        with dw_connection() as conn, conn.cursor() as cur:
            cur.execute(query, (now_wib,))
            for city, *values in cur.fetchall():
                if city in baseline:
                    baseline[city] = [
                        v if v is not None else baseline[city][i]
                        for i, v in enumerate(values)
                    ]
        source = "fakta jalur batch"
    except psycopg2.Error as exc:
        source = f"cadangan (database belum siap: {exc.__class__.__name__})"
    print(f"[SIMULATOR] Nilai dasar diperbarui dari {source}", flush=True)
    return baseline


def clean_reading(city: str, base: list, now_wib: datetime) -> dict:
    reading = {
        "reading_id": uuid.uuid4().hex[:12],
        "city": city,
        "reading_at": now_wib.strftime("%Y-%m-%d %H:%M:%S"),
        "dirty_type": "bersih",
    }
    for name, value in zip(POLLUTANTS, base):
        reading[name] = round(max(value * random.gauss(1.0, 0.08), 0.1), 1)
    return reading


def make_dirty(reading: dict) -> list[dict]:
    """Kotori satu bacaan. Hasilnya list karena 'duplikat' menghasilkan dua pesan."""
    kind = random.choices(list(DIRTY_TYPES), weights=list(DIRTY_TYPES.values()))[0]
    reading["dirty_type"] = kind
    target = random.choice(POLLUTANTS)

    if kind == "negatif":
        reading[target] = -abs(reading[target])
    elif kind == "outlier":
        reading["pm2_5"] = 9999
    elif kind == "format_koma":
        reading[target] = str(reading[target]).replace(".", ",")
    elif kind == "teks_bukan_angka":
        reading[target] = random.choice(["N/A", "error", "-", ""])
    elif kind == "field_hilang":
        reading.pop(target)
    elif kind == "nama_kota_beda":
        reading["city"] = random.choice(CITY_VARIANTS[reading["city"]])
    elif kind == "kota_tak_dikenal":
        reading["city"] = random.choice(["Bekasi", "Kota X", "???"])
    elif kind == "waktu_masa_depan":
        future = datetime.strptime(reading["reading_at"], "%Y-%m-%d %H:%M:%S") + timedelta(hours=2)
        reading["reading_at"] = future.strftime("%Y-%m-%d %H:%M:%S")
    elif kind == "waktu_terlambat":
        late = datetime.strptime(reading["reading_at"], "%Y-%m-%d %H:%M:%S") - timedelta(hours=3)
        reading["reading_at"] = late.strftime("%Y-%m-%d %H:%M:%S")
    elif kind == "semua_kosong":
        for name in POLLUTANTS:
            reading[name] = None
    elif kind == "duplikat":
        return [reading, dict(reading)]
    return [reading]


def write_file(messages: list[dict]) -> str:
    """Tulis ke folder sementara dulu, baru dipindah. Spark tidak boleh membaca file setengah jadi."""
    name = f"sensor_{datetime.now(WIB):%Y%m%d_%H%M%S_%f}.json"
    tmp_path = os.path.join(TMP_DIR, name)
    with open(tmp_path, "w", encoding="utf-8") as handle:
        for message in messages:
            handle.write(json.dumps(message) + "\n")
    os.replace(tmp_path, os.path.join(INCOMING_DIR, name))
    return name


def main() -> None:
    os.makedirs(INCOMING_DIR, exist_ok=True)
    os.makedirs(TMP_DIR, exist_ok=True)
    print(f"[SIMULATOR] Mulai: interval {INTERVAL} detik, porsi data kotor {DIRTY_RATE:.0%}", flush=True)

    baseline = load_baseline()
    last_refresh = time.monotonic()

    while True:
        if time.monotonic() - last_refresh > BASELINE_REFRESH_SECONDS:
            baseline = load_baseline()
            last_refresh = time.monotonic()

        now_wib = datetime.now(WIB).replace(tzinfo=None)
        messages = []
        for city, base in baseline.items():
            reading = clean_reading(city, base, now_wib)
            if random.random() < DIRTY_RATE:
                messages.extend(make_dirty(reading))
            else:
                messages.append(reading)

        name = write_file(messages)
        dirty = [m["dirty_type"] for m in messages if m["dirty_type"] != "bersih"]
        print(f"[SIMULATOR] {name}: {len(messages)} pesan, kotor: {', '.join(dirty) or '-'}", flush=True)
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
