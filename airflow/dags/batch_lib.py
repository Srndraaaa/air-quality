# HELPER MURNI JALUR BATCH
# ========================
# Modul ini sengaja TIDAK mengimpor Airflow, psycopg2, atau requests,
# supaya logikanya bisa diuji tanpa stack. Pola yang sama dipakai ml_lib.py
# (lihat design.md D-015): logika yang dipakai lebih dari satu task diletakkan
# di modul berdiri sendiri, bukan disalin ke dalam file DAG.
#
# Isi:
# - WITA / hour_floor_wita() : konvensi jam data bisnis (naive WITA, D-001).
# - missing_windows()         : mencari jam yang kosong lalu mengelompokkan
#                               jam-jam berurutan menjadi satu window.
# - count_missing_hours()     : berapa jam yang kosong di dalam rentang.
# - select_gaps()             : memilih baris yang masih boleh dicoba ulang
#                               berdasarkan nilai dan riwayat percobaan.
#
# Jalankan self-check dengan:
#   python airflow/dags/batch_lib.py

from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# Zona waktu seluruh data bisnis di repo ini (design.md D-001).
# WITA tidak pernah memakai daylight saving, jadi satu jam selalu 60 menit.
WITA = ZoneInfo("Asia/Makassar")

# Selisih satu jam, dipakai saat menggeser deret jam.
ONE_HOUR = timedelta(hours=1)


def hour_floor_wita(now: datetime | None = None) -> datetime:
    """Jam penuh saat ini menurut WITA, tanpa informasi zona waktu.

    Kolom `valid_at` di gudang adalah TIMESTAMP naive dalam WITA, jadi
    perbandingan "sekarang" dengan kolom itu harus dilakukan di jam WITA juga
    (design.md D-001). Argumen `now` dipakai oleh self-check.
    """
    return (now or datetime.now(WITA)).replace(
        tzinfo=None, minute=0, second=0, microsecond=0
    )


def missing_windows(
    existing: list[datetime],
    lo: datetime,
    hi: datetime,
) -> list[tuple[datetime, datetime]]:
    """Jam-jam yang kosong antara lo..hi, dikelompokkan jadi window berurutan.

    `existing` adalah jam-jam yang sudah ada di gudang. Hasilnya daftar
    (mulai, selesai) yang inklusif; jam-jam berurutan yang kosong digabung
    menjadi satu window supaya API hanya dipanggil sekali per window.
    """
    have = set(existing)
    windows: list[tuple[datetime, datetime]] = []
    start: datetime | None = None
    current = lo

    while current <= hi:
        if current not in have:
            # Jam kosong: mulai window baru kalau belum ada yang berjalan.
            if start is None:
                start = current
        elif start is not None:
            # Jam terisi: tutup window yang sedang berjalan.
            windows.append((start, current - ONE_HOUR))
            start = None
        current += ONE_HOUR

    # Kalau rentang berakhir saat masih ada lubuk, window terakhir belum ditutup.
    if start is not None:
        windows.append((start, hi))

    return windows


def count_missing_hours(
    existing: list[datetime],
    lo: datetime,
    hi: datetime,
) -> int:
    """Berapa jam yang kosong antara lo..hi.

    Dipakai quality_check untuk memutuskan apakah pipeline boleh lolos.
    """
    return sum(
        int((end - start) / ONE_HOUR) + 1
        for start, end in missing_windows(existing, lo, hi)
    )


def select_gaps(
    rows: list[tuple[datetime, float | None, float | None, int, datetime | None]],
    now: datetime,
    max_attempts: int,
    cooldown_hours: int,
) -> set[datetime]:
    """Pilih jam yang masih boleh diminta ulang ke Open-Meteo.

    `rows` berisi (valid_at, pm2_5, pm10, backfill_attempts, last_backfill_at)
    untuk jam yang barisnya sudah ada di raw staging. `now` dan
    `last_backfill_at` sama-sama naive WITA (design.md D-001).

    Satu jam tidak diminta ulang kalau:
    - pm2_5 dan pm10 sudah sama-sama terisi,
    - percobaannya sudah mencapai max_attempts, atau
    - percobaan terakhir masih di dalam cooldown.
    """
    cooldown = timedelta(hours=cooldown_hours)
    retry: set[datetime] = set()

    for valid_at, pm2_5, pm10, attempts, last_attempt in rows:
        if pm2_5 is not None and pm10 is not None:
            continue
        if attempts >= max_attempts:
            continue
        if (
            attempts > 0
            and last_attempt is not None
            and now - last_attempt < cooldown
        ):
            continue
        retry.add(valid_at)

    return retry


# ---------------------------------------------------------------------------
# SELF-CHECK
# ---------------------------------------------------------------------------

def _self_check() -> None:
    """Pemeriksaan mandiri logika di modul ini.

    Dijalankan tanpa Airflow dan tanpa database, jadi harus cepat.
    """
    lo = datetime(2026, 10, 1, 0, 0)
    hi = datetime(2026, 10, 1, 6, 0)
    hour = timedelta(hours=1)

    # Semua jam terisi -> tidak ada window.
    assert missing_windows([lo + hour * i for i in range(7)], lo, hi) == []

    # Tidak ada data sama sekali -> satu window menutupi seluruh rentang.
    assert missing_windows([], lo, hi) == [(lo, hi)]

    # Lubuk di tengah dan satu jam kosong di ekor -> dua window terpisah.
    existing = [lo, lo + hour, lo + hour * 4, lo + hour * 5]
    assert missing_windows(existing, lo, hi) == [
        (lo + hour * 2, lo + hour * 3),
        (lo + hour * 6, lo + hour * 6),
    ]

    # Hitungan jam harus sama dengan isi window di atas.
    assert count_missing_hours(existing, lo, hi) == 3

    # select_gaps: nilai lengkap tidak diminta ulang.
    now = datetime(2026, 10, 3, 12, 0)
    assert select_gaps([(lo, 10.0, 20.0, 0, None)], now, 3, 24) == set()

    # PM2.5 atau PM10 yang NULL masih dianggap gap.
    assert select_gaps(
        [(lo, None, 20.0, 0, None), (lo + hour, 10.0, None, 0, None)],
        now,
        3,
        24,
    ) == {lo, lo + hour}

    # Percobaan sudah maksimum -> jangan diminta lagi.
    assert select_gaps([(lo, None, None, 3, None)], now, 3, 24) == set()

    # Cooldown masih aktif -> tunggu; sudah lewat -> boleh dicoba.
    assert select_gaps(
        [(lo, None, None, 1, now - timedelta(hours=23))], now, 3, 24
    ) == set()
    assert select_gaps(
        [(lo, None, None, 1, now - timedelta(hours=24))], now, 3, 24
    ) == {lo}

    # attempts=0 dengan last_backfill_at lama tidak boleh terkunci cooldown.
    assert select_gaps([(lo, None, None, 0, now)], now, 3, 24) == {lo}

    # Campuran: hanya baris yang masih boleh dicoba yang dikembalikan.
    mixed = [
        (lo, 10.0, 20.0, 0, None),
        (lo + hour, None, 20.0, 0, None),
        (lo + hour * 2, None, None, 3, None),
    ]
    assert select_gaps(mixed, now, 3, 24) == {lo + hour}

    # Jam yang terisi penuh: menit dan detik dibuang, tzinfo juga dibuang.
    assert hour_floor_wita(datetime(2026, 10, 1, 7, 45, 30, tzinfo=WITA)) == datetime(
        2026, 10, 1, 7, 0
    )

    # Rentang terbalik -> tidak ada yang hilang.
    assert missing_windows([lo], hi, lo) == []

    print("[BATCH-LIB] Self-check lolos")


if __name__ == "__main__":
    _self_check()
