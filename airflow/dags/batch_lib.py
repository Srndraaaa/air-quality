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

    # Jam yang terisi penuh: menit dan detik dibuang, tzinfo juga dibuang.
    assert hour_floor_wita(datetime(2026, 10, 1, 7, 45, 30, tzinfo=WITA)) == datetime(
        2026, 10, 1, 7, 0
    )

    # Rentang terbalik -> tidak ada yang hilang.
    assert missing_windows([lo], hi, lo) == []

    print("[BATCH-LIB] Self-check lolos")


if __name__ == "__main__":
    _self_check()
