# Testing write_database() dengan Oracle di Docker

## Isi folder
```
oracle-test-setup/
├── docker-compose.yml            # 2 service: oracle-db + app
├── Dockerfile.app                # app container: Oracle Instant Client (sqlldr) + Python
├── requirements.txt
├── init-scripts/
│   ├── 01_create_tables.sql      # tabel 'customers' (untuk test_write_database.py)
│   └── 02_create_data_table.sql  # tabel 'data' (untuk test skala besar)
├── write_database.py
├── test_write_database.py        # file "main" untuk testing cepat (skala kecil)
├── generate_test_data.py         # generator data uji skala besar (1M+ baris)
└── load_test_data_oracle.py      # seed + upsert data besar via write_database()
```

## Kenapa 2 container?
`sqlldr` adalah binary Oracle Client, bukan library Python — jadi Python
container yang menjalankan `write_database()` juga butuh Oracle Instant
Client + SQL*Loader terpasang. Daripada instal manual di mesin lokal,
lebih gampang build sekali lewat Docker.

- **oracle-db**: image `gvenzl/oracle-xe:21-slim` (Oracle XE, gratis, ringan).
- **app**: base `oraclelinux:8-slim` + Oracle Instant Client (basiclite + tools)
  supaya `sqlldr` tersedia, plus `polars` & `oracledb`.

## Cara menjalankan

```bash
cd oracle-test-setup
docker compose up --build
```

Yang terjadi:
1. `oracle-db` start, tunggu sampai healthy (~30–60 detik untuk first run
   karena Oracle XE inisialisasi database baru), lalu otomatis membuat
   user `testuser` dan menjalankan `init-scripts/01_create_tables.sql`
   (membuat tabel `customers`).
2. `app` di-build (instal Instant Client + sqlldr + dependencies Python),
   lalu menunggu `oracle-db` sehat, lalu menjalankan `test_write_database.py`.
3. Log test (append, replace, upsert, empty df, invalid mode, dst) muncul
   di terminal.

Untuk menjalankan ulang test tanpa rebuild image:
```bash
docker compose run --rm app python3 test_write_database.py
```

Untuk reset total (hapus data Oracle, mulai dari nol):
```bash
docker compose down -v
```

## Kalau mau kirim SQL manual / cek isi tabel
```bash
docker exec -it oracle-xe-test sqlplus testuser/TestPass123@//localhost:1521/XEPDB1
```
Lalu misal:
```sql
SELECT COUNT(*) FROM customers;
SELECT * FROM customers WHERE customer_id < 10;
```

## Test skala besar (jutaan baris, pakai generate_test_data.py)

Kalau kamu mau uji `write_database()` dengan data realistis skala besar
(bukan cuma 500 baris sample), pakai `generate_test_data.py` yang sudah
disesuaikan ke dalam folder ini, plus `load_test_data_oracle.py`
(adaptasi dari `load_test_data.py` — versi aslinya untuk PostgreSQL/adbc,
di sini diganti ke Oracle + `write_database()`).

```bash
# 1) Generate data uji (parquet) — sekali saja
docker compose run --rm app python3 generate_test_data.py

# 2) Seed tabel 'data' + jalankan upsert dari staging
docker compose run --rm app python3 load_test_data_oracle.py
```

Apa yang terjadi:
1. `generate_test_data.py` membuat `test/seed/*.parquet` (tabel utama)
   dan `test/new_data.parquet` (data staging: campuran update + insert baru),
   memakai polars + numpy, tanpa menyentuh Oracle sama sekali.
2. `load_test_data_oracle.py`:
   - **Seed**: baca tiap file di `test/seed/` satu per satu (bukan gabung
     semua sekaligus) lalu panggil `write_database(mode="replace")` untuk
     file pertama dan `mode="append"` untuk file berikutnya, dengan
     `chunk_size=200_000` supaya tiap load ke SQL*Loader tetap terbatas.
   - **Upsert**: baca `test/new_data.parquet`, panggil
     `write_database(mode="upsert", key_columns=["id"], chunk_size=200_000)`.

### Catatan penting
- **Skala default**: `MAIN_ROWS = 1_000_000` (~1GB) dan `STAGING_ROWS` di
  file `generate_test_data.py` juga di-set `1_000_000` (bukan 100K seperti
  yang disebut di komentar/docstring file itu) — jadi total data yang
  dihasilkan ~2 juta baris, ~2GB. Kalau mau uji cepat dulu, turunkan
  `MAIN_ROWS`/`STAGING_ROWS` di `generate_test_data.py` (misal ke `50_000`)
  sebelum generate.
- **Dukungan datetime ditambahkan**: `write_database.py` di folder ini
  sudah saya perbarui supaya kolom `created_at`/`updated_at` (bertipe
  Datetime di Polars) otomatis dapat format mask `TIMESTAMP "YYYY-MM-DD
  HH24:MI:SS"` di control file SQL*Loader — tanpa ini, load akan gagal
  karena format tanggal default Oracle tidak cocok dengan yang ditulis
  Polars.
- **RAM caller**: `pl.read_parquet(path)` tetap memuat satu file batch
  penuh ke RAM sebelum dikirim ke `write_database()` (lihat diskusi
  sebelumnya soal batas chunking). Karena `generate_test_data.py` memakai
  `BATCH_SIZE = MAIN_ROWS`, saat ini hanya ada **1 file batch** berisi
  seluruh 1 juta baris (~1GB) — kalau mau benar-benar membagi beban RAM
  saat seeding, kecilkan `BATCH_SIZE` di `generate_test_data.py` (misal
  `200_000`) supaya jadi beberapa file, dan loop di
  `load_test_data_oracle.py` akan otomatis memuatnya satu-satu.
- Tabel `data` dibuat otomatis lewat `init-scripts/02_create_data_table.sql`
  saat container `oracle-db` pertama kali start (kolom disesuaikan dengan
  skema di `generate_test_data.py`). Kalau kamu ubah skema generator,
  update juga DDL ini.

