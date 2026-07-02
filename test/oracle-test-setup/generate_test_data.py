"""
Test Data Generator
-------------------
Generates:
  - 1M rows  ~1 GB  → saved as 'data.parquet'         (main table seed)
  - 100K rows ~100MB → saved as 'data_staging.parquet' (mixed: 50K updates + 50K new)

Schema designed to hit ~1KB/row:
  id, first_name, last_name, email, phone, address, city,
  country, postal_code, company, job_title, department,
  status, category, amount, score, description, notes,
  metadata, created_at, updated_at
"""

import polars as pl
import numpy as np
from datetime import datetime, timedelta
import random
import string
import time
import os
import shutil

# ── Seed for reproducibility ───────────────────────────────────────────────
SEED = 42
rng  = np.random.default_rng(SEED)
random.seed(SEED)

# ── Size targets ───────────────────────────────────────────────────────────
MAIN_ROWS    = 1_000_000   # ~1 GB
STAGING_ROWS = 1_000_000     # ~100 MB
UPDATE_RATIO = 0.5         # 50% updates, 50% new inserts in staging

# ── Lookup pools (realistic but fast — no Faker needed) ───────────────────
FIRST_NAMES  = ["Alice","Bob","Charlie","Diana","Edward","Fiona","George",
                "Hannah","Ivan","Julia","Kevin","Laura","Michael","Nina",
                "Oscar","Paula","Quinn","Rachel","Steve","Tina","Uma",
                "Victor","Wendy","Xander","Yara","Zach"]

LAST_NAMES   = ["Smith","Johnson","Williams","Brown","Jones","Garcia",
                "Miller","Davis","Rodriguez","Martinez","Hernandez",
                "Lopez","Gonzalez","Wilson","Anderson","Thomas","Taylor",
                "Moore","Jackson","Martin","Lee","Perez","Thompson","White"]

CITIES       = ["New York","Los Angeles","Chicago","Houston","Phoenix",
                "Philadelphia","San Antonio","San Diego","Dallas","San Jose",
                "Austin","Jacksonville","Fort Worth","Columbus","Charlotte",
                "Indianapolis","San Francisco","Seattle","Denver","Nashville"]

COUNTRIES    = ["US","UK","CA","AU","DE","FR","JP","IN","BR","MX"]

COMPANIES    = ["Acme Corp","Globex","Initech","Umbrella Ltd","Cyberdyne",
                "Soylent Corp","Massive Dynamic","Oscorp","Stark Industries",
                "Wayne Enterprises","Hooli","Pied Piper","Dunder Mifflin",
                "Vandelay Industries","Prestige Worldwide"]

JOB_TITLES   = ["Engineer","Manager","Analyst","Developer","Designer",
                "Director","VP","Consultant","Specialist","Coordinator",
                "Architect","Lead","Scientist","Administrator","Executive"]

DEPARTMENTS  = ["Engineering","Sales","Marketing","Finance","HR",
                "Operations","Legal","Product","Support","Research"]

STATUSES     = ["active","inactive","pending","suspended","verified"]

CATEGORIES   = ["premium","standard","basic","enterprise","trial"]

# ── Helper: random text of ~N chars ───────────────────────────────────────
def rand_text(size: int, n: int, pool_size: int = 20_000) -> list[str]:
    """Generate n strings by sampling from a pre-generated pool to save memory."""
    effective_pool = min(pool_size, n)
    chars = np.array(list(string.ascii_letters + string.digits + "     "), dtype="U1")
    
    # Using uint8 saves 8x memory over int64 during pool generation
    idx = rng.integers(0, len(chars), size=(effective_pool, size), dtype=np.uint8)
    pool = ["".join(row) for row in chars[idx]]
    
    # Randomly sample from our pool to hit the target row count 'n'
    return rng.choice(pool, size=n).tolist()

# ── Helper: random timestamps ─────────────────────────────────────────────
BASE_DATE = datetime(2020, 1, 1)
MAX_DAYS  = (datetime(2024, 12, 31) - BASE_DATE).days

def rand_timestamps(n: int) -> list[datetime]:
    offsets = rng.integers(0, MAX_DAYS * 86400, size=n)
    return [BASE_DATE + timedelta(seconds=int(s)) for s in offsets]

# ─────────────────────────────────────────────────────────────────────────
# CORE GENERATOR
# ─────────────────────────────────────────────────────────────────────────
def generate_rows(ids: np.ndarray, updated_rows: set = None) -> pl.DataFrame:
    """
    Build a Polars DataFrame for the given IDs.
    If updated_rows is provided, those IDs get modified field values
    to simulate staging updates.
    """
    n = len(ids)
    is_update = updated_rows is not None

    print(f"  Generating {n:,} rows ({'staging/updates' if is_update else 'main'})...")
    t0 = time.time()

    # -- Pools (vectorised picks) --
    first_names  = rng.choice(FIRST_NAMES,  size=n)
    last_names   = rng.choice(LAST_NAMES,   size=n)
    cities       = rng.choice(CITIES,       size=n)
    countries    = rng.choice(COUNTRIES,    size=n)
    companies    = rng.choice(COMPANIES,    size=n)
    job_titles   = rng.choice(JOB_TITLES,   size=n)
    departments  = rng.choice(DEPARTMENTS,  size=n)
    statuses     = rng.choice(STATUSES,     size=n)
    categories   = rng.choice(CATEGORIES,   size=n)

    # -- Derived fields --
    emails = [
        f"{fn.lower()}.{ln.lower()}{rng.integers(1,999)}@example.com"
        for fn, ln in zip(first_names, last_names)
    ]
    phones = [
        f"+1-{rng.integers(200,999)}-{rng.integers(100,999)}-{rng.integers(1000,9999)}"
        for _ in range(n)
    ]
    addresses = [
        f"{rng.integers(1,9999)} {rng.choice(['Main','Oak','Maple','Pine','Cedar'])} "
        f"{rng.choice(['St','Ave','Blvd','Dr','Rd'])}"
        for _ in range(n)
    ]
    postal_codes = [f"{rng.integers(10000,99999)}" for _ in range(n)]

    # -- Numerics --
    amounts = np.round(rng.uniform(0.01, 100_000.00, size=n), 2)
    scores  = np.round(rng.uniform(0.0,  100.0,      size=n), 4)

    # -- Long text fields (~500 chars each for description, ~200 for notes) --
    # Simulate update by slightly changing text for update rows
    prefix_d = "UPDATED: " if is_update else ""
    prefix_n = "REVISED: " if is_update else ""
    # -- Long text fields (using the new memory-efficient pool) --
    descriptions = rand_text(490, n)
    notes        = rand_text(190, n)
    
    if is_update:
        descriptions = [f"UPDATED: {t}s" for t in descriptions]
        notes        = [f"REVISED: {t}" for t in notes]

    # -- Metadata JSON-like string (~150 chars) --
    metadata = [
        f'{{"version":{rng.integers(1,10)},"region":"{c}","tier":"{cat}","ref":"{rng.integers(100000,999999)}"}}'
        for c, cat in zip(countries, categories)
    ]

    # -- Timestamps --
    created_at = rand_timestamps(n)
    updated_at = [
        ts + timedelta(days=int(rng.integers(0, 365)))
        for ts in created_at
    ]

    df = pl.DataFrame({
        "id":           ids.tolist(),
        "first_name":   first_names.tolist(),
        "last_name":    last_names.tolist(),
        "email":        emails,
        "phone":        phones,
        "address":      addresses,
        "city":         cities.tolist(),
        "country":      countries.tolist(),
        "postal_code":  postal_codes,
        "company":      companies.tolist(),
        "job_title":    job_titles.tolist(),
        "department":   departments.tolist(),
        "status":       statuses.tolist(),
        "category":     categories.tolist(),
        "amount":       amounts.tolist(),
        "score":        scores.tolist(),
        "description":  descriptions,
        "notes":        notes,
        "metadata":     metadata,
        "created_at":   created_at,
        "updated_at":   updated_at,
    })

    elapsed = time.time() - t0
    print(f"  Done in {elapsed:.1f}s")
    return df


# ─────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Configure chunking parameters
    BATCH_SIZE = 1_000_000  # 1M rows per batch (uses ~200MB-300MB RAM max)
    
    # ── 1. Generate main table in batches ──────────────────────────────────
    print(f"\n[1/2] Generating MAIN table ({MAIN_ROWS:,} rows total in chunks)...")
    
    out_main_dir = "test/seed/"
    if os.path.exists(out_main_dir):
        shutil.rmtree(out_main_dir)
    os.makedirs(out_main_dir, exist_ok=True)
    
    # Process in loops
    for start_idx in range(0, MAIN_ROWS, BATCH_SIZE):
        end_idx = min(start_idx + BATCH_SIZE, MAIN_ROWS)
        print(f"\n--- Batch: Rows {start_idx:,} to {end_idx:,} ---")
        
        # Generate the ID array for just this batch
        batch_ids = np.arange(start_idx + 1, end_idx + 1, dtype=np.int64)
        
        # Use your refactored generate_rows function
        df_batch = generate_rows(batch_ids)
        
        # Save this specific chunk to a numbered parquet file
        chunk_filename = os.path.join(out_main_dir, f"batch_{start_idx // BATCH_SIZE}.parquet")
        df_batch.write_parquet(chunk_filename, compression="snappy")
        print(f"  Saved chunk → {chunk_filename}")
        
        # Explicitly clean up memory before next loop iteration
        del df_batch
        import gc; gc.collect()

    # ── 2. Generate staging table (100K rows) ───────────────────────────
    # Staging is already small (~100MB), so we can generate it in one shot safely
    print("\n[2/2] Generating STAGING table (100K rows ~100MB)...")

    n_updates = int(STAGING_ROWS * UPDATE_RATIO)
    n_inserts = STAGING_ROWS - n_updates

    # Pick sample IDs from across the whole 10M spectrum
    main_ids_pool = np.arange(1, MAIN_ROWS + 1, dtype=np.int64)
    update_ids = rng.choice(main_ids_pool, size=n_updates, replace=False)
    update_ids_set = set(update_ids.tolist())

    new_ids = np.arange(MAIN_ROWS + 1, MAIN_ROWS + n_inserts + 1, dtype=np.int64)
    staging_ids = np.concatenate([update_ids, new_ids])
    rng.shuffle(staging_ids)

    df_staging = generate_rows(staging_ids, updated_rows=update_ids_set)

    out_staging = "test/new_data.parquet"
    df_staging.write_parquet(out_staging, compression="snappy")
    print(f"  Saved staging → {out_staging}")

    print("\n" + "─" * 55)
    print("  ALL BATCHES GENERATED SUCCESSFULLY!")
    print("─" * 55)

    # # ── 1. Generate main table (1M rows) ──────────────────────────────────
    # print("\n[1/2] Generating MAIN table (1M rows ~1GB)...")
    # main_ids = np.arange(1, MAIN_ROWS + 1, dtype=np.int64)
    # df_main  = generate_rows(main_ids)

    # out_main = "data.parquet"
    # df_main.write_parquet(out_main, compression="snappy")
    # size_main = __import__("os").path.getsize(out_main) / (1024 ** 3)
    # print(f"  Saved → {out_main}  ({size_main:.2f} GB, {len(df_main):,} rows)")

    # # ── 2. Generate staging (100K rows: 50K updates + 50K new) ───────────
    # print("\n[2/2] Generating STAGING table (100K rows ~100MB)...")

    # n_updates = int(STAGING_ROWS * UPDATE_RATIO)   # 50,000 existing IDs
    # n_inserts = STAGING_ROWS - n_updates            # 50,000 new IDs

    # # 50K IDs sampled from the existing main table (updates)
    # update_ids = rng.choice(main_ids, size=n_updates, replace=False)
    # update_ids_set = set(update_ids.tolist())

    # # 50K brand-new IDs (inserts)
    # new_ids = np.arange(MAIN_ROWS + 1, MAIN_ROWS + n_inserts + 1, dtype=np.int64)

    # staging_ids = np.concatenate([update_ids, new_ids])
    # rng.shuffle(staging_ids)  # mix updates and inserts

    # df_staging = generate_rows(staging_ids, updated_rows=update_ids_set)

    # out_staging = "data_staging.parquet"
    # df_staging.write_parquet(out_staging, compression="snappy")
    # size_staging = __import__("os").path.getsize(out_staging) / (1024 ** 2)
    # print(f"  Saved → {out_staging}  ({size_staging:.1f} MB, {len(df_staging):,} rows)")

    # # ── Summary ───────────────────────────────────────────────────────────
    # print("\n" + "─" * 55)
    # print("  SUMMARY")
    # print("─" * 55)
    # print(f"  data.parquet         {size_main:.2f} GB   {MAIN_ROWS:>10,} rows")
    # print(f"  data_staging.parquet {size_staging:.1f} MB  {STAGING_ROWS:>10,} rows")
    # print(f"    ├─ updates (existing IDs) : {n_updates:>8,} rows")
    # print(f"    └─ inserts (new IDs)      : {n_inserts:>8,} rows")
    # print("─" * 55)

    # # ── Schema preview ────────────────────────────────────────────────────
    # print("\n  SCHEMA:")
    # print(df_main.schema)
    # print("\n  SAMPLE (5 rows):")
    # print(df_main.head(5))
