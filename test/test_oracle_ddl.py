import polars as pl
from datetime import date
import oracledb
import sys, os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from src.dbdf.oracle import _ensure_table_exists, _is_table_exists, _infer_dtype

ORACLE_URI = "testuser/TestPass123@localhost:1521/XEPDB1"


def cleanup(conn, tables):
    cur = conn.cursor()
    for t in tables:
        try:
            cur.execute(f"DROP TABLE {t}")
        except oracledb.DatabaseError:
            pass
    conn.commit()
    cur.close()


def get_table_columns(conn, table_name):
    """Ambil kolom + tipe data dari Oracle."""
    cur = conn.cursor()
    cur.execute("""
        SELECT column_name, data_type, data_length, data_precision, data_scale
        FROM user_tab_columns
        WHERE table_name = :1
        ORDER BY column_id
    """, [table_name.upper()])
    cols = cur.fetchall()
    cur.close()
    return cols


# ═══════════════════════════════════════
conn = oracledb.connect(ORACLE_URI)
ALL_TABLES = ["TEST_DDL_1", "TEST_DDL_2", "TEST_DDL_3"]
cleanup(conn, ALL_TABLES)


# ── Test 1: Auto-create tabel sederhana ──
print("="*55)
print("  TEST 1: Auto-create tabel + Primary Key")
print("="*55)

df1 = pl.DataFrame({
    "customer_id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "score": [95.5, 87.3, 91.0],
    "join_date": [date(2024,1,1), date(2024,6,15), date(2024,2,20)],
    "is_active": [True, False, True],
})

print("Schema DataFrame:")
for col, dtype in df1.schema.items():
    print(f"  {col}: {dtype}")

_ensure_table_exists(conn, df1, "TEST_DDL_1", key_columns=["customer_id"])

print(f"\nTabel exists? {_is_table_exists(conn, 'TEST_DDL_1')}")
print("\nKolom di Oracle:")
for col_name, data_type, length, precision, scale in get_table_columns(conn, "TEST_DDL_1"):
    detail = f"({precision},{scale})" if precision else f"({length})" if data_type == "VARCHAR2" else ""
    print(f"  {col_name:20s} → {data_type}{detail}")
print("✅ Test 1 selesai")


# ── Test 2: Auto-create dengan dtype_overrides ──
print("\n" + "="*55)
print("  TEST 2: Auto-create dengan dtype_overrides")
print("="*55)

df2 = pl.DataFrame({
    "item_id": ["ABC-001", "ABC-002"],
    "price": [19.99, 25.50],
    "qty": [100, 200],
})

overrides = {
    "item_id": "VARCHAR2(50)",
    "price": "NUMBER(10,2)",
}
print(f"Overrides: {overrides}")

_ensure_table_exists(conn, df2, "TEST_DDL_2", dtype_overrides=overrides)

print("\nKolom di Oracle:")
for col_name, data_type, length, precision, scale in get_table_columns(conn, "TEST_DDL_2"):
    detail = f"({precision},{scale})" if precision else f"({length})" if data_type == "VARCHAR2" else ""
    print(f"  {col_name:20s} → {data_type}{detail}")
print("✅ Test 2 selesai")


# ── Test 3: Tabel sudah ada → SKIP create ──
print("\n" + "="*55)
print("  TEST 3: Tabel sudah ada → skip create")
print("="*55)

print(f"Sebelum: exists = {_is_table_exists(conn, 'TEST_DDL_1')}")
_ensure_table_exists(conn, df1, "TEST_DDL_1", key_columns=["customer_id"])
print("(Tidak ada [DBDF] log = skip, benar)")
print("✅ Test 3 selesai")


# ── Test 4: Semua tipe data ──
print("\n" + "="*55)
print("  TEST 4: Test semua Polars dtype → Oracle mapping")
print("="*55)

df4 = pl.DataFrame({
    "col_int8": pl.Series([1], dtype=pl.Int8),
    "col_int32": pl.Series([100], dtype=pl.Int32),
    "col_int64": pl.Series([999], dtype=pl.Int64),
    "col_float32": pl.Series([1.5], dtype=pl.Float32),
    "col_float64": pl.Series([2.5], dtype=pl.Float64),
    "col_string": pl.Series(["hello"], dtype=pl.String),
    "col_bool": pl.Series([True], dtype=pl.Boolean),
    "col_date": pl.Series([date(2024,1,1)], dtype=pl.Date),
})

_ensure_table_exists(conn, df4, "TEST_DDL_3")

print("Polars dtype → Oracle type:")
oracle_cols = {c[0]: c for c in get_table_columns(conn, "TEST_DDL_3")}
for col, dtype in df4.schema.items():
    ora = oracle_cols.get(col.upper(), ("?","?","?","?","?"))
    detail = f"({ora[3]},{ora[4]})" if ora[3] else f"({ora[2]})" if ora[1] == "VARCHAR2" else ""
    print(f"  {col:15s} {str(dtype):10s} → {ora[1]}{detail}")
print("✅ Test 4 selesai")


# ── Cleanup ──
cleanup(conn, ALL_TABLES)
conn.close()

print("\n" + "="*55)
print("  SEMUA TEST SELESAI ✅")
print("="*55)