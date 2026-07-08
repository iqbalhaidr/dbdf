import polars as pl
import adbc_driver_postgresql.dbapi

df = pl.DataFrame({
    "name": ["Alice", "Bob", "Charlies"],
    "age": [25, 30, 35],
    "city": ["New York", "London", "Paris"]
})

def _q(self, in_str: str):
    return f'"{in_str}"'

nama = "nNAMA"
QUERY_TRUNCATE = f'TRUNCATE {_q(nama)}'
print(QUERY_TRUNCATE)

with adbc_driver_postgresql.dbapi.connect("postgresql://postgres:postgres@localhost:5432/mydb2") as conn:
    with conn.cursor() as cur:
        # cur.adbc_ingest(
        #     table_name=nama,
        #     data=df.to_arrow(),
        #     mode="create"
        # )
        cur.execute(QUERY_TRUNCATE)
    conn.commit()
# Create the DataFrame



# print(df)
