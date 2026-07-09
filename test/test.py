import sys
import os

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
from dbdf.core import write_database
from dbdf.core import read_database

df = read_database(db_type="csv", connection_info="test/seed.csv", query=None, chunk_size=8192)
write_database(db_type="parquet", connection_info="test/seed.parquet", df=df, table_name="")
