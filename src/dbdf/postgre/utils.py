def _create_staging_table(conn, table_name):
    # Unlogged, should be TEMP
    QUERY = f'CREATE TEMP TABLE "{table_name}_staging" (LIKE "{table_name}")'

    with conn.cursor() as cur:
        cur.execute(QUERY)

    return

def _get_columns_name(conn, table_name, schema: str = "public"):
    QUERY = f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = '{schema}'
        AND table_name = '{table_name}'
        ORDER BY ordinal_position
    """

    with conn.cursor() as cur:
        cur.execute(QUERY)
        rows = cur.fetchall()

    columns = [row[0] for row in rows]
    return columns

def _upsert_query_builder(table_name, columns_name, primary_key):
    col_list    = ", ".join(f'"{c}"' for c in columns_name)
    update_list = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in columns_name if c != f"{primary_key}"])

    sql = f"""
        INSERT INTO "{table_name}" ({col_list})
        SELECT * FROM "{table_name}_staging"
        ON CONFLICT ("{primary_key}")
        DO UPDATE SET {update_list};
    """
    return sql