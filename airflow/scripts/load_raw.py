"""Loads new S3 files into the RAW tables. COPY INTO skips files it already loaded, so re-running is safe."""
import os

import snowflake.connector

TABLES = ["customers", "accounts", "transactions"]

conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    role="LOADER",
    warehouse="COMPUTE_WH",
    database="BANKING",
    schema="RAW",
)
try:
    cur = conn.cursor()
    for table in TABLES:
        cur.execute(
            f"COPY INTO {table} (event, source_file) "
            f"FROM (SELECT $1, METADATA$FILENAME FROM @s3_stage/{table}/)"
        )
        print(table, cur.fetchall())
finally:
    conn.close()
