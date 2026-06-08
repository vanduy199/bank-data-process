"""
Standalone loader: MinIO parquet -> Snowflake RAW tables.

A CLI version of docker/dags/minio_to_snowflake_dag.py so the warehouse load can
be run without Airflow. Reuses the same env vars (root .env). Run:

    python load_minio_to_snowflake.py
"""
import os
import tempfile

import boto3
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

# Host-side MinIO endpoint (override the in-cluster http://minio:9000 default)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT_HOST", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET = os.getenv("MINIO_BUCKET", "raw1")

TABLES = [
    t.strip()
    for t in os.getenv(
        "CDC_TABLES", "customers,accounts,transactions,services,service_usage"
    ).split(",")
    if t.strip()
]


def main():
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    conn = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.getenv("SNOWFLAKE_DB", "BANKING"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
    )
    cur = conn.cursor()

    with tempfile.TemporaryDirectory() as tmp:
        for table in TABLES:
            resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=f"{table}/")
            objects = resp.get("Contents", [])
            if not objects:
                print(f"[{table}] no files in MinIO, skipping.")
                continue

            # Deterministic full refresh: RAW.<table> == current MinIO contents.
            # 1) clear the table stage, 2) PUT current files, 3) TRUNCATE,
            # 4) COPY FORCE (TRUNCATE purges load history; FORCE ignores any leftover).
            cur.execute(f"REMOVE @%{table}")
            for obj in objects:
                key = obj["Key"]
                local = os.path.join(tmp, os.path.basename(key))
                s3.download_file(BUCKET, key, local)
                cur.execute(f"PUT file://{local} @%{table} OVERWRITE=TRUE")

            cur.execute(f"TRUNCATE TABLE {table}")
            cur.execute(
                f"COPY INTO {table} FROM @%{table} "
                f"FILE_FORMAT=(TYPE=PARQUET) ON_ERROR='CONTINUE' FORCE=TRUE"
            )
            cur.execute(f"SELECT count(*) FROM {table}")
            n = cur.fetchone()[0]
            print(f"[{table}] {len(objects)} file(s) -> {n} rows in BANKING.RAW.{table}")

    cur.close()
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
