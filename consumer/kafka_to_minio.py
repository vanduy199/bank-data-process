import boto3
from kafka import KafkaConsumer
import json
import time
import pandas as pd
from collections import defaultdict
from datetime import datetime
import os
from dotenv import load_dotenv
from pathlib import Path

# -----------------------------
# Load secrets from .env
# -----------------------------
load_dotenv(Path(__file__).resolve().parent / ".env")

# -----------------------------
# Build the topic list from config (no hardcoding)
# -----------------------------
TOPIC_PREFIX = os.getenv("KAFKA_TOPIC_PREFIX", "banking_server")
SCHEMA = os.getenv("SCHEMA", "public")
TABLES = [
    t.strip()
    for t in os.getenv(
        "CDC_TABLES", "customers,accounts,transactions,services,service_usage"
    ).split(",")
    if t.strip()
]
TOPICS = [f"{TOPIC_PREFIX}.{SCHEMA}.{t}" for t in TABLES]

# Kafka consumer settings
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP"),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=os.getenv("KAFKA_GROUP"),
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# MinIO client
s3 = boto3.client(
    's3',
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY")
)

bucket = os.getenv("MINIO_BUCKET")

# Create bucket if not exists
if bucket not in [b['Name'] for b in s3.list_buckets()['Buckets']]:
    s3.create_bucket(Bucket=bucket)

# Consume and write function
def write_to_minio(table_name, records):
    if not records:
        return
    df = pd.DataFrame(records)
    date_str = datetime.now().strftime('%Y-%m-%d')
    file_path = f'{table_name}_{date_str}.parquet'
    df.to_parquet(file_path, engine='fastparquet', index=False)
    s3_key = f'{table_name}/date={date_str}/{table_name}_{datetime.now().strftime("%H%M%S%f")}.parquet'
    s3.upload_file(file_path, bucket, s3_key)
    os.remove(file_path)
    print(f'✅ Uploaded {len(records)} records to s3://{bucket}/{s3_key}')

# Batch consume: flush a topic when it reaches batch_size OR every flush_interval seconds
batch_size = int(os.getenv("BATCH_SIZE", "50"))
flush_interval = int(os.getenv("FLUSH_INTERVAL", "8"))  # seconds
buffer = defaultdict(list)
last_flush = time.time()


def flush_all():
    for topic, records in buffer.items():
        if records:
            write_to_minio(topic.split('.')[-1], records)
            buffer[topic] = []


print(f"✅ Connected to Kafka. Listening on topics: {', '.join(TOPICS)}")

try:
    while True:
        batches = consumer.poll(timeout_ms=1000)
        for _tp, messages in batches.items():
            for message in messages:
                event = message.value
                payload = event.get("payload", {})
                record = payload.get("after")  # Only take the actual row
                if record:
                    buffer[message.topic].append(record)
                    print(f"[{message.topic}] -> {record}")  # Debugging
                if len(buffer[message.topic]) >= batch_size:
                    write_to_minio(message.topic.split('.')[-1], buffer[message.topic])
                    buffer[message.topic] = []

        # Time-based flush so small batches still land in MinIO
        if time.time() - last_flush >= flush_interval:
            flush_all()
            last_flush = time.time()
except KeyboardInterrupt:
    print("\nFlushing remaining records before exit...")
    flush_all()
