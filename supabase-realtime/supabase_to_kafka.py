import asyncio
import json
import os
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaProducer
from supabase import AsyncClient, acreate_client


load_dotenv(Path(__file__).resolve().parent / ".env")


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


SUPABASE_URL = require_env("SUPABASE_URL")
SUPABASE_KEY = require_env("SUPABASE_KEY")
SUPABASE_SCHEMA = os.getenv("SUPABASE_SCHEMA", "public")
SUPABASE_TABLES = [
    table.strip()
    for table in os.getenv("SUPABASE_TABLES", "customers,accounts,transactions").split(",")
    if table.strip()
]
KAFKA_BOOTSTRAP = require_env("KAFKA_BOOTSTRAP")
KAFKA_TOPIC_PREFIX = os.getenv("KAFKA_TOPIC_PREFIX", "banking_server")


def json_default(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return str(value)


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda value: json.dumps(value, default=json_default).encode("utf-8"),
)


def normalize_payload(payload: dict) -> dict:
    data = payload.get("data", payload)
    event_type = data.get("eventType") or data.get("event") or data.get("type")
    table = data.get("table")
    schema = data.get("schema") or SUPABASE_SCHEMA
    new_record = data.get("new") or data.get("record")
    old_record = data.get("old") or data.get("old_record")

    op = {
        "INSERT": "c",
        "UPDATE": "u",
        "DELETE": "d",
    }.get(str(event_type).upper(), "r")

    return {
        "payload": {
            "op": op,
            "source": {
                "connector": "supabase-realtime",
                "schema": schema,
                "table": table,
            },
            "ts_ms": int(datetime.now().timestamp() * 1000),
            "before": old_record,
            "after": None if op == "d" else new_record,
        }
    }


def publish_change(payload: dict) -> None:
    data = payload.get("data", payload)
    table = data.get("table")
    if not table:
        print(f"Skipped payload without table: {payload}")
        return
    if table not in SUPABASE_TABLES:
        return

    topic = f"{KAFKA_TOPIC_PREFIX}.{SUPABASE_SCHEMA}.{table}"
    message = normalize_payload(payload)
    producer.send(topic, message).get(timeout=10)
    print(f"Published {data.get('type') or data.get('eventType')} on {topic}: {message['payload']['after']}")


async def main() -> None:
    supabase: AsyncClient = await acreate_client(SUPABASE_URL, SUPABASE_KEY)
    await supabase.realtime.set_auth(SUPABASE_KEY)
    channel = supabase.channel("banking-db-changes")

    def on_subscribe(status, err):
        print(f"Supabase Realtime subscription status={status} err={err}")

    channel.on_postgres_changes(
        "*",
        schema=SUPABASE_SCHEMA,
        callback=publish_change,
    )

    await channel.subscribe(on_subscribe)
    print(
        "Listening to Supabase Realtime for "
        f"{SUPABASE_SCHEMA}.{', '.join(SUPABASE_TABLES)}; publishing to Kafka at {KAFKA_BOOTSTRAP}"
    )

    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    asyncio.run(main())
