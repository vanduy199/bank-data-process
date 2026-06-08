import os
import json
import requests
from dotenv import load_dotenv
from pathlib import Path

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv(Path(__file__).resolve().parent / ".env")

# -----------------------------
# Build connector JSON in memory
# -----------------------------
# Tables to capture, built from CDC_TABLES (default includes the service-usage flow)
tables = [
    t.strip()
    for t in os.getenv(
        "CDC_TABLES", "customers,accounts,transactions,services,service_usage"
    ).split(",")
    if t.strip()
]
table_include_list = ",".join(f"public.{t}" for t in tables)

connector_config = {
    "name": "postgres-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": os.getenv("POSTGRES_HOST"),
        "database.port": os.getenv("POSTGRES_PORT"),
        "database.user": os.getenv("POSTGRES_USER"),
        "database.password": os.getenv("POSTGRES_PASSWORD"),
        "database.dbname": os.getenv("POSTGRES_DB"),
        "database.sslmode": os.getenv("POSTGRES_SSLMODE", "disable"),
        "topic.prefix": "banking_server",
        "table.include.list": table_include_list,
        "plugin.name": "pgoutput",
        "slot.name": "banking_slot",
        "publication.autocreate.mode": "filtered",
        "tombstones.on.delete": "false",
        "decimal.handling.mode": "double",
    },
}

# -----------------------------
# Send request to Debezium Connect
# -----------------------------
url = "http://localhost:8083/connectors"
headers = {"Content-Type": "application/json"}

response = requests.post(url, headers=headers, data=json.dumps(connector_config))

# -----------------------------
# Debug/Output
# -----------------------------
if response.status_code == 201:
    print("✅ Connector created successfully!")
elif response.status_code == 409:
    print("⚠️ Connector already exists.")
else:
    print(f"❌ Failed to create connector ({response.status_code}): {response.text}")
