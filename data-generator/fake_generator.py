import time
import psycopg2
from decimal import Decimal, ROUND_DOWN
from datetime import datetime, timedelta, timezone
from faker import Faker
import random
import argparse
import sys
import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).resolve().parent / ".env")

# -----------------------------
# Project configuration (safe to hardcode here)
# -----------------------------
NUM_CUSTOMERS = 10
ACCOUNTS_PER_CUSTOMER = 2
NUM_TRANSACTIONS = 50
NUM_SERVICE_USAGE = 80
MAX_TXN_AMOUNT = 1000.00
CURRENCY = "USD"

# Banking service catalog (seeded once). (service_code, service_name, category)
SERVICE_CATALOG = [
    ("TRANSFER_INTERNAL", "Internal Transfer", "TRANSFERS"),
    ("TRANSFER_EXTERNAL", "External Transfer", "TRANSFERS"),
    ("BILL_PAYMENT", "Bill Payment", "PAYMENTS"),
    ("MOBILE_TOPUP", "Mobile Top-up", "PAYMENTS"),
    ("CARD_PAYMENT", "Card Payment", "CARDS"),
    ("CARD_ISSUE", "Card Issuance", "CARDS"),
    ("BALANCE_INQUIRY", "Balance Inquiry", "INQUIRY"),
    ("STATEMENT_REQUEST", "Statement Request", "INQUIRY"),
    ("LOAN_INQUIRY", "Loan Inquiry", "LOANS"),
    ("LOAN_REPAYMENT", "Loan Repayment", "LOANS"),
]
CHANNELS = ["WEB", "MOBILE", "ATM", "BRANCH", "CALL_CENTER"]
SERVICE_STATUSES = ["SUCCESS", "SUCCESS", "SUCCESS", "FAILED"]  # weighted ~75% success

# Non-zero initial balances
INITIAL_BALANCE_MIN = Decimal("10.00")
INITIAL_BALANCE_MAX = Decimal("1000.00")

# Loop config
DEFAULT_LOOP = True
SLEEP_SECONDS = 2

# CLI override (run once mode)
parser = argparse.ArgumentParser(description="Run fake data generator")
parser.add_argument("--once", action="store_true", help="Run a single iteration and exit")
args = parser.parse_args()
LOOP = not args.once and DEFAULT_LOOP

# -----------------------------
# Helpers
# -----------------------------
fake = Faker()

def random_money(min_val: Decimal, max_val: Decimal) -> Decimal:
    val = Decimal(str(random.uniform(float(min_val), float(max_val))))
    return val.quantize(Decimal("0.01"), rounding=ROUND_DOWN)

def seed_services():
    """Insert the fixed service catalog once (idempotent) and return service ids."""
    for code, name, category in SERVICE_CATALOG:
        cur.execute(
            "INSERT INTO services (service_code, service_name, category) "
            "VALUES (%s, %s, %s) ON CONFLICT (service_code) DO NOTHING",
            (code, name, category),
        )
    cur.execute("SELECT id FROM services")
    ids = [row[0] for row in cur.fetchall()]
    print(f"✅ Seeded service catalog ({len(ids)} services).")
    return ids

# -----------------------------
# Connect to Postgres
# -----------------------------
conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
)
conn.autocommit = True
cur = conn.cursor()

# -----------------------------
# Core generation logic (one iteration)
# -----------------------------
def run_iteration():
    customers = []
    # 1. Generate customers
    for _ in range(NUM_CUSTOMERS):
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = fake.unique.email()

        cur.execute(
            "INSERT INTO customers (first_name, last_name, email) VALUES (%s, %s, %s) RETURNING id",
            (first_name, last_name, email),
        )
        customer_id = cur.fetchone()[0]
        customers.append(customer_id)

    # 2. Generate accounts
    accounts = []
    for customer_id in customers:
        for _ in range(ACCOUNTS_PER_CUSTOMER):
            account_type = random.choice(["SAVINGS", "CHECKING"])
            initial_balance = random_money(INITIAL_BALANCE_MIN, INITIAL_BALANCE_MAX)
            cur.execute(
                "INSERT INTO accounts (customer_id, account_type, balance, currency) VALUES (%s, %s, %s, %s) RETURNING id",
                (customer_id, account_type, initial_balance, CURRENCY),
            )
            account_id = cur.fetchone()[0]
            accounts.append(account_id)

    # 3. Generate transactions
    txn_types = ["DEPOSIT", "WITHDRAWAL", "TRANSFER"]
    for _ in range(NUM_TRANSACTIONS):
        account_id = random.choice(accounts)
        txn_type = random.choice(txn_types)
        amount = round(random.uniform(1, MAX_TXN_AMOUNT), 2)
        related_account = None
        if txn_type == "TRANSFER" and len(accounts) > 1:
            related_account = random.choice([a for a in accounts if a != account_id])

        cur.execute(
            "INSERT INTO transactions (account_id, txn_type, amount, related_account_id, status) VALUES (%s, %s, %s, %s, 'COMPLETED')",
            (account_id, txn_type, amount, related_account),
        )

    # 4. Generate service-usage events ("service usage frequency" source)
    if SERVICE_IDS:
        for _ in range(NUM_SERVICE_USAGE):
            customer_id = random.choice(customers)
            account_id = random.choice(accounts) if random.random() < 0.8 else None
            service_id = random.choice(SERVICE_IDS)
            channel = random.choice(CHANNELS)
            status = random.choice(SERVICE_STATUSES)
            # Spread used_at over the last 14 days to get a realistic frequency trend
            used_at = datetime.now(timezone.utc) - timedelta(
                days=random.randint(0, 14),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
            )
            cur.execute(
                "INSERT INTO service_usage (customer_id, account_id, service_id, channel, status, used_at) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (customer_id, account_id, service_id, channel, status, used_at),
            )

    # 5. Generate customer_profiles + 6. rule-based customer_segments (Customer 360 flow)
    for customer_id in customers:
        total_txns = random.randint(0, 500)
        total_transfer = random_money(Decimal("0.00"), Decimal("60000.00"))
        avg_txn = random_money(Decimal("10.00"), Decimal("1500.00"))
        pref_type = random.choice(txn_types)
        login_freq = random.randint(0, 60)
        fav_feature = random.choice(
            ["dashboard", "transfer_page", "bill_payment", "account_settings"]
        )
        last_active = datetime.now(timezone.utc) - timedelta(
            days=random.randint(0, 60),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
        )
        risk = random_money(Decimal("0.00"), Decimal("100.00"))

        cur.execute(
            "INSERT INTO customer_profiles "
            "(customer_id, total_transactions, total_transfer_amount, avg_transaction_amount, "
            " preferred_transaction_type, login_frequency, favorite_feature, last_active_date, risk_score) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (customer_id, total_txns, total_transfer, avg_txn, pref_type,
             login_freq, fav_feature, last_active, risk),
        )

        # Rule-based segmentation (priority order)
        days_inactive = (datetime.now(timezone.utc) - last_active).days
        segment_name, segment_score = "Standard User", Decimal("50.00")
        if risk > Decimal("80.00"):
            segment_name, segment_score = "Risky User", risk
        elif days_inactive > 30:
            segment_name, segment_score = "Dormant User", Decimal("90.00")
        elif total_txns > 200 and total_transfer > Decimal("30000.00"):
            segment_name, segment_score = "VIP", Decimal("95.00")
        elif login_freq > 20:
            segment_name, segment_score = "Active User", Decimal("85.00")
        elif pref_type == "TRANSFER" and total_transfer > Decimal("10000.00"):
            segment_name, segment_score = "Transfer Heavy User", Decimal("75.00")

        cur.execute(
            "INSERT INTO customer_segments (customer_id, segment_name, segment_score) "
            "VALUES (%s, %s, %s)",
            (customer_id, segment_name, segment_score),
        )

    print(
        f"✅ Generated {len(customers)} customers (with profiles & segments), "
        f"{len(accounts)} accounts, {NUM_TRANSACTIONS} transactions, "
        f"{NUM_SERVICE_USAGE} service-usage events."
    )

# -----------------------------
# Seed service catalog once (before the loop)
# -----------------------------
SERVICE_IDS = seed_services()

# -----------------------------
# Main loop
# -----------------------------
try:
    iteration = 0
    while True:
        iteration += 1
        print(f"\n--- Iteration {iteration} started ---")
        run_iteration()
        print(f"--- Iteration {iteration} finished ---")
        if not LOOP:
            break
        time.sleep(SLEEP_SECONDS)

except KeyboardInterrupt:
    print("\nInterrupted by user. Exiting gracefully...")

finally:
    cur.close()
    conn.close()
    sys.exit(0)
