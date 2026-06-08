#!/usr/bin/env bash
# =============================================================================
# run_pipeline.sh — chạy toàn bộ luồng "tần suất sử dụng dịch vụ" end-to-end:
#
#   Generator -> Local Postgres -> Debezium -> Kafka -> Consumer -> MinIO
#       -> load -> Snowflake RAW -> dbt (staging -> snapshot -> marts) -> result
#
# Cách dùng:
#   ./run_pipeline.sh              # chạy full (infra -> ingest -> warehouse)
#   ./run_pipeline.sh ingest       # chỉ tới MinIO (bỏ Snowflake/dbt)
#   ./run_pipeline.sh warehouse    # chỉ chặng load + dbt (giả định MinIO đã có data)
#   ./run_pipeline.sh infra        # chỉ bật hạ tầng + đăng ký connector
#
# Biến môi trường tuỳ chọn:
#   GEN_RUNS=3        số lần chạy generator --once (mặc định 1)
#   DRAIN_SECONDS=20  thời gian cho consumer chạy để gom + flush (mặc định 20)
#   VENV=.venv        đường dẫn virtualenv
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")"

VENV="$(realpath "${VENV:-.venv}" 2>/dev/null || echo "$PWD/.venv")"
PY="$VENV/bin/python"
DBT="$VENV/bin/dbt"
GEN_RUNS="${GEN_RUNS:-1}"
DRAIN_SECONDS="${DRAIN_SECONDS:-20}"

c() { printf "\n\033[1;36m=== %s ===\033[0m\n" "$*"; }
ok() { printf "\033[1;32m✓ %s\033[0m\n" "$*"; }
die() { printf "\033[1;31m✗ %s\033[0m\n" "$*" >&2; exit 1; }

[ -x "$PY" ] || die "Không tìm thấy $PY — tạo venv & pip install -r requirement.txt trước."
[ -f .env ] || die "Thiếu file .env ở gốc dự án."

start_infra() {
  c "Bật hạ tầng (zookeeper, kafka, connect, postgres, minio)"
  docker compose up -d zookeeper kafka connect postgres minio

  c "Chờ Postgres sẵn sàng"
  for i in $(seq 1 30); do
    docker compose exec -T postgres pg_isready -U postgres >/dev/null 2>&1 && { ok "Postgres ready"; break; }
    sleep 2; [ "$i" = 30 ] && die "Postgres không sẵn sàng."
  done

  c "Đảm bảo schema (idempotent)"
  docker compose exec -T postgres psql -U postgres -d banking -f /docker-entrypoint-initdb.d/01_create_table.sql >/dev/null 2>&1 \
    && ok "Schema OK" || echo "  (schema có thể đã tồn tại)"

  c "Chờ Kafka Connect REST"
  for i in $(seq 1 40); do
    curl -sf http://localhost:8083/ >/dev/null 2>&1 && { ok "Connect ready"; break; }
    sleep 2; [ "$i" = 40 ] && die "Kafka Connect không sẵn sàng."
  done

  c "Đăng ký Debezium connector (xoá cũ nếu có)"
  curl -s -X DELETE http://localhost:8083/connectors/postgres-connector >/dev/null 2>&1 || true
  sleep 2
  "$PY" kafka-debezium/generate_and_post_connector.py
  sleep 5
  state=$(curl -s http://localhost:8083/connectors/postgres-connector/status | "$PY" -c "import sys,json;print(json.load(sys.stdin)['tasks'][0]['state'])" 2>/dev/null || echo UNKNOWN)
  [ "$state" = "RUNNING" ] && ok "Connector task RUNNING" || die "Connector task = $state (xem: curl localhost:8083/connectors/postgres-connector/status)"
}

ingest() {
  c "Chạy consumer nền ${DRAIN_SECONDS}s (gom + flush vào MinIO)"
  "$PY" -u consumer/kafka_to_minio.py > /tmp/consumer.log 2>&1 &
  CONS_PID=$!
  sleep 2

  c "Sinh dữ liệu (generator --once x ${GEN_RUNS})"
  for n in $(seq 1 "$GEN_RUNS"); do
    "$PY" data-generator/fake_generator.py --once
  done

  c "Chờ consumer drain ${DRAIN_SECONDS}s rồi dừng"
  sleep "$DRAIN_SECONDS"
  kill -INT "$CONS_PID" 2>/dev/null || true
  wait "$CONS_PID" 2>/dev/null || true
  echo "--- uploads ---"; grep "Uploaded" /tmp/consumer.log || echo "(chưa có upload nào — kiểm tra /tmp/consumer.log)"
  ok "Ingestion xong"
}

warehouse() {
  if ! grep -qE '^SNOWFLAKE_USER=.+' .env; then
    echo "⚠️  Bỏ qua chặng warehouse: chưa điền SNOWFLAKE_* trong .env"
    return 0
  fi
  c "Nạp MinIO -> Snowflake RAW"
  "$PY" load_minio_to_snowflake.py

  c "Chạy dbt: staging -> snapshot -> marts -> test"
  ( cd banking_dbt
    set -a; . ../.env; set +a
    export DBT_PROFILES_DIR="$PWD/.dbt"
    "$DBT" run --select staging
    "$DBT" snapshot
    "$DBT" run --select marts
    "$DBT" test
  )
  ok "dbt xong"

  c "🎯 Bảng tần suất sử dụng dịch vụ (top 10)"
  set -a; . ./.env; set +a
  "$PY" - <<'PYEOF'
import os, snowflake.connector
c = snowflake.connector.connect(
    user=os.environ["SNOWFLAKE_USER"], password=os.environ["SNOWFLAKE_PASSWORD"],
    account=os.environ["SNOWFLAKE_ACCOUNT"], warehouse=os.getenv("SNOWFLAKE_WAREHOUSE","COMPUTE_WH"),
    database="BANKING", schema="ANALYTICS", role=os.getenv("SNOWFLAKE_ROLE","ACCOUNTADMIN"))
cur=c.cursor()
cur.execute("""SELECT usage_rank, service_name, category, total_uses, distinct_customers, pct_share
               FROM BANKING.ANALYTICS.agg_service_frequency ORDER BY usage_rank LIMIT 10""")
print(f"{'#':<3}{'SERVICE':<22}{'CATEGORY':<12}{'USES':>6}{'CUST':>6}{'%':>8}")
print("-"*60)
for r in cur.fetchall():
    print(f"{r[0]:<3}{r[1]:<22}{r[2]:<12}{r[3]:>6}{r[4]:>6}{float(r[5]):>7.1f}%")
cur.close(); c.close()
PYEOF
}

MODE="${1:-full}"
case "$MODE" in
  infra)     start_infra ;;
  ingest)    start_infra; ingest ;;
  warehouse) warehouse ;;
  full)      start_infra; ingest; warehouse ;;
  *) die "Mode không hợp lệ: $MODE (dùng: full | ingest | warehouse | infra)" ;;
esac

c "HOÀN TẤT (mode=$MODE)"
