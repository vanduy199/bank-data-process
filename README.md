# bank-data-process — Luồng "Tần suất sử dụng dịch vụ"

Pipeline dữ liệu ngân hàng end-to-end theo mô hình **Modern Data Stack**, đo **tần suất sử dụng các
dịch vụ ngân hàng**. Dữ liệu giả lập chảy realtime từ OLTP qua CDC vào data lake, nạp lên data
warehouse và biến đổi thành các bảng phân tích.

## 🏗️ Kiến trúc (8 chặng)

```
[1 Generator] → [2 Local Postgres] → [3 Debezium CDC] → [4 Kafka] → [5 Consumer]
   → [6 MinIO (parquet)] → [7 Snowflake RAW] → [8 dbt: staging → snapshot SCD2 → marts]
```

| Chặng | Công cụ | Vai trò |
|---|---|---|
| 1 | `data-generator/fake_generator.py` (Faker) | Sinh customers, accounts, transactions, **services, service_usage** vào Postgres |
| 2 | PostgreSQL 15 (`wal_level=logical`) | Hệ OLTP nguồn (system of record) |
| 3 | Debezium (`kafka-debezium/`) | Bắt thay đổi từ WAL (CDC) |
| 4 | Kafka | Message broker / buffer |
| 5 | `consumer/kafka_to_minio.py` | Gom batch → ghi Parquet |
| 6 | MinIO (S3) | Data lake (bucket `raw1`) |
| 7 | `load_minio_to_snowflake.py` / Airflow DAG | Nạp Parquet → Snowflake RAW (cột VARIANT `v`) |
| 8 | dbt (`banking_dbt/`) | staging → snapshot SCD2 (`services`) → marts tần suất |

**Bảng kết quả** (`BANKING.ANALYTICS`):
- `agg_service_frequency` — xếp hạng tần suất theo dịch vụ (total_uses, distinct_customers, pct_share, usage_rank)
- `agg_service_usage_daily` — tần suất theo ngày × kênh
- `agg_customer_service_freq` — tần suất theo khách × dịch vụ
- `fct_service_usage`, `dim_services`

---

## ✅ Yêu cầu

- Docker + Docker Compose
- Python 3.12 + virtualenv:
  ```bash
  python -m venv .venv && source .venv/bin/activate
  pip install -r requirement.txt
  ```
- (Tùy chọn, cho chặng 7–8) Tài khoản **Snowflake** (free trial là đủ)

---

## ⚙️ Cấu hình

Các file `.env` đã có sẵn giá trị mặc định cho local. Cần điền thêm **Snowflake** vào `.env` (gốc):

```dotenv
SNOWFLAKE_USER=<user>
SNOWFLAKE_PASSWORD=<password>
SNOWFLAKE_ACCOUNT=<orgname-accountname>   # chạy SQL bên dưới để lấy
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DB=BANKING
SNOWFLAKE_SCHEMA=RAW
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

Lấy `SNOWFLAKE_ACCOUNT` + `SNOWFLAKE_USER` — chạy trong Snowflake Worksheet:
```sql
SELECT CURRENT_USER() AS snowflake_user,
       CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS snowflake_account;
```

**Setup Snowflake (1 lần):** mở Worksheet, dán & Run toàn bộ `migration/snowflake_setup.sql`
(tạo DB `BANKING`, schema `RAW`/`ANALYTICS`, 5 RAW tables `(v VARIANT)`).

---

## 🚀 Chạy nhanh (1 lệnh)

```bash
./run_pipeline.sh            # full: hạ tầng → ingest → Snowflake → dbt → in bảng tần suất
```

Các mode khác:
```bash
./run_pipeline.sh infra      # chỉ bật hạ tầng + đăng ký Debezium connector
./run_pipeline.sh ingest     # tới MinIO (bỏ Snowflake/dbt)
./run_pipeline.sh warehouse  # chỉ load + dbt (giả định MinIO đã có data)
```

Biến tùy chọn:
```bash
GEN_RUNS=3 DRAIN_SECONDS=30 ./run_pipeline.sh ingest   # sinh 3 lần, consumer drain 30s
```

> Nếu chưa điền `SNOWFLAKE_*`, mode `full` tự bỏ qua chặng warehouse và dừng ở MinIO.

---

## 🔧 Chạy thủ công (để hiểu / debug)

```bash
# 1. Hạ tầng
docker compose up -d zookeeper kafka connect postgres minio

# 2. (Schema tự tạo khi data dir rỗng; nếu cần áp tay)
docker compose exec -T postgres psql -U postgres -d banking < migration/create_table.sql

# 3. Đăng ký Debezium connector (xoá cũ trước nếu cần đổi config)
curl -s -X DELETE http://localhost:8083/connectors/postgres-connector
python kafka-debezium/generate_and_post_connector.py
curl -s http://localhost:8083/connectors/postgres-connector/status   # task phải RUNNING

# 4. Consumer (terminal riêng, để chạy liên tục)
python consumer/kafka_to_minio.py

# 5. Sinh dữ liệu
python data-generator/fake_generator.py --once     # bỏ --once để chạy vòng lặp

# 6. Nạp MinIO → Snowflake RAW
python load_minio_to_snowflake.py

# 7. dbt
cd banking_dbt
export $(grep -E '^SNOWFLAKE_' ../.env | xargs)
export DBT_PROFILES_DIR="$PWD/.dbt"
dbt run --select staging && dbt snapshot && dbt run --select marts && dbt test
```

---

## 🔍 Kiểm tra & xem kết quả

- **MinIO Console:** http://localhost:9001 (user/pass `minioadmin`/`minioadmin`) → bucket `raw1`
- **Kafka topics:** `docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list`
- **Bảng tần suất (Snowflake):**
  ```sql
  SELECT service_name, total_uses, distinct_customers, pct_share, usage_rank
  FROM BANKING.ANALYTICS.agg_service_frequency ORDER BY usage_rank;
  ```

---

## 🩹 Troubleshooting (các lỗi đã gặp)

| Triệu chứng | Nguyên nhân | Cách sửa |
|---|---|---|
| Consumer in "Connected" nhưng MinIO rỗng, không có consumer group | `host.docker.internal` không resolve trên Linux | Đã đổi `KAFKA_ADVERTISED_LISTENERS` → `localhost:29092` trong `docker-compose.yml` |
| Connector lỗi `CREATE_REPLICATION_SLOT` | Connector cũ còn trỏ Supabase pooler (POST lại bị 409, không update) | `curl -X DELETE .../connectors/postgres-connector` rồi đăng ký lại |
| MinIO chỉ có vài prefix | Consumer chỉ flush khi đủ 50 record/topic | Consumer đã thêm flush theo thời gian (`FLUSH_INTERVAL=8s`); hoặc chạy generator nhiều lần |
| Postgres "Skipping initialization", không tạo bảng | Data dir cũ không rỗng | `docker compose stop postgres && sudo rm -rf docker/postgres/data && docker compose up -d postgres` |
| `Stage '%SERVICES' does not exist` khi load | Thiếu RAW table | Chạy `migration/snowflake_setup.sql` |
| EACCES khi sửa `banking_dbt/` hoặc `docker/dags/` | Thư mục do Docker tạo thuộc root | `sudo chown -R $USER:$USER banking_dbt docker/dags` |

---

## 🧹 Dừng / dọn

```bash
docker compose down                 # dừng container
docker compose down -v              # + xoá volume (airflow metadata)
sudo rm -rf docker/postgres/data    # reset Postgres (cần init lại schema)
```

---

## 📂 Cấu trúc

```
data-generator/        # Faker → Postgres (services, service_usage, ...)
kafka-debezium/        # Debezium connector (CDC từ Postgres WAL)
consumer/              # Kafka → MinIO (parquet, flush theo batch + thời gian)
migration/             # DDL Postgres + setup Snowflake
banking_dbt/           # dbt project (staging, snapshots SCD2, marts tần suất)
docker/dags/           # Airflow DAGs (minio→snowflake, dbt) — orchestration tùy chọn
load_minio_to_snowflake.py   # loader CLI (bản standalone của Airflow DAG)
run_pipeline.sh        # chạy toàn bộ luồng
docker-compose.yml     # hạ tầng: kafka, connect, postgres, minio, airflow
```
