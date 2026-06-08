# Dockerfile-airflow
FROM apache/airflow:2.9.3

# Switch to airflow user first
USER airflow

# Install dbt + pipeline packages (boto3 for MinIO, snowflake connector via dbt-snowflake)
RUN pip install --no-cache-dir dbt-core dbt-snowflake boto3 python-dotenv