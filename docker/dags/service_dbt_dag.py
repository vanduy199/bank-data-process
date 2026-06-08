from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

DBT_DIR = "/opt/airflow/banking_dbt"
PROFILES_DIR = "/home/airflow/.dbt"


def dbt(cmd: str) -> str:
    return f"cd {DBT_DIR} && dbt {cmd} --profiles-dir {PROFILES_DIR}"


with DAG(
    dag_id="service_usage_dbt",
    default_args=default_args,
    description="Run dbt staging + SCD2 snapshot + frequency marts for service usage",
    schedule_interval="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["dbt", "service_usage"],
) as dag:

    # 1. Build staging views (snapshots/marts depend on them)
    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=dbt("run --select staging"),
    )

    # 2. SCD2 snapshot of the service catalog
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=dbt("snapshot"),
    )

    # 3. Frequency marts (dim/fct/agg)
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=dbt("run --select marts"),
    )

    # 4. Data tests
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=dbt("test"),
    )

    dbt_staging >> dbt_snapshot >> dbt_run_marts >> dbt_test
