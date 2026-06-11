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
    # The dbt project dir is bind-mounted from the host (owned by the host user),
    # so the airflow user (uid 50000) cannot write logs/target there. Redirect both
    # to container-writable paths under /tmp.
    return (
        f"cd {DBT_DIR} && "
        f"DBT_LOG_PATH=/tmp/dbt_logs DBT_TARGET_PATH=/tmp/dbt_target "
        f"dbt {cmd} --profiles-dir {PROFILES_DIR}"
    )


with DAG(
    dag_id="banking_dbt",
    default_args=default_args,
    description="dbt for 3 flows: service usage + transactions + customer 360 (staging -> snapshot -> intermediate/marts)",
    schedule_interval="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["dbt", "service_usage", "transactions", "customer_360"],
) as dag:

    # 1. Build staging views (snapshots/intermediate/marts depend on them)
    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=dbt("run --select staging"),
    )

    # 2. SCD2 snapshots (services, customers, accounts, customer profiles/segments)
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=dbt("snapshot"),
    )

    # 3. Intermediate (customer 360 features) + all marts
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=dbt("run --select intermediate marts"),
    )

    # 4. Data tests
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=dbt("test"),
    )

    dbt_staging >> dbt_snapshot >> dbt_run_marts >> dbt_test
