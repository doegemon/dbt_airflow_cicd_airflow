from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
import os
from pendulum import datetime

profile_config = ProfileConfig(
    profile_name="dbt_airflow_dw",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="docker_postgres_db",
        profile_args={"schema": "public"},
    ),
)

my_cosmos_dag = DbtDag(
    project_config=ProjectConfig(
        dbt_project_path="/usr/local/airflow/dbt/dbt_airflow_dw",
        project_name="dbt_airflow_dw",
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
    ),
    operator_args={
        "install_deps": True,
    },
    schedule="@daily",
    start_date=datetime(2025, 9, 25),
    catchup=False,
    dag_id=f"dag_airflow_dw",
    default_args={"retries": 2},
)
