from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

#DAG
with DAG(
    dag_id="example_elt_pipeline",
    default_args=default_args,
    description="ETL process load example data from generated data into PortgresDB",
    start_date=datetime(2025, 8, 26),
    catchup=False,
    tags=["example", "etl", "database"],
) as dag:

    # task_generate_data = BashOperator(
    #     task_id="generate_data",
    #     bash_command="python /opt/airflow/data/sampledata.py",
    # )

    task_load_data = BashOperator(
        task_id="load_data",
        bash_command="python /opt/airflow/dags/scripts/load_data.py",
        execution_timeout=timedelta(minutes=30),
    )

    # Task
    task_load_data
    # task_generate_data.set_downstream(task_load_data)
