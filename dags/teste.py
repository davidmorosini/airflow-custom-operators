from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


dag = DAG(
    dag_id="teste",
    start_date=datetime(2022, 1, 1, 0, 0, 0),
    schedule_interval=None
)


def func(**kwargs):
    return "teste"

with dag:

    PythonOperator(
        task_id="teste-task",
        python_callable=func
    )
