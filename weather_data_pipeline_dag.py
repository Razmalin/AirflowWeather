from airflow import DAG
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from tasks.download_data import download_data
from tasks.process_data import process_data
from tasks.save_data import save_data

with DAG(
        dag_id="first_dag",
        schedule_interval="@daily",
        default_args={
            "owners": "airflow",
            "retries": 1,
            "owners": timedelta(minutes=5),
            "start_date": datetime(2024, 5, 1)
        },
        catchup=False
    ) as dag:
    download_data() >> process_data() >> save_data()