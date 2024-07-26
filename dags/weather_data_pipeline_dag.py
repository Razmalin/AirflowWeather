from airflow import DAG
from datetime import datetime
from tasks.download_data import download_data
from tasks.process_data import process_data
from tasks.save_data import save_data

"""
Этот модуль определяет DAG Airflow, состоящий из трех задач.

DAG выполняется ежедневно в полночь и состоит из следующих задач:
    - Download Data: загрузка данных о погоде из API OpenWeatherMap;
    - Process Data: очистка и преобразование загруженных данных;
    - Save Data: сохранение обработанных данных в файл parquet.
"""

with DAG(
        dag_id="weather_data_pipeline_dag",
        schedule_interval="@daily",
        default_args={
            "owner": "airflow",
            "retries": 1,
            "start_date": datetime(2024, 5, 1)
        },
        catchup=False
    ) as dag:
    download_data() >> process_data() >> save_data()