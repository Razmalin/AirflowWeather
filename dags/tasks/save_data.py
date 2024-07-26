import pandas as pd
from airflow.decorators import task

"""
Этот модуль определяет третью задачу для DAG Airflow,
которая реализует преобразование данных из csv в parquet-файл.

Функции:
    save_data(**kwargs): сохраняет DataFrame, загруженный из 
    csv-файла в parquet-файл.

Использование:
    Этот модуль предназначен для использования в качестве задачи в DAG Airflow.
"""

@task(task_id="save_data")
def save_data(**kwargs) -> None:
    try:
        # Попытка прочитать CSV файл
        df = pd.read_csv("/tmp/processed_weather_data.csv")
    except IOError as e:
        print(f"Error reading file: {e}")
        return
    
    try:
        # Попытка сохранить DataFrame в Parquet файл
        df.to_parquet('/tmp/weather.parquet', index=False)
    except IOError as e:
        print(f"Error writing to file: {e}")
        return