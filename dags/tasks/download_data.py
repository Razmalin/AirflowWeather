import requests
import json
import os
from configuration import URL
from airflow.decorators import task

"""
Этот модуль определяет первую задачу для DAG Airflow,
которая реализует получение данных о погоде из API OpenWeatherMap.

Функции:
    download_data(**kwargs): получает данные о погоде из API OpenWeatherMap
    и сохраняет их в json-файл.

Использование:
    Этот модуль предназначен для использования в качестве задачи в DAG Airflow.
"""

@task(task_id="download_data")
def download_data(**kwargs) -> None:
    file_path = os.path.join(os.path.dirname(__file__), 'weather_data.json')
    print(file_path)
    try:
        # Попытка получить данные с указанного URL
        response = requests.get(URL)
        response.raise_for_status()  # Вызывает исключение для неудачного статуса HTTP
    except requests.exceptions.RequestException as e:
        # Обработка всех исключений, связанных с HTTP запросом
        print(f"Error downloading data: {e}")
        return
    
    # Декодирование JSON
    data_json = response.json()

    try:
        with open("/tmp/weather_data.json", 'w') as file:
            json.dump(data_json, file)
    except IOError as e:
        # Обработка ошибок при записи файла
        print(f"Error writing to file: {e}")
        return