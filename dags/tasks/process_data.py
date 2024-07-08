from datetime import datetime
import pandas as pd
import json
from configuration import CITY
from airflow.decorators import task

"""
Этот модуль определяет вторую задачу для DAG Airflow,
которая реализует очистку и преобразование данных полученных в
ходе выполнения предыдущей задачи.

Функции:
    process_data(**kwargs): производит очистку и преобразование 
    данных, а также сохраняет их в csv-файл.

Использование:
    Этот модуль предназначен для использования в качестве задачи в DAG Airflow.
"""

@task(task_id="process_data")
def process_data(**kwargs) -> None:
    try:
        # Открытие и чтение JSON файла
        with open("/tmp/weather_data.json", 'r') as file:
            data_json = json.load(file)
    except FileNotFoundError as e:
        print(f"Error: File not found. {e}")
        return
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return
    except IOError as e:
        print(f"Error reading file: {e}")
        return
    
    main_data_json = data_json['main']
    weather_main = data_json["weather"][0]["main"]
    weather_description = data_json["weather"][0]["description"]

    weather = f'{weather_main} ({weather_description})'
    temp_celsius = round(main_data_json['temp'] - 273.15, 1)
    pressure = main_data_json['pressure']
    humidity = main_data_json['humidity']

    processed_data = [{
        "city": CITY,
        "date": datetime.now().isoformat(),
        "weather": weather,
        "temperature_celsius": temp_celsius,
        "pressure": pressure,
        "humidity": humidity,
    }]

    try:
        # Создание DataFrame
        dataframe = pd.DataFrame(processed_data)
        dataframe.to_csv("/tmp/processed_weather_data.csv", index=False)
    except IOError as e:
        print(f"Error writing to file: {e}")
        return