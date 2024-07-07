from datetime import datetime
import pandas as pd
import json
from configuration import CITY
from airflow.decorators import task


@task(task_id="process_data")
def process_data() -> None:
    with open("/tmp/weather_data.json", 'r') as file:
        data_json = json.load(file)
    main_data_json = data_json["main"]

    weather = data_json["weather"]["main"] + f' ({data_json["weather"]["description"]})'
    temp_celsius = main_data_json['temp'] - 273.15
    pressure = main_data_json['pressure']
    humidity = main_data_json['humidity']

    process_data = {
        "city": CITY,
        "date": datetime.now().isoformat(),
        "weather": weather,
        "temperature_celsius": temp_celsius,
        "pressure": pressure,
        "humidity": humidity,
    }

    dataframe = pd.DataFrame(process_data)
    dataframe.to_csv("/tmp/processed_weather_data.csv", index=False)