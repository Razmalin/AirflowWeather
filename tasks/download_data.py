import requests
import json
from configuration import URL
from airflow.decorators import task


@task(task_id="download_data")
def download_data() -> None:
    response = requests.get(URL)
    data_json = response.json()
    with open("/tmp/weather_data.json", 'w') as file:
        json.dump(data_json, file)