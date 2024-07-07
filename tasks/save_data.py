import pandas as pd
from airflow.decorators import task


@task(task_id="save_data")
def save_data() -> None:
    df = pd.read_csv("/tmp/processed_weather_data.csv")
    df.to_parquet('/tmp/weather.parquet', index=False)