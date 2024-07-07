from datetime import datetime, timedelta

API_KEY = 'aa95d6041c835891fc5f94ccf2a6fdbf'
CITY = 'Moscow'
URL = f'http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}