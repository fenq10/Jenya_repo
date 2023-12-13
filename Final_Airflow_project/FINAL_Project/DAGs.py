import datetime as dt
import os
import sys

from airflow.models import DAG
from airflow.operators.python import PythonOperator


path = os.path.expanduser('~/airflow_hw')
# Добавим путь к коду проекта в переменную окружения, чтобы он был доступен python-процессу
os.environ['PROJECT_PATH'] = path
# Добавим путь к коду проекта в $PATH, чтобы импортировать функции
sys.path.insert(0, path)

from main import main
from json_loader import json_load
# <YOUR_IMPORTS>

#прробуем запустить разово код по загрузке стартовых данных
main()

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2022, 6, 10),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=10),
    'depends_on_past': False,
}

with DAG(
        dag_id='car_price_prediction',
        schedule_interval="00 30 * * *",
        default_args=args,
) as dag:
    # pipeline = PythonOperator(
    #     task_id='pipeline',
    #     python_callable=main,
    # )
    js = PythonOperator(
        task_id='predict',
        python_callable=json_load,
    )

    js
    # <YOUR_CODE>

