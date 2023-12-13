import datetime as dt
import os
import sys

from airflow.models import DAG
from airflow.operators.python import PythonOperator
#from airflow.operators.bash import BashOperator


#path = os.path.expanduser('~/airflow-postgre')
#path = os.path.expanduser('~/airflow_hw')
path = os.path.expanduser('~/airflow_DE/dags')


# Добавим путь к коду проекта в переменную окружения, чтобы он был доступен python-процессу
os.environ['PROJECT_PATH'] = path
# Добавим путь к коду проекта в $PATH, чтобы импортировать функции
sys.path.insert(0, path)

from modules.main import main
#from modules.json_loader import json_load
# <YOUR_IMPORTS>

#прробуем запустить разово код по загрузке стартовых данных
#main()

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2023, 11, 6),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=10),
    'depends_on_past': False,
}

with DAG(
        dag_id='DB_Dag',
        schedule = None,
        #schedule_interval="0 0 1 1 *", #yearly
        #schedule_interval='0 0 1 1 *',
        default_args=args,
) as dag:
    DB_insert= PythonOperator(
        task_id='DB_insert',
        python_callable=main,
    )

    DB_insert
    # <YOUR_CODE>

