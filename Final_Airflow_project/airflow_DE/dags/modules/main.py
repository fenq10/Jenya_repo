import dill
import pandas as pd
import psycopg2
import os

#path = '/home/airflow'
#path = os.environ.get('PROJECT_PATH', '.') #'/home/airflow/airflow-postgre/pickls/pipiline_sessions.pkl'
#path = os.getcwd() #'/opt/airflow/pickls/pipiline_sessions.pkl'
#path = './airflow-postgre' #'./airflow-postgre/pickls/pipiline_sessions.pkl'
#path = '~/airflow-postgre' #'~/airflow-postgre/pickls/pipiline_sessions.pkl'
#path = '/opt/airflow'
#path = '/mnt/c/Users/epsokolo/airflow_hw'
path = '/opt/airflow/dags'

#функции для инсерта
def insert_sessions(data):
    # подключаемся к БД
    conn = psycopg2.connect(database="airflow",
                            user="airflow",
                            password="airflow",
                            host="postgres",  #""
                            port=5432)
    #переменная для инсерта
    sql_data_sessions = tuple(map(tuple, data.values))
    #предварительно почистим
    cursor = conn.cursor()
    cursor.execute('delete from ga_sessions')
    conn.commit()
    #сам инсерт
    cursor.executemany("INSERT INTO ga_sessions VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                       sql_data_sessions)
    conn.commit()
    conn.close()
    print(f'insert completed {len(data)}')

def insert_hits(data):
    # подключаемся к БД
    conn = psycopg2.connect(database="airflow",
                            user="airflow",
                            password="airflow",
                            host="postgres",  #""
                            port=5432)
    #переменная для инсерта
    sql_data_hits = tuple(map(tuple, data.values))

    cursor = conn.cursor()
    #предварительно почистим
    cursor.execute('delete from ga_hits')
    conn.commit()
    #сам инсерт
    cursor.executemany("INSERT INTO ga_hits VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)", sql_data_hits)

    conn.commit()
    conn.close()
    print(f'insert completed {len(data)}')


def main():
    #загружаем пиклы с пайплайнами обработки
    with open(f'{path}/pickls/pipiline_sessions.pkl', 'rb') as file:
    #with open(f'{path}/pickls/sessions.pkl', 'rb') as file:
        model_sessions = dill.load(file)

    with open(f'{path}/pickls/pipiline_hits.pkl', 'rb') as file:
        model_hits = dill.load(file)

    # обрабатываем исходники по пайплайнам и грузим в БД
    with open(f'{path}/data/csv/ga_sessions.csv') as fin:
        df = pd.read_csv(fin, nrows=100)
        new_df_s = model_sessions['model'].transform(df)
        # инсерт данных
        insert_sessions(new_df_s)


    with open(f'{path}/data/csv/ga_hits.csv') as fin:
        df = pd.read_csv(fin, nrows=100)
        new_df_h = model_hits['model'].transform(df)
        # инсерт данных
        insert_hits(new_df_h)

if __name__ == '__main__':
    main()