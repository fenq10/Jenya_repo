import dill
import pandas as pd
import psycopg2
import os

path = os.environ.get('PROJECT_PATH', '.')

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
    #сам инсерт
    cursor = conn.cursor()
    cursor.executemany("INSERT INTO ga_sessions VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                       sql_data_sessions)
    conn.commit()
    conn.close()

def insert_hits(data):
    # подключаемся к БД
    conn = psycopg2.connect(database="airflow",
                            user="airflow",
                            password="airflow",
                            host="postgres",  #""
                            port=5432)
    #переменная для инсерта
    sql_data_hits = tuple(map(tuple, data.values))
    #сам инсерт
    cursor = conn.cursor()
    cursor.executemany("INSERT INTO ga_hits VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)", sql_data_hits)

    conn.commit()
    conn.close()


def main():
    #загружаем пиклы с пайплайнами обработки
    with open(f'{path}/pickls/pipiline_sessions.pkl', 'rb') as file:
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