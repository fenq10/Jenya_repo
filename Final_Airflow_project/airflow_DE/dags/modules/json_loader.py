import json
import pandas as pd
import dill
import os
import traceback
from modules.main import insert_sessions, insert_hits

def json_load():
    #path = os.environ.get('PROJECT_PATH', '.')
    #path = r'C:\Users\epsokolo\PycharmProjects\FINAL_Project'
    path = '/opt/airflow/dags'

    with open(f'{path}/pickls/pipiline_sessions.pkl', 'rb') as file:
        model_sessions = dill.load(file)

    with open(f'{path}/pickls/pipiline_hits.pkl', 'rb') as file:
        model_hits = dill.load(file)

    #циклы для jsonoв - загрузка, обработка
    for file in os.listdir(f'{path}/data/json/ga_sessions'):
        #для обращения к словарю по ключу
        file_key = file.replace('ga_sessions_new_','')
        file_key = file_key.replace('.json','')

        with open(f'{path}/data/json/ga_sessions/{file}') as jsfile:
            form = json.load(jsfile)
            df = pd.DataFrame(form[file_key] )
            try:
                new_df_s = model_sessions['model'].transform(df)
                insert_sessions(new_df_s)
                print(f'insert {file} completed')
            except:
                print(f'Error at {file}:', traceback.format_exc())
                #print('Error')

    for file in os.listdir(f'{path}/data/json/ga_hits'):
        #для обращения к словарю по ключу
        file_key = file.replace('ga_hits_new_','')
        file_key = file_key.replace('.json','')

        with open(f'{path}/data/json/ga_hits/{file}') as jsfile:
            form = json.load(jsfile)
            df = pd.DataFrame(form[file_key] )
            try:
                new_df_h = model_hits['model'].transform(df)
                insert_hits(new_df_h)
                print(f'insert {file} completed')
            except:
                print(f'Error at {file}:', traceback.format_exc())
                #print('Error')

if __name__ == '__main__':
    json_load()