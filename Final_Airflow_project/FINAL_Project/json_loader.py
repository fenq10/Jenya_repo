import json
import pandas as pd
import dill
import os
from main import insert_sessions, insert_hits

def json_load():
    path = os.environ.get('PROJECT_PATH', '.')
    #path = r'C:\Users\epsokolo\PycharmProjects\FINAL_Project'

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
            new_df_s = model_sessions['model'].transform(df)
            insert_sessions(new_df_s)

    for file in os.listdir(f'{path}/data/json/ga_hits'):
        #для обращения к словарю по ключу
        file_key = file.replace('ga_hits_new_','')
        file_key = file_key.replace('.json','')

        with open(f'{path}/data/json/ga_hits/{file}') as jsfile:
            form = json.load(jsfile)
            df = pd.DataFrame(form[file_key] )
            new_df_h = model_hits['model'].transform(df)
            insert_hits(new_df_h)

if __name__ == '__main__':
    json_load()