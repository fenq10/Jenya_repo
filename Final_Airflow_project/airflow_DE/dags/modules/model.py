import pandas as pd
import datetime as dt
import os
import dill
import datetime
import psycopg2

from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer

directory = os.getcwd()

def sessions_preparation(data):
# в ga_sessions самые существенные пропуски:
# столбец                   %пропусков    что делаем
# device_model                99.121633      эту колонку стоит удалить
# utm_keyword                 58.174009      удалим
# device_os                   57.533002      постараемся заполнить
# device_brand                19.740307      постараемся заполнить
# utm_adcontent               18.043410      оставим как есть
# utm_campaign                11.806346      оставим как есть
    data.device_os[(data.device_os.isna())
                          & (data.device_category.isin(['mobile', 'tablet']))
                          & (data.device_brand == 'Apple')] = 'iOS'

    data.device_os[(data.device_os.isna())
                          & (data.device_category.isin(['mobile', 'tablet']))
                          & (data.device_brand.isin(['Xiaomi', 'Samsung', 'Huawei']))] = 'Android'

    data.device_os[data.device_os.isna()] = '(not set)'

    data.device_brand[data.device_brand.isna()] = '(not set)'

    data['visit_datetime'] = pd.to_datetime(data.visit_date + ' ' + data.visit_time)

    return data


def hits_preparation(data):
# будет чистится специальной функцией пайплайна
# в ga_hits самые существенные пропуски:
# столбец            %пропусков    что делаем
# event_value       100.000000    удаляем
# hit_time           58.247795    среднее проставим
# hit_referer        39.899634    оставляем как есть
# event_label        23.909905    оставляем как есть
    data['hit_datetime'] = pd.to_datetime(data.hit_date) + data.hit_time.apply(
        lambda x: dt.timedelta(milliseconds=x))

    data.hit_time[data.hit_time.isna()] = data.hit_time.mean()
    return data


def drop_columns_sessions(data):
    columns_to_drop_sessions = ['device_model','utm_keyword','visit_date','visit_time']
    return data.drop(columns_to_drop_sessions, axis=1)

def drop_columns_hits(data):
    columns_to_drop_hits = ['event_value','hit_date','hit_time']
    return data.drop(columns_to_drop_hits, axis=1)


def sessions_dtypes(data):
    data.device_category = data.device_category.astype('category')
    data.device_os = data.device_os.astype('category')
    return data

def hits_dtypes(data):
    data.hit_type = data.hit_type.astype('category')
    data.event_category = data.event_category.astype('category')
    data.event_action = data.event_action.astype('category')
    return data

def main():

    columns_deleter_sessions = Pipeline(steps=[
        ('drop_columns_sessions', FunctionTransformer(drop_columns_sessions))
    ])
    columns_deleter_hits = Pipeline(steps=[
        ('drop_columns_hits', FunctionTransformer(drop_columns_hits))
    ])

    columns_preparation_sessions = Pipeline(steps=[
        ('sessions_preparation', FunctionTransformer(sessions_preparation))
    ])

    columns_preparation_hits = Pipeline(steps=[
       ('hits_preparation', FunctionTransformer(hits_preparation))
    ])

    columns_dtypes_sessions = Pipeline(steps=[
        ('sessions_dtypes', FunctionTransformer(sessions_dtypes))
    ])

    columns_dtypes_hits = Pipeline(steps=[
        ('hits_dtypes', FunctionTransformer(hits_dtypes))
    ])


    pipe_sessions = Pipeline(steps=[
                ('sessions_preparation', columns_preparation_sessions),
                ('session_drop_columns', columns_deleter_sessions),
                ('change_dtypes_sessions', columns_dtypes_sessions)
            ])

    pipe_hits = Pipeline(steps=[
                ('hits_preparation', columns_preparation_hits),
                ('hits_drop_columns', columns_deleter_hits),
                ('change_dtypes_hits', columns_dtypes_hits)
            ])
    #сохраним пайплайн в пикл файл
    with open('pickls/pipiline_sessions.pkl', 'wb') as file:
        # dill.dump(best_pipe, file)
        dill.dump({
            'model': pipe_sessions,
            'metadata': {
                "name": "ga_sessions processing",
                "author": "Evgenyi",
                "version": 1,
                "date": datetime.datetime.now()
            }
        }, file, recurse=True)

    with open('pickls/pipiline_hits.pkl', 'wb') as file:
        # dill.dump(best_pipe, file)
        dill.dump({
            'model': pipe_hits,
            'metadata': {
                "name": "ga_hits processing",
                "author": "Evgenyi",
                "version": 1,
                "date": datetime.datetime.now()
            }
        }, file, recurse=True)
    #тест
    #df = pd.read_csv(r'C:\Users\epsokolo\final_work\data\database\ga_sessions.csv', nrows=100)
    # def transform_df(data):
    #     new_df = pipe_sessions.transform(data)
    #     return new_df
    #
    # new_df = pipe_sessions.transform(df)
    # print(new_df.head(5))

if __name__ == '__main__':
    main()

