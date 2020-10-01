import datetime

import psycopg2
import pandas as pd
import pandas.io.sql as psql
from sklearn import preprocessing
import numpy as np
from tensorflow.keras.models import load_model
from utils_load_data.dbinit import update_predict
import csv
from io import StringIO
import pandas.io.sql as psql
from sqlalchemy import create_engine


database_name = 'keep_data'
table_name ='keep_predict'
engine = create_engine('postgresql://postgres:postgres@localhost:5432/'+database_name)


def get_data():
    conn = psycopg2.connect(database="keep_data", user="postgres", password="postgres", host="localhost", port="5432")
    df = psql.read_sql("Select * from markets_data", conn)
    conn.close()
    return df

def get_predict(df):
    history_points = 10
    df = df.reset_index(drop=True)
    data = df.copy()
    del data['time']
    data = data.reset_index()
    data = data.drop('index', axis=1)
    data_normaliser = preprocessing.MinMaxScaler()
    data_normalised = data_normaliser.fit_transform(data)
    ohlcv_histories_normalised = np.array([data_normalised[i: i + history_points].copy() for i in range(len(data_normalised) - history_points)])
    next_day_open_values_normalised = np.array([data_normalised[:,1][i + history_points].copy() for i in range(len(data_normalised) - history_points)])
    next_day_open_values_normalised = np.expand_dims(next_day_open_values_normalised, -1)
    next_day_open_values = np.array([data.loc[:, "close_b"][i + history_points].copy() for i in range(len(data) - history_points)])
    next_day_open_values = np.expand_dims(next_day_open_values, -1)
    y_normaliser = preprocessing.MinMaxScaler()
    y_normaliser.fit(next_day_open_values)
    model = load_model("models\keep_predit_lstm.h5")
    save = pd.DataFrame(columns=['time', 'lstm_predict'])
    predicted = model.predict(ohlcv_histories_normalised)
    predicted = y_normaliser.inverse_transform(predicted)

    for i in range(len(predicted)):

        predicted_lstm_value = predicted[i][0].copy()
        if i == ohlcv_histories_normalised.shape[0] -1:
            predicted_time = df['time'][history_points + i:].values[0].copy() + pd.Timedelta('1 days')
        else:
            predicted_time = df['time'][history_points + i:].values[0].copy() + pd.Timedelta('1 days')

        buf = pd.DataFrame([{'time': predicted_time,
                             'lstm_predict': predicted_lstm_value + 0.00075}])
        save = save.append(buf)

    return save

def psql_insert_copy(table, conn, keys, data_iter):
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)


def run_predict():
    data = get_data()
    data = data.sort_values('time')
    predicted_lstm_value = get_predict(data)
    predicted_lstm_value.reset_index(drop=True, inplace=True)
    predicted_lstm_value['time'] = predicted_lstm_value['time'] - datetime.timedelta(hours=3)
    predicted_lstm_value.set_index(predicted_lstm_value['time'], inplace=True)
    predicted_lstm_value.sort_index(inplace=True)
    predicted_lstm_value.drop(['time'], axis=1, inplace=True)
    predicted_lstm_value.to_sql(table_name, engine, if_exists='replace', method=psql_insert_copy)
    print(datetime.datetime.now(), 'Predict Writed')

