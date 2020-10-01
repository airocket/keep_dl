import datetime
import traceback
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import numpy as np

psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)
psycopg2.extensions.register_adapter(np.datetime64, psycopg2._psycopg.AsIs)
psycopg2.extensions.register_adapter(np.float32, psycopg2._psycopg.AsIs)
import pandas as pd


def create_database():
    try:
        conn = psycopg2.connect("user=postgres password='postgres'")
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute("create database keep_data")
        conn.commit()
        conn.close()
    except:
        print(datetime.datetime.now(), traceback.format_exc())


def create_table_markets():
    try:
        conn = psycopg2.connect(dbname='keep_data', user='postgres',
                                password='postgres', host='localhost', port=5432)
        cur = conn.cursor()
        cur.execute("CREATE TABLE markets_data (time timestamp,"
                    " open_b double precision, close_b double precision, high_b double precision,"
                    " low_b double precision, vol_b double precision, vol_conv_b double precision,"
                    " open double precision, close double precision, high double precision,"
                    " low double precision, vol double precision, vol_conv double precision,"
                    " average double precision, cnt int)")

        conn.commit()
        conn.close()
        conn = psycopg2.connect(dbname='keep_data', user='postgres',
                                password='postgres', host='localhost', port=5432)
        cur = conn.cursor()
        cur.execute("SELECT create_hypertable('markets_data', 'time',chunk_time_interval => INTERVAL '30 day')")
        conn.commit()
        conn.close()
    except:
        print(datetime.datetime.now(), traceback.format_exc())


def create_table_predict():
    try:
        conn = psycopg2.connect(dbname='keep_data', user='postgres',
                                password='postgres', host='localhost', port=5432)
        cur = conn.cursor()
        cur.execute("CREATE TABLE predict (time timestamp,"
                    " lstm_predict double precision, simple_predict double precision)")

        conn.commit()
        conn.close()
        conn = psycopg2.connect(dbname='keep_data', user='postgres',
                                password='postgres', host='localhost', port=5432)
        cur = conn.cursor()
        cur.execute("SELECT create_hypertable('predict', 'time',chunk_time_interval => INTERVAL '30 day')")
        conn.commit()
        conn.close()
    except:
        print(datetime.datetime.now(), traceback.format_exc())

def update_markets_data(df):
    try:
        conn = psycopg2.connect(dbname='keep_data', user='postgres',
                                password='postgres', host='localhost', port=5432)
        cur = conn.cursor()
        for i in range(len(df)):
            df_send = df[i:i + 1]
            df_send[0] = datetime.datetime.fromtimestamp(df_send[0].values[0])
            df_send[0] = pd.to_datetime(df_send[0].values[0], utc=False)
            cur.execute(f"SELECT * FROM markets_data WHERE time = '{df_send[0].values[0]}'")
            records = cur.fetchall()
            if len(records) == 0:
                cur.execute("INSERT INTO markets_data (time,"
                            " open_b, close_b, high_b,"
                            " low_b , vol_b, vol_conv_b,"
                            " open, close, high ,"
                            " low, vol, vol_conv,"
                            " average, cnt)"
                            " VALUES (%s,%s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s)",
                            (str(df_send[0].values[0]), df_send[3].values[0], df_send[4].values[0],
                             df_send[5].values[0], df_send[6].values[0], df_send[7].values[0],
                             df_send[8].values[0], df_send['open'].values[0], df_send['close'].values[0],
                             df_send['high'].values[0], df_send['low'].values[0], df_send['volume'].values[0],
                             df_send['volumeConverted'].values[0], df_send['average'].values[0],
                             df_send['cnt'].values[0]))

                conn.commit()
            elif i == len(df) - 1 or i == len(df) - 2:
                cur.execute(f"DELETE FROM markets_data WHERE time = '{df_send[0].values[0]}'")
                conn.commit()
                cur.execute("INSERT INTO markets_data (time,"
                            " open_b, close_b, high_b,"
                            " low_b , vol_b, vol_conv_b,"
                            " open, close, high ,"
                            " low, vol, vol_conv,"
                            " average, cnt)"
                            " VALUES (%s,%s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s)",
                            (str(df_send[0].values[0]), df_send[3].values[0], df_send[4].values[0],
                             df_send[5].values[0], df_send[6].values[0], df_send[7].values[0],
                             df_send[8].values[0], df_send['open'].values[0], df_send['close'].values[0],
                             df_send['high'].values[0], df_send['low'].values[0], df_send['volume'].values[0],
                             df_send['volumeConverted'].values[0], df_send['average'].values[0],
                             df_send['cnt'].values[0]))

                conn.commit()
                print(datetime.datetime.now(), 'update row, time:', df_send[0].values[0])

            else:
                print(datetime.datetime.now(), 'already exists, time:', df_send[0].values[0])
        conn.close()
    except:
        print(datetime.datetime.now(),traceback.format_exc())


def update_predict(time_predict, lstm_predict, simple_predict):
    try:
        conn = psycopg2.connect(dbname='keep_data', user='postgres',
                                password='postgres', host='localhost', port=5432)
        cur = conn.cursor()

        cur.execute(f"SELECT * FROM predict WHERE time = '{time_predict}'")
        records = cur.fetchall()
        if len(records) == 0:
            cur.execute("INSERT INTO predict (time,"
                        " lstm_predict, simple_predict)"
                        " VALUES (%s,%s,%s)",
                        (str(time_predict), lstm_predict, simple_predict))

            conn.commit()
        else:
            cur.execute(f"DELETE FROM predict WHERE time = '{time_predict}'")
            conn.commit()
            cur.execute("INSERT INTO predict (time,"
                        " lstm_predict, simple_predict)"
                        " VALUES (%s,%s,%s)",
                        (str(time_predict), lstm_predict, simple_predict))

            conn.commit()
            print(datetime.datetime.now(), 'update predict row, time:', time_predict)


        conn.close()
    except:
        print(datetime.datetime.now(),traceback.format_exc())


create_database()
create_table_markets()
create_table_predict()
