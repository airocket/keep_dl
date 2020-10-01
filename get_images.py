import datetime
import io
import time
import traceback

import psycopg2
import telebot
import pandas as pd
import pandas.io.sql as psql
import matplotlib.pyplot as plt
from pylab import rcParams


rcParams['figure.figsize'] = 12, 10




def get_keep_data():
    conn = psycopg2.connect(database="keep_data", user="postgres", password="postgres", host="localhost", port="5432")
    market_data = psql.read_sql("Select * from markets_data", conn)
    predict = psql.read_sql("Select * from predict", conn)
    conn.close()
    return market_data, predict


def get_data():
    market_data, predict = get_keep_data()
    market_data = market_data[-15:]
    data = pd.DataFrame()
    data['time'] = market_data['time']
    data['close'] = market_data['close_b']
    data = data.set_index('time')
    predict = predict[-1:]
    predict = predict.set_index('time')
    plt.suptitle("Price KEEP-ETH prediction @Keep_Predict\n Disclaimer: for entertainment only, do not use for trading decisions.", fontsize=24)
    plt.plot(data, label='day close price',color='orange')
    plt.plot(predict['simple_predict'],'bo', label='simple predict close price')
    plt.plot(predict['lstm_predict'], 'ro', label='lstm predict close price')
    plt.xlabel('time',fontsize=20)
    plt.ylabel('close day price KEEP-ETH',fontsize=20)

    plt.legend()
    plt.xticks(rotation=35)
    plt.grid(True)
    buf = io.BytesIO()
    plt.savefig('image.png', format='png')
    buf.seek(0)
    plt.close()
    text = f"Predict {predict.index.date[0]}\nSimple neural network prediction price : {predict['simple_predict'].values[0].round(5)} ETH\nLSTM neural network prediction price : {predict['lstm_predict'].values[0].round(5)} ETH  "
    print(text)
    return buf, data, predict
get_data()

