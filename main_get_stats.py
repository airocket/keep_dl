import datetime
import time
import traceback
from io import StringIO
from sqlalchemy import create_engine
import psycopg2
import requests
import decimal
import pandas as pd
import csv
import pandas.io.sql as psql
from datetime import timedelta
from utils_load_data.marketdata import get_trades, get_orderbook, get_ohlc
from utils_load_data.tokendata import get_token_data

pd.options.mode.chained_assignment = None

database_name = 'keep_data'
table_name ='keep_info'
engine = create_engine('postgresql://postgres:postgres@localhost:5432/'+database_name)

def filter_start_date(start, df):
    df = df.query(f"ts >= {start}")
    df.reset_index(drop=True, inplace=True)
    return df


class GetStats:

    def timestamp_to_date(self, timestamp):
        return datetime.datetime.fromtimestamp(timestamp / 1000)

    def nonce_to_date(self, nonce):
        return datetime.datetime.fromtimestamp(nonce)

    def get_btc_candle(self):
        try:
            url_spot_btc = 'https://www.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1d'
            response_btc = requests.get(url_spot_btc).json()
            df_btc = pd.DataFrame(response_btc)
            df_btc[0] = df_btc[0].apply(lambda x: self.timestamp_to_date(x - 10800000))
            df_btc = df_btc.set_index(df_btc[0])
            df_btc = df_btc.drop([0, 6, 7, 10, 11], axis=1)
            columns = ['open_btc', 'high_btc', 'low_btc', 'close_btc', 'volume_btc', 'num_trades_btc', 'buy_volume_btc']
            df_btc.columns = columns
            return df_btc
        except:
            print(datetime.datetime.now(), 'Except get_btc_candle', traceback.format_exc())
            return None

    def get_eth_candle(self):
        try:
            url_spot_eth = 'https://www.binance.com/api/v3/klines?symbol=ETHUSDT&interval=1d'
            response_eth = requests.get(url_spot_eth).json()
            df_eth = pd.DataFrame(response_eth)
            df_eth[0] = df_eth[0].apply(lambda x: self.timestamp_to_date(x - 10800000))
            df_eth = df_eth.set_index(df_eth[0])
            df_eth = df_eth.drop([0, 6, 7, 10, 11], axis=1)
            columns = ['open_eth', 'high_eth', 'low_eth', 'close_eth', 'volume_eth', 'num_trades_eth', 'buy_volume_eth']
            df_eth.columns = columns
            return df_eth
        except:
            print(datetime.datetime.now(), 'Except get_eth_candle', traceback.format_exc())
            return None

    def get_history_tx_keep(self):
        try:
            # start_block = 9961949
            # block_step = 3000 # 6000 - 1 day
            # current_block = 12958633
            # i = 0
            # while True:
            # downoload no csv
            # url = 'https://api.etherscan.io/api?module=logs&action=getLogs&fromBlock=' + str(start_block) + '&toBlock=' + str(start_block + block_step - 1) + '&address=0x85eee30c52b0b379b046fb0f85f4f3dc3009afec&apikey=QSGJ9D1GEUD64Y9VYASQ2N89APH8549HRA'
            # response = requests.get(url).json()
            # result_list = []
            # for item in response['result']:
            #     try:
            #         row = []
            #         nonce = decimal.Decimal(float.fromhex(item['timeStamp']))
            #         value_tx = round(decimal.Decimal(float.fromhex(item['data']))/decimal.Decimal('1000000000000000000'), 2)
            #         row.append(int(nonce))
            #         row.append(float(value_tx))
            #         result_list.append(row)
            #     except:
            #         print('exc')
            #
            #
            #
            # with open('history_tx.scv', "a", newline='') as csv_file:
            #     writer = csv.writer(csv_file, delimiter=',')
            #     for line in result_list:
            #         writer.writerow(line)
            # if start_block > current_block:
            #     break
            # start_block = start_block + block_step
            #
            # print(i, start_block)
            # i = i + 1
            #
            # time.sleep(2)

            df = pd.read_csv('history_tx.csv')
            df['date'] = df['nonce'].apply(lambda x: self.nonce_to_date(x))
            df = df.drop_duplicates(subset=['nonce'], keep=False)
            df = df.set_index(df['date'])
            df = df.drop(['nonce', 'date'], axis=1)
            df_result = pd.DataFrame()
            df = df.loc[df.ne(0).all(axis=1)]
            df_result['sum_volume'] = df['value'].resample("1D").sum()
            df_result['count'] = df['value'].resample("1D").count()
            df_result['avg'] = df['value'].resample("1D").mean()
            df_result['min'] = df['value'].resample("1D").min()
            df_result['max'] = df['value'].resample("1D").max()
            return df_result
        except:
            print(datetime.datetime.now(), 'Except get_eth_candle', traceback.format_exc())
            return None

    def get_one_day_tx_keep(self):
        try:
            url = 'https://api.etherscan.io/api?module=block&action=getblocknobytime&timestamp=' + str(
                int(time.time())) + '&closest=before&apikey=QSGJ9D1GEUD64Y9VYASQ2N89APH8549HRA'
            response = requests.get(url).json()
            current_block = int(response['result'])
            block_from = current_block - 2000
            url = 'https://api.etherscan.io/api?module=logs&action=getLogs&fromBlock=' + str(
                block_from) + '&toBlock=' + str(
                current_block) + '&address=0x85eee30c52b0b379b046fb0f85f4f3dc3009afec&apikey=QSGJ9D1GEUD64Y9VYASQ2N89APH8549HRA'
            response = requests.get(url).json()
            result_list = []
            for item in response['result']:
                try:
                    row = []
                    nonce = decimal.Decimal(float.fromhex(item['timeStamp']))
                    value_tx = round(
                        decimal.Decimal(float.fromhex(item['data'])) / decimal.Decimal('1000000000000000000'), 2)
                    row.append(int(nonce))
                    row.append(float(value_tx))
                    result_list.append(row)
                except:
                    print('exc')
            df = pd.read_csv('history_tx.csv')
            df = df.append(pd.DataFrame(result_list, columns=['nonce', 'value']), ignore_index=True)
            df['date'] = df['nonce'].apply(lambda x: self.nonce_to_date(x))
            df = df.drop_duplicates(subset=['nonce'], keep=False)
            df = df.set_index(df['date'])
            df = df.drop(['date'], axis=1)
            df.to_csv('history_tx.csv', index=False)
            df = df.drop(['nonce'], axis=1)
            df_result = pd.DataFrame()
            df = df.loc[df.ne(0).all(axis=1)]
            df_result['sum_volume'] = df['value'].resample("1D").sum()
            df_result['count'] = df['value'].resample("1D").count()
            df_result['avg'] = df['value'].resample("1D").mean()
            df_result['min'] = df['value'].resample("1D").min()
            df_result['max'] = df['value'].resample("1D").max()
            df_result = df_result[-2:]
            return df_result
        except:
            print(datetime.datetime.now(), 'Except get_eth_candle', traceback.format_exc())
            return None

    def get_keep_data(self):
        conn = psycopg2.connect(database="keep_data", user="postgres", password="postgres", host="localhost",
                                port="5432")
        market_data = psql.read_sql("Select * from markets_data", conn)
        predict = psql.read_sql("Select * from predict", conn)
        conn.close()
        market_data["time"] = market_data["time"] - timedelta(hours=3)
        predict["time"] = predict["time"] - timedelta(hours=3)
        market_data.set_index(market_data['time'], inplace=True)
        predict.set_index(predict['time'], inplace=True)
        market_data.sort_index(inplace=True)
        predict.sort_index(inplace=True)
        market_data.drop(['time'], axis=1, inplace=True)
        return market_data

    def market_data(self, first_start):
        ohlc = get_ohlc()
        token_data = get_token_data()
        start_date = ohlc[0][0]
        token_data = filter_start_date(start_date, token_data)
        ohlc['ts'] = ohlc[0]
        data = pd.merge(ohlc, token_data, on='ts')
        data.drop(['ts'], axis='columns', inplace=True)
        data.reset_index(drop=True, inplace=True)
        data[0] = data[0].apply(lambda x: self.timestamp_to_date((x * 1000) - 10800000))
        data.set_index(data[0], inplace=True)
        data.sort_index(inplace=True)
        data.drop([0], axis=1, inplace=True)
        columns = data.columns.to_list()
        columns[0] = "open_keep"
        columns[1] = "close_keep"
        columns[2] = "high_keep"
        columns[3] = "low_keep"
        columns[4] = "vol_keep"
        columns[5] = "volcon_keep"
        data.columns = columns
        return data


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


def get_keep_data():
    conn = psycopg2.connect(database="keep_data", user="postgres", password="postgres", host="localhost", port="5432")
    market_data = psql.read_sql("Select * from keep_info", conn)
    conn.close()
    return market_data

if __name__ == "__main__":
    stats = GetStats()
    first_start = True
    get_keep_data()
    while True:
        try:
            btc = stats.get_btc_candle()
            eth = stats.get_eth_candle()
            hist = stats.get_history_tx_keep()
            day = stats.get_one_day_tx_keep()
            keep_data = stats.market_data(True)
            hist[-2:-1] = day[0:1]
            hist[-1:] = day[1:]
            data = pd.merge(btc, eth, left_index=True, right_index=True)
            data = pd.merge(data, hist, left_index=True, right_index=True)
            data = pd.merge(data, keep_data, left_index=True, right_index=True)
            if first_start:
                data.to_sql(table_name, engine, if_exists='replace', method=psql_insert_copy)
            else:
                pass
            print('END')
            time.sleep(1800)
        except:
            print(datetime.datetime.now(), traceback.format_exc())
            time.sleep(1800)

