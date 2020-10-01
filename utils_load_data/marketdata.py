import requests
import pandas as pd


def get_ohlc():
    url = 'https://bilaxy.com/api/v2/market/period'
    params = {'symbol': 410,
              'step': 86400}
    r = requests.get(url, params=params)
    df = pd.DataFrame(r.json())
    df.drop([1, 2], axis='columns', inplace=True)
    #df = df[:-1]
    return df


def get_orderbook():
    url = 'https://newapi.bilaxy.com/v1/orderbook'
    params = {'pair': 'KEEP_ETH'}
    r = requests.get(url, params=params)
    json_data = r.json()
    df_bids = pd.DataFrame(json_data['bids'])
    df_asks = pd.DataFrame(json_data['asks'])
    df_bids = df_bids.astype('float64')
    df_asks = df_asks.astype('float64')
    return df_bids, df_asks


def get_trades():
    url = 'https://newapi.bilaxy.com/v1/trades'
    params = {'pair': 'KEEP_ETH'}
    r = requests.get(url, params=params)
    df = pd.DataFrame(r.json())
    return df
