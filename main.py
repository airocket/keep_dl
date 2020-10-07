import traceback
import pandas as pd
import datetime
import time
from utils_load_data.marketdata import get_trades, get_orderbook, get_ohlc
from utils_load_data.tokendata import get_token_data
from utils_load_data.dbinit import update_markets_data
from predict import run_predict
pd.options.mode.chained_assignment = None


def filter_start_date(start, df):
    df = df.query(f"ts >= {start}")
    df.reset_index(drop=True, inplace=True)
    return df


def market_data(first_start):
    ohlc = get_ohlc()
    token_data = get_token_data()
    start_date = ohlc[0][0]
    token_data = filter_start_date(start_date, token_data)
    ohlc['ts'] = ohlc[0]
    data = pd.merge(ohlc, token_data, on='ts')
    data.drop(['ts'], axis='columns', inplace=True)
    data = data.sort_values(0)
    data.reset_index(drop=True, inplace=True)
    data = data[11:]
    if not first_start:
        data = data[-2:]
    update_markets_data(data)


def exchange_data(first_start):
    # TODO futures
    trades = get_trades()
    bids, asks = get_orderbook()
    bids['pr'] = bids[0] * bids[1]
    bids_avg = bids['pr'].sum() / bids[1].sum()
    asks['pr'] = asks[0] * asks[1]
    asks_avg = asks['pr'].sum() / asks[1].sum()


if __name__ == "__main__":
    first_start = True
    while True:
        try:
            market_data(first_start)
            exchange_data(first_start)
            run_predict()
            first_start = False
            time.sleep(1800)
        except:
            print(datetime.datetime.now(), traceback.format_exc())
            time.sleep(10)
