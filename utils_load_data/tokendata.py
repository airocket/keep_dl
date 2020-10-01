import requests
import pandas as pd


def get_token_data():
    url = 'https://api2.ethplorer.io/getTokenPriceHistoryGrouped/0x85eee30c52b0b379b046fb0f85f4f3dc3009afec?address=' \
          '0x85eee30c52b0b379b046fb0f85f4f3dc3009afec&apiKey=ethplorer.widget&domain=https%3A%2F%2Fethplorer.io' \
          '%2Faddress%2F0x85eee30c52b0b379b046fb0f85f4f3dc3009afec%23tab%3Dtab-holders&period=730&theme=dark&type=area'
    r = requests.get(url)
    json_data = r.json()
    countTxs = pd.DataFrame(json_data['history']['countTxs'])
    countTxs.drop(['_id'], axis='columns', inplace=True)
    countTxs.sort_values('ts', inplace=True, ignore_index=True)
    prices = pd.DataFrame(json_data['history']['prices'])
    prices.drop(['date', 'hour', 'cap', 'tmp'], axis='columns', inplace=True)
    prices.sort_values('ts', inplace=True, ignore_index=True)
    first_timestamp = prices['ts'][0]
    countTxs = countTxs.query(f"ts >= {first_timestamp}")
    countTxs.reset_index(drop=True, inplace=True)
    countTxs['ts'] = prices['ts'].copy()
    #countTxs = countTxs[:-1]
    #prices = prices[:-1]
    token_data = prices
    token_data['cnt'] = countTxs['cnt']
    return token_data

