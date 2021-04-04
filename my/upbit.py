import pandas as pd
import numpy as np
import requests
from websocket import create_connection
import gzip
from json import dumps, loads
import time
from datetime import datetime

if __name__=="__main__":
    params = [{"ticket":"test"}, {"type":"trade", "codes":["KRW-BTC"]}]
    start = time.time()
    ws = create_connection('wss://api.upbit.com/websocket/v1')
    count = 0
    count2 = 0
    prices = []
    volumes = []
    vwap = dict()
    vwap['vwap'] = []
    vwap['time'] = []
    while True:
        ws.send(dumps(params))
        data= loads(ws.recv())
        trade_time = datetime.fromtimestamp(data['trade_timestamp']/1000).strftime('%H:%M:%S.%f')
        ticker = data['code']
        price = data['trade_price']
        volume = data['trade_volume'] * price
        ask_bid = data['ask_bid']
        prices.append(price)
        volumes.append(volume)
        print(f'Time : {trade_time}, Ticker : {ticker}, Trade Price : {price}, Trade Volume : {volume:.2f}, ask_bid : {ask_bid}')
        count += 1
        if count%10==0:
            print('VWAP!')
            vwap['vwap'].append(np.average(prices, weights=volumes))
            vwap['time'].append(trade_time)
            count = 0
            prices = []
            volumes = []
        count2 += 1
        if count2 > 300:
            df = pd.DataFrame(vwap)
            df.to_csv("../data/{0}.csv".format(data['trade_timestamp']))
            count2 = 0 
        time.sleep(1)


