import asyncio
import websockets
import json
import pandas as pd
from data import Database
import datetime


def split(data):
    return list(data.split(','))


def push(res, db, headers, markets):
    try:
        res = {headers[i]: res[i] for i in range(len(headers))}
        res['time'] = datetime.datetime.now()
        df = pd.DataFrame([res])
        print(df)
        df.to_sql(markets,
                  con=db.connection,
                  if_exists='append')
    except IndexError:
        pass


async def collect():
    path = 'Bitfinex.db'
    db = Database(path)
    uri = "wss://api-pub.bitfinex.com/ws/2"
    channel = ['ticker']
    markets = 'tBTCUSD'
    headers = [
        "BID",
        "BID_SIZE",
        "ASK",
        "ASK_SIZE",
        "DAILY_CHANGE",
        "DAILY_CHANGE_RELATIVE",
        "LAST_PRICE",
        "VOLUME",
        "HIGH",
        "LOW"
    ]
    async with websockets.connect(uri) as websocket:
        data = json.dumps({"event": "subscribe",
                           "channel": channel,
                           "symbol": markets})
        await websocket.send(data)
        while True:
            data = await websocket.recv()
            data = (data[9:-2])
            res = split(data)
            push(res, db, headers, markets)


asyncio.get_event_loop().run_until_complete(collect())
