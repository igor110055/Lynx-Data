import asyncio
import websockets
import json
import pandas as pd
import sys

sys.path.append('../')
from data import Database


def push(res, db):
    try:
        df = pd.DataFrame([res['data']])
        df.to_sql(res['market'],
                  con=db.connection,
                  if_exists='append')
        print(df)
    except KeyError:
        print(f'Successfully subscribed to {res["market"]}')


async def collect():
    path = 'Ftx.db'
    db = Database(path)
    uri = "wss://ftx.com/ws/"
    channel = 'ticker'
    markets = ['BNB/USDT', 'BTC/USDT']
    async with websockets.connect(uri) as websocket:
        for market in markets:
            data = json.dumps({'op': 'subscribe',
                               'channel': channel,
                               'market': market})
            await websocket.send(data)
        while True:
            res = await websocket.recv()
            res = json.loads(res)
            push(res, db)


asyncio.get_event_loop().run_until_complete(collect())
