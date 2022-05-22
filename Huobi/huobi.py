import asyncio
import websockets
import json
import gzip
import pandas as pd
from data import Database
import datetime


def push(res, db):
    uncompressed_data = gzip.decompress(res)
    final_data = json.loads(uncompressed_data)
    try:
        final_data['tick']['time'] = datetime.datetime.now()
        df = pd.DataFrame([final_data['tick']])
        print(df)
        df.to_sql(final_data['ch'].split('.')[1],
                  con=db.connection,
                  if_exists='append')
    except KeyError:
        pass


async def collect():
    path = 'Huobi.db'
    db = Database(path)
    uri = "wss://api.huobi.pro/ws"
    markets = ['bnbusdt', 'btcusdt']
    async with websockets.connect(uri) as websocket:
        for market in markets:
            data = json.dumps({"sub": f"market.{market}.ticker"})
        await websocket.send(data)
        while True:
            data = await websocket.recv()
            push(data, db)


asyncio.get_event_loop().run_until_complete(collect())
