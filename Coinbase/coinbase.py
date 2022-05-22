import asyncio
import websockets
import json
import pandas as pd
from data import Database


def push(result, db):
    try:
        result.pop('type')
        print(result)
        df = pd.DataFrame([result])
        df.to_sql(result['product_id'],
                  con=db.connection,
                  if_exists='append')
    except KeyError:
        print(f'Successfully Subscribed to {result["channels"]}')


async def collect():
    path = 'Coinbase.db'
    db = Database(path)
    uri = "wss://ws-feed.exchange.coinbase.com"
    channel = ["ticker"]
    markets = ["ETH-USD", "BTC-USD"]
    dump_data = {"type": "subscribe",
                 "product_ids": markets,
                 "channels": channel,
                 }
    async with websockets.connect(uri) as websocket:
        data = json.dumps(dump_data)
        await websocket.send(data)
        while True:
            res = await websocket.recv()
            res = json.loads(res)
            push(res, db)

asyncio.get_event_loop().run_until_complete(collect())
