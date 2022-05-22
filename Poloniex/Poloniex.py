import asyncio
import websockets
import json
import pandas as pd
from data import Database
import datetime


def split(data):
    return list(data.split(','))


def push(final_data, db, headers):
    print(final_data)
    try:
        res = {headers[i]: final_data[i] for i in range(len(headers))}
        res['time'] = datetime.datetime.now()
        df = pd.DataFrame([res])
        print(df)
        df.to_sql('Poloniex',
                  con=db.connection,
                  if_exists='append')
    except IndexError:
        pass


async def collect():
    path = 'Poloniex.db'
    db = Database(path)
    uri = "wss://api2.poloniex.com"
    channel = 1002
    headers = ["currency pair id",
               "last trade price",
               "lowest ask", "highest bid",
               "percent change in last 24 hours",
               "base currency volume in last 24 hours",
               "quote currency volume in last 24 hours",
               "is frozen",
               "highest trade price in last 24 hours",
               "lowest trade price in last 24 hours",
               "post only", "maintenance mode"]

    async with websockets.connect(uri) as websocket:
        data = json.dumps({
            "command": "subscribe",
            "channel": channel,
            "currency pair id": "149"
        })
        await websocket.send(data)
        while True:
            result = await websocket.recv()
            final_data = split(result[12:-2])
            push(final_data, db, headers)


asyncio.get_event_loop().run_until_complete(collect())
