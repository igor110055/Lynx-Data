import asyncio
import websockets
import json


async def collect():
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
            data = await websocket.recv()
            print(data)


asyncio.get_event_loop().run_until_complete(collect())
