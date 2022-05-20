import asyncio
import websockets
import json


async def collect():
    uri = "wss://api-pub.bitfinex.com/ws/2"
    channel = ['ticker']
    markets = 'tBTCUSD'
    async with websockets.connect(uri) as websocket:
        data = json.dumps({"event": "subscribe",
                           "channel": channel,
                           "symbol": markets})
        await websocket.send(data)
        while True:
            data = await websocket.recv()
            print(data)


asyncio.get_event_loop().run_until_complete(collect())
