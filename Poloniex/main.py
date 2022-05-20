import asyncio
import websockets
import json
import time


async def collect():
    uri = "wss://api2.poloniex.com"
    channel = 1002
    markets = ["ETH-USD", "BTC-USD"]
    async with websockets.connect(uri) as websocket:
        data = json.dumps({
                    "command": "subscribe",
                    "channel": channel,
                    })
        await websocket.send(data)
        while True:
            data = await websocket.recv()
            print(data)


asyncio.get_event_loop().run_until_complete(collect())
