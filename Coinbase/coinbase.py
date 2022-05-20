import asyncio
import websockets
import json


async def collect():
    uri = "wss://ws-feed.exchange.coinbase.com"
    channel = ["ticker"]
    markets = ["ETH-USD", "BTC-USD"]
    async with websockets.connect(uri) as websocket:
        data = json.dumps({
                    "type": "subscribe",
                    "product_ids": markets,
                    "channels": channel,
                    })
        await websocket.send(data)
        while True:
            data = await websocket.recv()
            print(data)


asyncio.get_event_loop().run_until_complete(collect())
