import asyncio
import websockets
import json
import gzip



async def collect():
    uri = "wss://api.huobi.pro/ws"
    markets = 'btcusd'
    async with websockets.connect(uri) as websocket:
        data = json.dumps({"sub": "market.btcusdt.ticker"})
        await websocket.send(data)
        while True:
            data = await websocket.recv()
            uncompressed_data = gzip.decompress(data)
            print(str(uncompressed_data, 'utf-8'))


asyncio.get_event_loop().run_until_complete(collect())
