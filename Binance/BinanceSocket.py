import asyncio
from binance import AsyncClient, BinanceSocketManager
import pandas as pd
from data import Database


async def main():
    path = "Binance.db"
    db = Database(path)
    client = await AsyncClient.create()
    bm = BinanceSocketManager(client)
    markets = ['bnbusdt@miniTicker', 'btcusdt@miniTicker']
    # start any sockets here, i.e a trade socket
    ms = bm.multiplex_socket(markets)
    # then start receiving messages
    async with ms as tscm:
        while True:
            res = await tscm.recv()
            data = res['data']
            data.pop('e')
            print(data)
            df = pd.DataFrame([data])
            df.to_sql(res['stream'].split('@', 1)[0],
                      con=db.connection,
                      if_exists='append')


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())