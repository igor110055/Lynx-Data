import asyncio
from binance import AsyncClient, BinanceSocketManager
import pandas as pd
from data import Database


async def main():
    db = Database("Binance.db")
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
            if res['stream'] == 'bnbusdt@miniTicker':
                df1 = pd.DataFrame([data])
                df1.to_sql('BNBUSDT',
                           con=db.connection,
                           if_exists='append')
            else:
                df2 = pd.DataFrame([data])
                df2.to_sql('BTCUSDT',
                       con=db.connection,
                       if_exists='append')


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
