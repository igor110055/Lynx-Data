import asyncio
from binance import AsyncClient, BinanceSocketManager
import sqlite3


async def main():
    con = sqlite3.connect('Database/binance.db')
    cur = con.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS bnbusdt
                   (event text, time text, symbol text, close real, open real,
                   high real, low real, totalbase real, totalquote real)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS btcusdt
                   (event text, time text, symbol text, close real, open real,
                    high real, low real, totalbase real, totalquote real)''')
    client = await AsyncClient.create()
    bm = BinanceSocketManager(client)
    # start any sockets here, i.e a trade socket
    ms = bm.multiplex_socket(['bnbusdt@miniTicker', 'btcusdt@miniTicker'])
    # then start receiving messages
    async with ms as tscm:
        while True:
            res = await tscm.recv()
            print(list(res['data'].values()))
            if res['stream'] == 'bnbusdt@miniTicker':
                cur.execute("INSERT INTO bnbusdt VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", list(res['data'].values()))
            else:
                cur.execute("INSERT INTO btcusdt VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", list(res['data'].values()))
            con.commit()

    await client.close_connection()
    con.close()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
