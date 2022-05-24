import websocket
import rel
import json
import gzip
import pandas as pd
import datetime
from sockets.data import Database


class Huobi:
    def __init__(self):
        self.path = 'databases/Huobi.db'
        self.db = Database(self.path)
        self.uri = "wss://api.huobi.pro/ws"
        self.markets = ['bnbusdt', 'btcusdt']

    def push(self, res, db):
        uncompressed_data = gzip.decompress(res)
        final_data = json.loads(uncompressed_data)
        try:
            final_data['tick']['time'] = datetime.datetime.now()
            df = pd.DataFrame([final_data['tick']])
            df.to_sql(final_data['ch'].split('.')[1],
                      con=db.connection,
                      if_exists='append')
        except KeyError:
            pass

    def on_message(self, ws, message):
        self.push(message, self.db)

    def on_error(self, ws, error):
        print(error)
        with open("logs/Huobi.txt", 'a') as f:
            error = str(error) + " " + str(datetime.datetime.now()) + "\n"
            f.write(error)

    def on_close(self, ws, close_status_code, close_msg):
        print("### closed ###")
        with open("logs/Huobi.txt", 'a') as f:
            message = str(close_msg) + " " + str(datetime.datetime.now()) + "\n"
            f.write(message)
        self.start()

    def on_open(self, ws):
        print("Opened connection")
        for market in self.markets:
            data = json.dumps({"sub": f"market.{market}.ticker"})
            ws.send(data)

    def start(self):
        websocket.enableTrace(True)
        ws = websocket.WebSocketApp(self.uri,
                                    on_open=self.on_open,
                                    on_message=self.on_message,
                                    on_error=self.on_error,
                                    on_close=self.on_close)

        ws.run_forever(dispatcher=rel)  # Set dispatcher to automatic reconnection
        rel.signal(2, rel.abort)  # Keyboard Interrupt
        rel.dispatch()

