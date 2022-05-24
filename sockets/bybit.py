import websocket
import pandas as pd
import rel
import datetime
import json
from sockets.data import Database


class Bybit:
    def __init__(self):
        self.uri = "wss://stream.bybit.com/realtime"
        self.markets = ["instrument_info.100ms.BTCUSD", "instrument_info.100ms.ETHUSD"]
        self.path = "databases/Bybit.db"
        self.db = Database(self.path)
        self.subdata = json.dumps({"op": "subscribe", "args": self.markets})

    def push(self, res, db):
        res = eval(res)
        data = res['data']['update'][0]
        df = pd.DataFrame([data])
        print(df)
        df.to_sql(res['data']['update'][0]['symbol'],
                  con=db.connection,
                  if_exists='append')

    def on_message(self, ws, message):
        print(message)
        self.push(message, self.db)

    def on_error(self, ws, error):
        print(error)
        with open("logs/Bybit.txt", 'a') as f:
            error = str(error) + " " + str(datetime.datetime.now()) + "\n"
            f.write(error)

    def on_close(self, ws, close_status_code, close_msg):
        print("### closed ###")
        with open("logs/Bybit.txt", 'a') as f:
            message = str(close_msg) + " " + str(datetime.datetime.now()) + "\n"
            f.write(message)
        self.start()

    def on_open(self, ws):
        print("Opened connection")
        ws.send(self.subdata)

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


