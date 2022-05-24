import websocket
import pandas as pd
import rel
import datetime
from sockets.data import Database
import json


class Gate:
    def __init__(self):
        self.uri = "wss://ws.gate.io/v3/"
        self.markets = ["BTC_USDT", "BNB_USDT"]
        self.path = "databases/Gate.db"
        self.db = Database(self.path)
        self.subdata = json.dumps({"id": 12312, "method": "ticker.subscribe", "params": self.markets})

    def push(self, res, db):
        res = json.loads(res)
        data = res['params'][1]
        df = pd.DataFrame([data])
        df.to_sql(res['params'][0],
                  con=db.connection,
                  if_exists='append')

    def on_message(self, ws, message):
        print(message)
        self.push(message, self.db)


    def on_error(self, ws, error):
        print(error)
        with open("logs/Gate.txt", 'a') as f:
            error = str(error) + " " + str(datetime.datetime.now()) + "\n"
            f.write(error)

    def on_close(self, ws, close_status_code, close_msg):
        print("### closed ###")
        with open("logs/Gate.txt", 'a') as f:
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
