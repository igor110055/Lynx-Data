import websocket
import rel
import json
import pandas as pd
import datetime
from sockets.data import Database


class Poloniex:
    def __init__(self):
        self.path = 'databases/Poloniex.db'
        self.db = Database(self.path)
        self.uri = "wss://api2.poloniex.com"
        self.channel = 1002
        self.headers = ["currency pair id",
                        "last trade price",
                        "lowest ask", "highest bid",
                        "percent change in last 24 hours",
                        "base currency volume in last 24 hours",
                        "quote currency volume in last 24 hours",
                        "is frozen",
                        "highest trade price in last 24 hours",
                        "lowest trade price in last 24 hours",
                        "post only", "maintenance mode"]
        self.data = json.dumps({"command": "subscribe",
                                "channel": self.channel,
                                "currency pair id": "149"
                                })

    def split(self, data):
        return list(data.split(','))

    def push(self, final_data, db, headers):
        print(final_data)
        try:
            res = {headers[i]: final_data[i] for i in range(len(headers))}
            res['time'] = datetime.datetime.now()
            df = pd.DataFrame([res])
            df.to_sql('Poloniex',
                      con=db.connection,
                      if_exists='append')
        except IndexError:
            pass

    def on_message(self, ws, message):
        final_data = self.split(message[12:-2])
        self.push(final_data, self.db, self.headers)

    def on_error(self, ws, error):
        print(error)
        with open("logs/Poloniex.txt", 'a') as f:
            error = str(error) + " " + str(datetime.datetime.now()) + "\n"
            f.write(error)

    def on_close(self, ws, close_status_code, close_msg):
        print("### closed ###")
        with open("logs/Poloniex.txt", 'a') as f:
            message = str(close_msg) + " " + str(datetime.datetime.now()) + "\n"
            f.write(message)
        self.start()

    def on_open(self, ws):
        print("Opened connection")
        ws.send(self.data)

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

