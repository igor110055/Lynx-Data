import websocket
import json
import pandas as pd
import datetime
import rel
from sockets.data import Database


class Bitfinex:
    def __init__(self):
        self.path = 'databases/Bitfinex.db'
        self.db = Database(self.path)
        self.uri = "wss://api-pub.bitfinex.com/ws/2"
        self.channel = ['ticker']
        self.markets = 'tBTCUSD'
        self.data = json.dumps({"event": "subscribe",
                                "channel": self.channel,
                                "symbol": self.markets})
        self.headers = [
            "BID",
            "BID_SIZE",
            "ASK",
            "ASK_SIZE",
            "DAILY_CHANGE",
            "DAILY_CHANGE_RELATIVE",
            "LAST_PRICE",
            "VOLUME",
            "HIGH",
            "LOW"
        ]

    def split(self, data):
        return list(data.split(','))

    def push(self, res, db, headers, markets):
        try:
            res = {headers[i]: res[i] for i in range(len(headers))}
            res['time'] = datetime.datetime.now()
            df = pd.DataFrame([res])
            df.to_sql(markets,
                      con=db.connection,
                      if_exists='append')
        except IndexError:
            pass

    def on_message(self, ws, message):
        data = (message[9:-2])
        res = self.split(data)
        self.push(res, self.db, self.headers, self.markets)

    def on_error(self, ws, error):
        print(error)
        with open("logs/Bitfinex.txt", 'a') as f:
            error = str(error) + " " + str(datetime.datetime.now()) + "\n"
            f.write(error)

    def on_close(self, ws, close_status_code, close_msg):
        print("### closed ###")
        with open("logs/Bitfinex.txt", 'a') as f:
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
