import websocket
import datetime
import rel
import json
import pandas as pd
from sockets.data import Database


class Coinbase:
    def __init__(self):
        self.path = 'databases/Coinbase.db'
        self.db = Database(self.path)
        self.uri = "wss://ws-feed.exchange.coinbase.com"
        self.channel = ["ticker"]
        self.markets = ["ETH-USD", "BTC-USD"]
        self.dump_data = json.dumps({"type": "subscribe",
                                     "product_ids": self.markets,
                                     "channels": self.channel,
                                     })

    def push(self, result, db):
        try:
            result.pop('type')
            df = pd.DataFrame([result])
            df.to_sql(result['product_id'],
                      con=db.connection,
                      if_exists='append')
        except KeyError:
            print(f'Successfully Subscribed to {result["channels"]}')

    def on_message(self, ws, message):
        print(message)
        res = json.loads(message)
        print(res)
        self.push(res, self.db)

    def on_error(self, ws, error):
        print(error)
        with open("logs/Coinbase.txt", 'a') as f:
            error = error + " " + str(datetime.datetime.now()) + "\n"
            f.write(error)

    def on_close(self, ws, close_status_code, close_msg):
        print("### closed ###")
        with open("logs/Coinbase.txt", 'a') as f:
            message = close_msg + " " + str(datetime.datetime.now()) + "\n"
            f.write(message)
        self.start()

    def on_open(self, ws):
        print("Opened connection")
        ws.send(self.dump_data)

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
