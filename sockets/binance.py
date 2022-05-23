import websocket
import pandas as pd
import rel
from sockets.data import Database


class Binance:
    def __init__(self):
        self.uri = "wss://stream.binance.com"
        self.markets = ['bnbusdt@miniTicker', 'btcusdt@miniTicker']
        self.stream = '/'.join(self.markets)
        self.path = "databases/Binance.db"
        self.db = Database(self.path)
        self.subdata = {"method": "SUBSCRIBE",
                        "params": self.markets,
                        "id": 1}

    def push(self, res, db):
        res = eval(res)
        type(res)
        data = res['data']
        data.pop('e')
        df = pd.DataFrame([data])
        df.to_sql(res['stream'].split('@', 1)[0],
                  con=db.connection,
                  if_exists='append')

    def on_message(self, ws, message):
        print(message)
        self.push(message, self.db)

    def on_error(self, ws, error):
        print(error)

    def on_close(self, ws, close_status_code, close_msg):
        print("### closed ###")

    def on_open(self, ws):
        print("Opened connection")

    def start(self):
        websocket.enableTrace(True)
        ws = websocket.WebSocketApp(f"wss://stream.binance.com/stream?streams=" + self.stream,
                                    on_open=self.on_open,
                                    on_message=self.on_message,
                                    on_error=self.on_error,
                                    on_close=self.on_close)

        ws.run_forever(dispatcher=rel)  # Set dispatcher to automatic reconnection
        rel.signal(2, rel.abort)  # Keyboard Interrupt
        rel.dispatch()
