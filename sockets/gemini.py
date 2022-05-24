import websocket
import pandas as pd
import rel
import datetime
from sockets.data import Database


class Gemini:
    def __init__(self):
        self.uri = "wss://api.gemini.com/v1/multimarketdata?symbols="
        self.market = ['BTCUSD', 'ETHUSD']
        self.url = ','.join([str(element) for element in self.market])
        self.path = "databases/Gemini.db"
        self.db = Database(self.path)

    def push(self, res, db):
        res = eval(res)
        data = res['events'][0]
        data['timestamp'] = res['timestampms']
        df = pd.DataFrame([data])
        df.to_sql(res['events'][0]['symbol'],
                  con=db.connection,
                  if_exists='append')

    def on_message(self, ws, message):
        print(message)
        self.push(message, self.db)

    def on_error(self, ws, error):
        print(error)
        with open("logs/Gemini.txt", 'a') as f:
            error = str(error) + " " + str(datetime.datetime.now()) + "\n"
            f.write(error)

    def on_close(self, ws, close_status_code, close_msg):
        print("### closed ###")
        with open("logs/Gemini.txt", 'a') as f:
            message = str(close_msg) + " " + str(datetime.datetime.now()) + "\n"
            f.write(message)
        self.start()

    def on_open(self, ws):
        print("Opened connection")

    def start(self):
        websocket.enableTrace(True)
        print(self.url)
        ws = websocket.WebSocketApp(self.uri + self.url,
                                    on_open=self.on_open,
                                    on_message=self.on_message,
                                    on_error=self.on_error,
                                    on_close=self.on_close)

        ws.run_forever(dispatcher=rel)  # Set dispatcher to automatic reconnection
        rel.signal(2, rel.abort)  # Keyboard Interrupt
        rel.dispatch()
