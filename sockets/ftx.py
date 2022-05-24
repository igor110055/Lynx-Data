import websocket
import rel
import datetime
import json
import pandas as pd
from sockets.data import Database


class Ftx:
    def __init__(self):
        self.path = 'databases/Ftx.db'
        self.db = Database(self.path)
        self.uri = "wss://ftx.com/ws/"
        self.channel = 'ticker'
        self.markets = ['BNB/USDT', 'BTC/USDT']

    def push(self, res, db):
        try:
            df = pd.DataFrame([res['data']])
            df.to_sql(res['market'],
                      con=db.connection,
                      if_exists='append')
        except KeyError:
            print(f'Successfully subscribed to {res["market"]}')

    def on_message(self, ws, message):
        res = json.loads(message)
        self.push(res, self.db,)

    def on_error(self, ws, error):
        print(error)
        with open("logs/FTX.txt", 'a') as f:
            error = str(error) + " " + str(datetime.datetime.now()) + "\n"
            f.write(error)

    def on_close(self, ws, close_status_code, close_msg):
        print("### closed ###")
        with open("logs/FTX.txt", 'a') as f:
            message = str(close_msg) + " " + str(datetime.datetime.now()) + "\n"
            f.write(message)
        self.start()

    def on_open(self, ws):
        print("Opened connection")
        for market in self.markets:
            data = json.dumps({'op': 'subscribe',
                               'channel': self.channel,
                               'market': market})
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

