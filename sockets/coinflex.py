import websocket
import pandas as pd
import rel
import datetime
import json
from sockets.data import Database


class Coinflex:
    def __init__(self):
        self.uri = "wss://v2api.coinflex.com/v2/websocket"
        self.path = "databases/Coinflex.db"
        self.db = Database(self.path)
        self.subdata = json.dumps({
                                    "op": "subscribe",
                                    "tag": 1,
                                    "args": ["ticker:all"]
                                    })

    def push(self, res, db):
        data = json.loads(res)
        data = data['data']
        for items in data:
            df = pd.DataFrame([items])
            df.to_sql(items['marketCode'],
                      con=db.connection,
                      if_exists='append')

    def on_message(self, ws, message):
        self.push(message, self.db)

    def on_error(self, ws, error):
        print(error)
        with open("logs/Coinflex.txt", 'a') as f:
            error = str(error) + " " + str(datetime.datetime.now()) + "\n"
            f.write(error)

    def on_close(self, ws, close_status_code, close_msg):
        print("### closed ###")
        with open("logs/Coinflex.txt", 'a') as f:
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
