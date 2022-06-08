import websocket
import pandas as pd
import rel
import json
import logging
from logging import handlers
from sockets.data import Database

rfh = handlers.RotatingFileHandler(filename='logs/Coinflex.log',
                                   maxBytes=2.3 * 1024 * 1024, backupCount= 1,
                                   encoding=None,
                                   delay=0)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p', handlers=[rfh])

uri = "wss://v2api.coinflex.com/v2/websocket"
path = "databases/Coinflex.db"
db = Database(path)
subdata = json.dumps({
    "op": "subscribe",
    "tag": 1,
    "args": ["ticker:all"]
})


def push(res):
    data = json.loads(res)
    try:
        data = data['data']
        for items in data:
            df = pd.DataFrame([items])
            df.to_sql(items['marketCode'],
                      con=db.connection,
                      if_exists='append')
    except :
        pass

def write_logs(log):
    logging.info(log)


def on_message(ws, message):
    push(message)


def on_error(ws, error):
    print(error)
    write_logs(error)


def on_close(ws, close_status_code, close_msg):
    print("### closed ###")
    write_logs(str(close_status_code) + str(close_msg))
    start()


def on_open(ws):
    print("Opened connection")
    ws.send(subdata)


def start():
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp(uri,
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)

    ws.run_forever(dispatcher=rel)  # Set dispatcher to automatic reconnection
    rel.signal(2, rel.abort)  # Keyboard Interrupt
    rel.dispatch()
