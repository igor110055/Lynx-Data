import websocket
import pandas as pd
import rel
import json
from logging import handlers
import logging
from sockets.data import Database

rfh = handlers.RotatingFileHandler(filename='logs/Binance.log',
                                   maxBytes=2.3 * 1024 * 1024, backupCount= 1,
                                   encoding=None,
                                   delay=0)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p', handlers=[rfh])
uri = "wss://stream.binance.com:9443/ws/!miniTicker@arr"
path = "databases/Binance.db"
db = Database(path)


def write_logs(log):
    logging.info(log)


def push(res, db):
    res = json.loads(res)
    print(res)
    for item in res:
        item.pop('e')
        print(item)
        df = pd.DataFrame([item])
        df.to_sql(item['s'],
                  con=db.connection,
                  if_exists='append')


def on_message(ws, message):
    push(message, db)


def on_error(ws, error):
    print(error)
    write_logs(error)


def on_close(ws, close_status_code, close_msg):
    print("### closed ###")
    write_logs(str(close_status_code) + str(close_msg))
    start()


def on_open(ws):
    print("Opened connection")


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
