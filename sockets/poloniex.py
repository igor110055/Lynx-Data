import websocket
import rel
import json
import pandas as pd
import datetime
import logging
from logging import handlers
from sockets.data import Database

rfh = handlers.RotatingFileHandler(filename='logs/Poloniex.log',
                                   maxBytes=2.3 * 1024 * 1024, backupCount= 1,
                                   encoding=None,
                                   delay=0)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p', handlers=[rfh])
path = 'databases/Poloniex.db'
db = Database(path)
uri = "wss://api2.poloniex.com"
channel = 1002
headers = ["currency pair id",
           "last trade price",
           "lowest ask", "highest bid",
           "percent change in last 24 hours",
           "base currency volume in last 24 hours",
           "quote currency volume in last 24 hours",
           "is frozen",
           "highest trade price in last 24 hours",
           "lowest trade price in last 24 hours",
           "post only", "maintenance mode"]
data = json.dumps({"command": "subscribe",
                   "channel": channel,
                   "currency pair id": "149"
                   })


def write_logs(log):
    logging.info(log)


def split(data):
    return list(data.split(','))


def push(final_data, db, headers):
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


def on_message(ws, message):
    final_data = split(message[12:-2])
    push(final_data, db, headers)


def on_error(ws, error):
    print(error)
    write_logs(error)


def on_close(ws, close_status_code, close_msg):
    print("### closed ###")
    write_logs(str(close_status_code) + str(close_msg))
    start()


def on_open(ws):
    print("Opened connection")
    ws.send(data)


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
