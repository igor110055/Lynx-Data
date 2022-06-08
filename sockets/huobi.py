import websocket
import rel
import json
import gzip
import pandas as pd
import datetime
import logging
from logging import handlers
from sockets.data import Database

rfh = handlers.RotatingFileHandler(filename='logs/Huobi.log',
                                   maxBytes=2.3 * 1024 * 1024, backupCount= 1,
                                   encoding=None,
                                   delay=0)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p', handlers=[rfh])
path = 'databases/Huobi.db'
db = Database(path)
uri = "wss://api.huobi.pro/ws"
markets = ['bnbusdt', 'btcusdt']


def write_logs(log):
    logging.info(log)


def push(res, db):
    uncompressed_data = gzip.decompress(res)
    final_data = json.loads(uncompressed_data)
    try:
        final_data['tick']['time'] = datetime.datetime.now()
        df = pd.DataFrame([final_data['tick']])
        df.to_sql(final_data['ch'].split('.')[1],
                  con=db.connection,
                  if_exists='append')
    except KeyError:
        pass


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
    for market in markets:
        data = json.dumps({"sub": f"market.{market}.ticker"})
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


