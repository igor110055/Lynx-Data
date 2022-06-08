

#FIX DATA HANDLING

import websocket
import pandas as pd
import rel
import json
import logging
from logging import handlers
from sockets.data import Database

rfh = handlers.RotatingFileHandler(filename='logs/Bybit.log',
                                   maxBytes=2.3 * 1024 * 1024, backupCount= 1,
                                   encoding=None,
                                   delay=0)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p', handlers=[rfh])
uri = "wss://stream.bybit.com/spot/quote/ws/v1"
markets = ["instrument_info.100ms.BTCUSD", "instrument_info.100ms.ETHUSD", "instrument_info.100ms.BNBUSD", "instrument_info.100ms.ADAUSD", "instrument_info.100ms.XRPUSD", "instrument_info.100ms.SOLUSD", "instrument_info.100ms.DOGEUSD", "instrument_info.100ms.DOTUSD", "instrument_info.100ms.WBTCUSD", "instrument_info.100ms.TRXUSD", "instrument_info.100ms.DAIUSD", "instrument_info.100ms.AVAXUSD", "instrument_info.100ms.SHIBUSD", "instrument_info.100ms.MATICUSD", "instrument_info.100ms.CROUSD", "instrument_info.100ms.LTCUSD", "instrument_info.100ms.NEARUSD", "instrument_info.100ms.UNIUSD", "instrument_info.100ms.XLMUSD", "instrument_info.100ms.FTTUSD", "instrument_info.100ms.BCHUSD", "instrument_info.100ms.XMRUSD", "instrument_info.100ms.LINKUSD", "instrument_info.100ms.ETCUSD", "instrument_info.100ms.ATOMUSD", "instrument_info.100ms.ALGOUSD", "instrument_info.100ms.FLOWUSD", "instrument_info.100ms.ICPUSD", "instrument_info.100ms.VETUSD", "instrument_info.100ms.HBARUSD", "instrument_info.100ms.MANAUSD", "instrument_info.100ms.APEUSD", "instrument_info.100ms.XTZUSD", "instrument_info.100ms.EGLDUSD", "instrument_info.100ms.SANDUSD", "instrument_info.100ms.FILUSD", "instrument_info.100ms.AAVEUSD", "instrument_info.100ms.ZECUSD", "instrument_info.100ms.EOSUSD", "instrument_info.100ms.AXSUSD", "instrument_info.100ms.THETAUSD", "instrument_info.100ms.KLAYUSD", "instrument_info.100ms.MKRUSD", "instrument_info.100ms.WAVES"]
path = "databases/Bybit.db"
db = Database(path)
subdata = json.dumps({"op": "subscribe", "args": markets})


def write_logs(log):
    logging.info(log)


def push(res, db):
    res = eval(res)
    data = res['data']['update'][0]
    df = pd.DataFrame([data])
    print(df)
    df.to_sql(res['data']['update'][0]['symbol'],
              con=db.connection,
              if_exists='append')


def on_message(ws, message):
    print(message)
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
