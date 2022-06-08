import websocket
import pandas as pd
import rel
import logging
from logging import handlers
from sockets.data import Database

rfh = handlers.RotatingFileHandler(filename='logs/Gemini.log',
                                   maxBytes=2.3 * 1024 * 1024, backupCount= 1,
                                   encoding=None,
                                   delay=0)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p', handlers=[rfh])
uri = "wss://api.gemini.com/v1/multimarketdata?symbols="
market = ['btcusd', 'ethbtc', 'ethusd', 'zecusd', 'zecbtc', 'zeceth', 'zecbch', 'zecltc', 'bchusd', 'bchbtc', 'bcheth', 'ltcusd', 'ltcbtc', 'ltceth', 'ltcbch', 'batusd', 'daiusd', 'linkusd', 'oxtusd', 'batbtc', 'linkbtc', 'oxtbtc', 'bateth', 'linketh','oxteth', 'ampusd', 'compusd', 'paxgusd', 'mkrusd', 'zrxusd', 'kncusd', 'manausd', 'storjusd', 'snxusd', 'crvusd', 'balusd', 'uniusd', 'renusd', 'umausd', 'yfiusd', 'btcdai', 'ethdai', 'aaveusd', 'filusd', 'btceur', 'btcgbp', 'etheur', 'ethgbp', 'btcsgd', 'ethsgd', 'sklusd', 'grtusd', 'bntusd', '1inchusd', 'enjusd', 'lrcusd', 'sandusd', 'cubeusd', 'lptusd', 'bondusd', 'maticusd', 'injusd', 'sushiusd', 'dogeusd', 'alcxusd', 'mirusd', 'ftmusd', 'ankrusd', 'btcgusd', 'ethgusd', 'ctxusd', 'xtzusd', 'axsusd', 'slpusd', 'lunausd', 'ustusd', 'mco2usd', 'dogebtc', 'dogeeth', 'wcfgusd', 'rareusd', 'radusd', 'qntusd', 'nmrusd', 'maskusd', 'fetusd', 'ashusd', 'audiousd', 'api3usd', 'usdcusd', 'shibusd', 'rndrusd', 'mcusd', 'galausd', 'ensusd', 'kp3rusd', 'cvcusd', 'mimusd', 'spellusd', 'tokeusd', 'ldousd', 'rlyusd', 'solusd', 'rayusd', 'sbrusd', 'apeusd', 'rbnusd', 'fxsusd', 'dpiusd', 'lqtyusd', 'lusdusd', 'fraxusd', 'indexusd', 'mplusd', 'gusdsgd', 'metisusd','qrdousd', 'zbcusd', 'chzusd', 'revvusd', 'jamusd', 'fidausd', 'gmtusd', 'orcausd', 'gfiusd']
url = ','.join([str(element) for element in market])
path = "databases/Gemini.db"
db = Database(path)


def write_logs(log):
    logging.info(log)


# def push(res):
#     res = eval(res)
#     data = res['events'][0]
#     data['timestamp'] = res['timestampms']
#     df = pd.DataFrame([data])
#     df.to_sql(res['events'][0]['symbol'],
#               con=db.connection,
#               if_exists='append')


def on_message(ws, message):
    print(message)
    # push(message)


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
    print(url)
    ws = websocket.WebSocketApp(uri + url,
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)

    ws.run_forever(dispatcher=rel)  # Set dispatcher to automatic reconnection
    rel.signal(2, rel.abort)  # Keyboard Interrupt
    rel.dispatch()
