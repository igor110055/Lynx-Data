import websocket
import rel
import json
import pandas as pd
import logging
from logging import handlers
from sockets.data import Database

rfh = handlers.RotatingFileHandler(filename='logs/Coinbase.log',
                                   maxBytes=2.3 * 1024 * 1024, backupCount= 1,
                                   encoding=None,
                                   delay=0)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p', handlers=[rfh])
path = 'databases/Coinbase.db'
db = Database(path)
uri = "wss://ws-feed.exchange.coinbase.com"
channel = ["ticker"]
markets = ["DOT-USD", "ADA-USDC", "WBTC-USD", "AGLD-USD", "CHZ-EUR", "DOT-USDT", "ETH-DAI", "QNT-USD", "RNDR-USD", "FIL-BTC", "ZRX-BTC", "SOL-USDT", "ACH-USD", "API3-USD", "BAT-USD", "BAND-BTC", "AAVE-USD", "STORJ-BTC", "1INCH-EUR", "AIOZ-USDT", "TRU-USD", "MASK-USD", "ZEN-BTC", "NCT-USD", "BICO-USDT", "RAI-USD", "LCX-EUR", "BAND-GBP", "ANKR-USD", "FET-USD", "UMA-GBP", "QSP-USD", "MASK-USDT", "ZEC-USDC", "MANA-USD", "DIA-EUR", "POLS-USDT", "ENS-USDT", "XTZ-USD", "LINK-ETH", "EOS-USD", "CRO-USDT", "DASH-BTC", "BUSD-USD", "CLV-USDT", "BTRST-USD", "PAX-USD", "LPT-USD", "TRU-EUR", "SUSHI-BTC", "DDX-EUR", "AVAX-BTC", "QSP-USDT", "AUCTION-EUR", "SOL-GBP", "FORTH-EUR", "DNT-USDC", "OXT-USD", "LTC-BTC", "BLZ-USD", "MDT-USDT", "GRT-GBP", "NMR-GBP", "AUCTION-USD", "EOS-EUR", "ENJ-BTC", "KRL-EUR", "RLC-BTC", "LRC-USDT", "UNFI-USD", "MIR-EUR", "RNDR-USDT", "SOL-USD", "BAND-EUR", "MANA-BTC", "1INCH-BTC", "MATIC-USD", "NKN-EUR", "RAD-BTC", "MCO2-USDT", "POWR-USDT", "MIR-BTC", "FARM-USD", "HIGH-USD", "BTRST-BTC", "SUSHI-USD", "AVT-USD", "FOX-USD", "TRU-BTC", "ZEC-USD", "ALGO-USD", "BADGER-USD", "LQTY-EUR", "OMG-EUR", "XYO-USD", "SNX-USD", "TRB-USD", "POWR-USD", "OGN-USD", "DOGE-USDT", "POLY-USD", "IOTX-EUR", "ANKR-EUR", "STX-USD", "SKL-EUR", "KEEP-USD", "DNT-USD", "NKN-GBP", "GRT-BTC", "MCO2-USD", "POWR-EUR", "STX-USDT", "CGLD-USD", "BTC-GBP", "SUSHI-ETH", "ADA-EUR", "PRO-USD", "SHIB-USDT", "RAD-USD", "CHZ-USD", "PAX-USDT", "CLV-USD", "BICO-USD", "ASM-USD", "COMP-BTC", "NCT-USDT", "SKL-GBP", "TRAC-EUR", "MATIC-GBP", "SKL-BTC", "BAL-USD", "ORCA-USD", "ETC-EUR", "CVC-USDC", "STORJ-USD", "WCFG-EUR", "GMT-USD", "LTC-USD", "RLY-USD", "LRC-BTC", "GALA-USD", "YFI-BTC", "USDT-EUR", "SUSHI-GBP", "REQ-USD", "MUSD-USD", "NCT-EUR", "WBTC-BTC", "WCFG-GBP", "GLM-USD", "COTI-USD", "BICO-EUR", "ICP-USDT", "BAT-ETH", "FIDA-EUR", "ORN-BTC", "ALGO-EUR", "ICP-USD", "ALCX-USD", "WCFG-USDT", "BADGER-USDT", "BTC-USDT", "CGLD-BTC", "ETH-GBP", "BTC-EUR", "BOND-USD", "SPELL-USDT", "CLV-EUR", "BTRST-GBP", "DOGE-GBP", "UPI-USDT", "SHIB-GBP", "GTC-USD", "BTC-USD", "FIL-EUR", "FORTH-GBP", "APE-USD", "GFI-USD", "BNT-EUR", "BCH-GBP", "CGLD-EUR", "LINK-GBP", "COVAL-USD", "NKN-BTC", "ERN-USDT", "MDT-USD", "PERP-EUR", "FORTH-BTC", "CRO-EUR", "XLM-USD", "KRL-USD", "CLV-GBP", "NU-GBP", "SUPER-USD", "DESO-USD", "AERGO-USD", "ALGO-GBP", "YFI-USD", "ATOM-BTC", "ENJ-USDT", "APE-USDT", "BNT-BTC", "ETH-EUR", "SPELL-USD", "KSM-USD", "NMR-BTC", "FOX-USDT", "CHZ-GBP", "POLY-USDT", "AAVE-BTC", "ENS-USD", "GALA-EUR", "ROSE-USDT", "BAND-USD", "SHPING-USD", "KSM-USDT", "REN-BTC", "CGLD-GBP", "ADA-USD", "AXS-BTC", "AXS-EUR", "USDC-GBP", "ENJ-USD", "BADGER-EUR", "YFII-USD", "ERN-EUR", "SHPING-EUR", "ETC-USD", "UNI-BTC", "LINK-EUR", "ATOM-GBP", "NMR-EUR", "ARPA-USDT", "SOL-EUR", "COVAL-USDT", "CRO-USD", "MINA-USDT", "LTC-EUR", "ZEC-BTC", "SHIB-EUR", "UNI-EUR", "TRAC-USD", "SHIB-USD", "AUCTION-USDT", "OMG-GBP", "USDT-USD", "SHPING-USDT", "ETH-USD", "RGT-USD", "SUKU-EUR", "AAVE-GBP", "TRAC-USDT", "PLU-USD", "SNT-USD", "MATIC-USDT", "DIA-USDT", "FARM-USDT", "FORTH-USD", "AVAX-USD", "BCH-USD", "RLY-GBP", "SUSHI-EUR", "BTRST-USDT", "SKL-USD", "FLOW-USD", "IOTX-USD", "ARPA-EUR", "1INCH-GBP", "MPL-USD", "RLY-USDT", "DOT-GBP", "COMP-USD", "TRU-USDT", "RAD-USDT", "CRV-BTC", "CRV-USD", "MANA-ETH", "AAVE-EUR", "REQ-USDT", "REQ-BTC", "CRPT-USD", "GALA-USDT", "AIOZ-USD", "BAL-BTC", "EOS-BTC", "ALCX-EUR", "VGX-USDT", "ARPA-USD", "ZRX-USD", "ZRX-EUR", "AVAX-USDT", "XYO-EUR", "ANKR-GBP", "GMT-USDT", "MIR-GBP", "APE-EUR", "ATOM-EUR", "DOGE-EUR", "SAND-USD", "MKR-BTC", "CTX-EUR", "CRV-EUR", "GAL-USDT", "LRC-USD", "GRT-USD", "AXS-USDT", "IMX-USDT", "SAND-USDT", "REP-USD", "XYO-BTC", "ADA-BTC", "BTC-USDC", "MATIC-BTC", "CRV-GBP", "LQTY-USD", "DAI-USD", "KNC-USD", "MLN-USD", "RAD-EUR", "UNI-GBP", "DESO-USDT", "NU-BTC", "QUICK-USD", "ERN-USD", "ADA-GBP", "DOT-EUR", "FIL-GBP", "FLOW-USDT", "LINK-USDT", "NU-USD", "DOGE-USD", "ALICE-USD", "SNX-GBP", "MANA-EUR", "ETH-USDT", "WCFG-BTC", "MASK-EUR", "PERP-USD", "ROSE-USD", "USDT-GBP", "MATIC-EUR", "DDX-USD", "PERP-USDT", "ORN-USD", "NU-EUR", "ATOM-USDT", "REQ-GBP", "OP-USD", "ETH-USDC", "ETH-BTC", "BCH-BTC", "CVC-USD", "LINK-USD", "LCX-USD", "IMX-USD", "UPI-USD", "JASMY-USD", "XYO-USDT", "GODS-USD", "CTSI-USD", "AMP-USD", "MKR-USD", "SOL-BTC", "XTZ-BTC", "SOL-ETH", "USDT-USDC", "DIA-USD", "XLM-EUR", "ICP-BTC", "ALGO-BTC", "KRL-USDT", "SNX-EUR", "ZEN-USD", "ALCX-USDT", "DASH-USD", "LOOM-USDC", "LOOM-USD", "UNI-USD", "ASM-USDT", "NMR-USD", "MIR-USD", "INV-USD", "XTZ-GBP", "UMA-EUR", "DDX-USDT", "PLA-USD", "DESO-EUR", "SUKU-USDT", "RLC-USD", "RNDR-EUR", "ICP-GBP", "SUPER-USDT", "FIDA-USD", "API3-USDT", "QNT-USDT", "MINA-EUR", "BCH-EUR", "REP-BTC", "1INCH-USD", "TRIBE-USD", "GST-USD", "ETC-GBP", "CTX-USDT", "WCFG-USD", "GAL-USD", "LCX-USDT", "AGLD-USDT", "MASK-GBP", "CHZ-USDT", "IDEX-USDT", "BNT-GBP", "LTC-GBP", "ANKR-BTC", "XTZ-EUR", "XLM-USDT", "CTX-USD", "MINA-USD", "BAT-BTC", "ZEN-USDT", "LINK-BTC", "DOT-BTC", "UMA-BTC", "REQ-EUR", "ATOM-USD", "SNX-BTC", "ADA-USDT", "KNC-BTC", "ADA-ETH", "POLS-USD", "RARI-USD", "BAT-USDC", "BAT-EUR", "RAD-GBP", "ORN-USDT", "MANA-USDC", "DAI-USDC", "ENS-EUR", "ETC-BTC", "IDEX-USD", "AXS-USD", "RLY-EUR", "FX-USD", "ICP-EUR", "NKN-USD", "VGX-USD", "REN-USD", "VGX-EUR", "SYN-USD", "AVAX-EUR", "CTSI-BTC", "USDC-EUR", "LQTY-USDT", "RBN-USD", "FET-USDT", "OMG-USD", "OMG-BTC", "FIL-USD", "OGN-BTC", "DOGE-BTC", "FIDA-USDT", "TRB-BTC", "BTRST-EUR", "GYEN-USD", "XLM-BTC", "UMA-USD", "GRT-EUR", "SUKU-USD", "BNT-USD", "XRP-BTC", "WLUNA-EUR", "XRP-EUR", "UST-USD", "GNT-USDC", "WLUNA-GBP", "WLUNA-USD", "UST-USDT", "UST-EUR", "XRP-USD", "XRP-GBP", "WLUNA-USDT"]
dump_data = json.dumps({"type": "subscribe",
                        "product_ids": markets,
                        "channels": channel,
                        })


def write_logs(log):
    logging.info(log)


def push(result, db):
    try:
        result.pop('type')
        df = pd.DataFrame([result])
        df.to_sql(result['product_id'],
                  con=db.connection,
                  if_exists='append')
    except KeyError:
        print(f'Successfully Subscribed to {result["channels"]}')


def on_message(ws, message):
    print(message)
    res = json.loads(message)
    print(res)
    push(res, db)


def on_open(ws):
    print("Opened connection")
    ws.send(dump_data)


def on_error(ws, error):
    print(error)
    write_logs(error)


def on_close(ws, close_status_code, close_msg):
    print("### closed ###")
    write_logs(str(close_status_code) + str(close_msg))
    start()


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
