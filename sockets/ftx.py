import websocket
import rel
import json
import pandas as pd
import logging
from logging import handlers
from sockets.data import Database


rfh = handlers.RotatingFileHandler(filename='logs/Ftx.log',
                                   maxBytes=2.3 * 1024 * 1024, backupCount= 1,
                                   encoding=None,
                                   delay=0)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p', handlers=[rfh])
path = 'databases/Ftx.db'
db = Database(path)
uri = "wss://ftx.com/ws/"
channel = 'ticker'
markets = ["1INCH/USD", "AAPL/USD", "AAVE/USD", "ABNB/USD", "ACB/USD", "AGLD/USD", "AKRO/USD", "ALCX/USD", "ALEPH/USD", "ALGO/USD", "ALICE/USD", "ALPHA/USD", "AMC/USD", "AMD/USD", "AMPL/USD", "AMZN/USD", "ANC/USD", "APE/USD", "APHA/USD", "ARKK/USD", "ASD/USD", "ATLAS/USD", "ATOM/USD", "AUDIO/USD", "AURY/USD", "AVAX/USD", "AXS/USD", "BABA/USD", "BADGER/USD", "BAL/USD", "BAND/USD", "BAO/USD", "BAR/USD", "BAT/USD", "BB/USD", "BCH/USD", "BICO/USD", "BILI/USD", "BIT/USD", "BITO/USD", "BITW/USD", "BLT/USD", "BNB/USD", "BNT/USD", "BNTX/USD", "BOBA/USD", "BRZ/USD", "BTC/USD", "BTT/USD", "BYND/USD", "C98/USD", "CAD/USD", "CEL/USD", "CGC/USD", "CHR/USD", "CHZ/USD", "CITY/USD", "CLV/USD", "COIN/USD", "COMP/USD", "CONV/USD", "COPE/USD", "CQT/USD", "CREAM/USD", "CRO/USD", "CRON/USD", "CRV/USD", "CTX/USD", "CVC/USD", "CVX/USD", "DAI/USD", "DAWN/USD", "DENT/USD", "DFL/USD", "DKNG/USD", "DMG/USD", "DODO/USD", "DOGE/USD", "DOT/USD", "DYDX/USD", "EDEN/USD", "EMB/USD", "ENJ/USD", "ENS/USD", "ETH/USD", "ETHE/USD", "EUR/USD", "EURT/USD", "FB/USD", "FIDA/USD", "FRONT/USD", "FTM/USD", "FTT/USD", "FXS/USD", "GAL/USD", "GALA/USD", "GALFAN/USD", "GARI/USD", "GBP/USD", "GBTC/USD", "GDX/USD", "GDXJ/USD", "GENE/USD", "GLD/USD", "GLXY/USD", "GME/USD", "GMT/USD", "GODS/USD", "GOG/USD", "GOOGL/USD", "GRT/USD", "GST/USD", "GT/USD", "HGET/USD", "HMT/USD", "HNT/USD", "HOLY/USD", "HOOD/USD", "HT/USD", "HUM/USD", "HXRO/USD", "IMX/USD", "INDI/USD", "INTER/USD", "JET/USD", "JOE/USD", "JST/USD", "KBTT/USD", "KIN/USD", "KNC/USD", "KSHIB/USD", "KSOS/USD", "LEO/USD", "LINA/USD", "LINK/USD", "LOOKS/USD", "LRC/USD", "LTC/USD", "LUA/USD", "MANA/USD", "MAPS/USD", "MATH/USD", "MATIC/USD", "MBS/USD", "MCB/USD", "MEDIA/USD", "MER/USD", "MKR/USD", "MNGO/USD", "MOB/USD", "MRNA/USD", "MSOL/USD", "MSTR/USD", "MTA/USD", "MTL/USD", "NEAR/USD", "NEXO/USD", "NFLX/USD", "NIO/USD", "NOK/USD", "NVDA/USD", "OKB/USD", "OMG/USD", "ORBS/USD", "OXY/USD", "PAXG/USD", "PENN/USD", "PEOPLE/USD", "PERP/USD", "PFE/USD", "POLIS/USD", "PORT/USD", "PRISM/USD", "PROM/USD", "PSG/USD", "PSY/USD", "PTU/USD", "PUNDIX/USD", "PYPL/USD", "QI/USD", "RAMP/USD", "RAY/USD", "REAL/USD", "REEF/USD", "REN/USD", "RNDR/USD", "ROOK/USD", "RSR/USD", "RUNE/USD", "SAND/USD", "SECO/USD", "SHIB/USD", "SKL/USD", "SLND/USD", "SLP/USD", "SLRS/USD", "SLV/USD", "SNX/USD", "SNY/USD", "SOL/USD", "SOS/USD", "SPA/USD", "SPELL/USD", "SPY/USD", "SQ/USD", "SRM/USD", "STARS/USD", "STEP/USD", "STETH/USD", "STG/USD", "STMX/USD", "STORJ/USD", "STSOL/USD", "SUN/USD", "SUSHI/USD", "SXP/USD", "TLM/USD", "TLRY/USD", "TOMO/USD", "TONCOIN/USD", "TRU/USD", "TRX/USD", "TRYB/USD", "TSLA/USD", "TSM/USD", "TULIP/USD", "TWTR/USD", "UBER/USD", "UBXT/USD", "UMEE/USD", "UNI/USD", "USO/USD", "VGX/USD", "WAVES/USD", "WBTC/USD", "WFLOW/USD", "WNDR/USD", "WRX/USD", "XAUT/USD", "XRP/USD", "YFI/USD", "YFII/USD", "YGG/USD", "ZM/USD", "ZRX/USD", "ADABEAR/USD", "ADABULL/USD", "ADAHALF/USD", "ADAHEDGE/USD", "ALGOBEAR/USD", "ALGOBULL/USD", "ALGOHALF/USD", "ALGOHEDGE/USD", "ALTBEAR/USD", "ALTBULL/USD", "ALTHALF/USD", "ALTHEDGE/USD", "ASDBEAR/USD", "ASDBULL/USD", "ASDHALF/USD", "ASDHEDGE/USD", "ATOMBEAR/USD", "ATOMBULL/USD", "ATOMHALF/USD", "ATOMHEDGE/USD", "BALBEAR/USD", "BALBULL/USD", "BALHALF/USD", "BALHEDGE/USD", "BCHBEAR/USD", "BCHBULL/USD", "BCHHALF/USD", "BCHHEDGE/USD", "BEAR/USD", "BEARSHIT/USD", "BNBBEAR/USD", "BNBBULL/USD", "BNBHALF/USD", "BNBHEDGE/USD", "BSVBEAR/USD", "BSVBULL/USD", "BSVHALF/USD", "BSVHEDGE/USD", "BULL/USD", "BULLSHIT/USD", "BVOL/USD", "COMPBEAR/USD", "COMPBULL/USD", "COMPHALF/USD", "COMPHEDGE/USD", "DEFIBEAR/USD", "DEFIBULL/USD", "DEFIHALF/USD", "DEFIHEDGE/USD", "DOGEBEAR2021/USD", "DOGEBULL/USD", "DOGEHALF/USD", "DOGEHEDGE/USD", "DRGNBEAR/USD", "DRGNBULL/USD", "DRGNHALF/USD", "DRGNHEDGE/USD", "EOSBEAR/USD", "EOSBULL/USD", "EOSHALF/USD", "EOSHEDGE/USD", "ETCBEAR/USD", "ETCBULL/USD", "ETCHALF/USD", "ETCHEDGE/USD", "ETHBEAR/USD", "ETHBULL/USD", "ETHHALF/USD", "ETHHEDGE/USD", "EXCHBEAR/USD", "EXCHBULL/USD", "EXCHHALF/USD", "EXCHHEDGE/USD", "GRTBEAR/USD", "GRTBULL/USD", "HALF/USD", "HALFSHIT/USD", "HEDGE/USD", "HEDGESHIT/USD", "HTBEAR/USD", "HTBULL/USD", "HTHALF/USD", "HTHEDGE/USD", "IBVOL/USD", "KNCBEAR/USD", "KNCBULL/USD", "KNCHALF/USD", "KNCHEDGE/USD", "LEOBEAR/USD", "LEOBULL/USD", "LEOHALF/USD", "LEOHEDGE/USD", "LINKBEAR/USD", "LINKBULL/USD", "LINKHALF/USD", "LINKHEDGE/USD", "LTCBEAR/USD", "LTCBULL/USD", "LTCHALF/USD", "LTCHEDGE/USD", "MATICBEAR2021/USD", "MATICBULL/USD", "MATICHALF/USD", "MATICHEDGE/USD", "MIDBEAR/USD", "MIDBULL/USD", "MIDHALF/USD", "MIDHEDGE/USD", "MKRBEAR/USD", "MKRBULL/USD", "OKBBEAR/USD", "OKBBULL/USD", "OKBHALF/USD", "OKBHEDGE/USD", "PAXGBEAR/USD", "PAXGBULL/USD", "PAXGHALF/USD", "PAXGHEDGE/USD", "PRIVBEAR/USD", "PRIVBULL/USD", "PRIVHALF/USD", "PRIVHEDGE/USD", "SUSHIBEAR/USD", "SUSHIBULL/USD", "SXPBEAR/USD", "SXPBULL/USD", "SXPHALF/USD", "SXPHEDGE/USD", "THETABEAR/USD", "THETABULL/USD", "THETAHALF/USD", "THETAHEDGE/USD", "TOMOBEAR2021/USD", "TOMOBULL/USD", "TOMOHALF/USD", "TOMOHEDGE/USD", "TRXBEAR/USD", "TRXBULL/USD", "TRXHALF/USD", "TRXHEDGE/USD", "TRYBBEAR/USD", "TRYBBULL/USD", "TRYBHALF/USD", "TRYBHEDGE/USD", "UNISWAPBEAR/USD", "UNISWAPBULL/USD", "VETBEAR/USD", "VETBULL/USD", "VETHEDGE/USD", "XAUTBEAR/USD", "XAUTBULL/USD", "XAUTHALF/USD", "XAUTHEDGE/USD", "XLMBEAR/USD", "XLMBULL/USD", "XRPBEAR/USD", "XRPBULL/USD", "XRPHALF/USD", "XRPHEDGE/USD", "XTZBEAR/USD", "XTZBULL/USD", "XTZHALF/USD", "XTZHEDGE/USD", "ZECBEAR/USD", "ZECBULL/USD"]
def write_logs(log):
    logging.info(log)

def push(res, db):
    try:
        df = pd.DataFrame([res['data']])
        df.to_sql(res['market'],
                  con=db.connection,
                  if_exists='append')
    except KeyError:
        print(f'Successfully subscribed to {res["market"]}')

def on_message(ws, message):
    res = json.loads(message)
    push(res, db,)

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
        data = json.dumps({'op': 'subscribe',
                           'channel': channel,
                           'market': market})
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

