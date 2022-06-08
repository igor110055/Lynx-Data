import websocket
import json
import pandas as pd
import datetime
import rel
import logging
from logging import handlers
from sockets.data import Database

rfh = handlers.RotatingFileHandler(filename='logs/Bitfinex.log',
                                   maxBytes=5 * 1024 * 1024,
                                   backupCount=1,
                                   encoding=None,
                                   delay=0)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p', handlers=[rfh])
path = 'databases/Bitfinex.db'
db = Database(path)
uri = "wss://api-pub.bitfinex.com/ws/2"
channel = ['ticker']
markets = ['tBTCUSD', 'tLTCUSD', 'tLTCBTC', 'tETHUSD', 'tETHBTC', 'tETCBTC', 'tETCUSD', 'tRRTUSD', 'tZECUSD', 'tZECBTC', 'tXMRUSD', 'tXMRBTC', 'tDSHUSD', 'tDSHBTC', 'tBTCEUR', 'tBTCJPY', 'tXRPUSD', 'tXRPBTC', 'tIOTUSD', 'tIOTBTC', 'tIOTETH', 'tEOSUSD', 'tEOSBTC', 'tEOSETH', 'tSANUSD', 'tOMGUSD', 'tOMGBTC', 'tOMGETH', 'tNEOUSD', 'tNEOBTC', 'tNEOETH', 'tETPUSD', 'tETPBTC', 'tQTMUSD', 'tQTMBTC', 'tEDOUSD', 'tBTGUSD', 'tBTGBTC', 'tDATUSD', 'tGNTUSD', 'tSNTUSD', 'tIOTEUR', 'tBATUSD', 'tMNAUSD', 'tMNABTC', 'tFUNUSD', 'tZRXUSD', 'tZRXBTC', 'tZRXETH', 'tTRXUSD', 'tTRXBTC', 'tTRXETH', 'tREPUSD', 'tREPBTC', 'tBTCGBP', 'tETHEUR', 'tETHJPY', 'tETHGBP', 'tNEOEUR', 'tNEOJPY', 'tNEOGBP', 'tEOSEUR', 'tEOSJPY', 'tEOSGBP', 'tIOTJPY', 'tIOTGBP', 'tREQUSD', 'tLRCUSD', 'tWAXUSD', 'tDAIUSD', 'tDAIBTC', 'tDAIETH', 'tBFTUSD', 'tANTUSD', 'tANTBTC', 'tANTETH', 'tSTJUSD', 'tXLMUSD', 'tXLMBTC', 'tXLMETH', 'tXVGUSD', 'tMKRUSD', 'tKNCUSD', 'tKNCBTC', 'tLYMUSD', 'tUTKUSD', 'tVEEUSD', 'tZCNUSD', 'tESSUSD', 'tIQXUSD', 'tZILUSD', 'tZILBTC', 'tBNTUSD', 'tXRAUSD', 'tVETUSD', 'tVETBTC', 'tGOTUSD', 'tXTZUSD', 'tXTZBTC', 'tTRXEUR', 'tMLNUSD', 'tOMNUSD', 'tPNKUSD', 'tPNKETH', 'tDGBUSD', 'tBSVUSD', 'tBSVBTC', 'tENJUSD', 'tRBTUSD', 'tRBTBTC', 'tUSTUSD', 'tEUTEUR', 'tEUTUSD', 'tUDCUSD', 'tTSDUSD', 'tPAXUSD', 'tPASUSD', 'tVSYUSD', 'tVSYBTC', 'tBTTUSD', 'tBTCUST', 'tETHUST', 'tCLOUSD', 'tLTCUST', 'tEOSUST', 'tGNOUSD', 'tATOUSD', 'tATOBTC', 'tATOETH', 'tWBTUSD', 'tXCHUSD', 'tEUSUSD', 'tLEOUSD', 'tLEOBTC', 'tLEOUST', 'tLEOETH', 'tGTXUSD', 'tKANUSD', 'tGTXUST', 'tKANUST', 'tAMPUSD', 'tALGUSD', 'tALGBTC', 'tALGUST', 'tAMPUST', 'tDUSK:USD', 'tDUSK:BTC', 'tUOSUSD', 'tUOSBTC', 'tAMPBTC', 'tFTTUSD', 'tFTTUST', 'tPAXUST', 'tUDCUST', 'tTSDUST', 'tBTC:CNHT', 'tUST:CNHT', 'tCNH:CNHT', 'tCHZUSD', 'tCHZUST', 'tXAUT:USD', 'tXAUT:BTC', 'tXAUT:UST', 'tBTSE:USD', 'tTESTBTC:TESTUSD', 'tTESTBTC:TESTUSDT', 'tAAABBB', 'tDOGUSD', 'tDOGBTC', 'tDOGUST', 'tDOTUSD', 'tADAUSD', 'tADABTC', 'tADAUST', 'tFETUSD', 'tFETUST', 'tDOTUST', 'tLINK:USD', 'tLINK:UST', 'tCOMP:USD', 'tCOMP:UST', 'tKSMUSD', 'tKSMUST', 'tEGLD:USD', 'tEGLD:UST', 'tUNIUSD', 'tUNIUST', 'tBAND:USD', 'tBAND:UST', 'tAVAX:USD', 'tAVAX:UST', 'tSNXUSD', 'tSNXUST', 'tYFIUSD', 'tYFIUST', 'tBALUSD', 'tBALUST', 'tFILUSD', 'tFILUST', 'tJSTUSD', 'tJSTBTC', 'tJSTUST', 'tIQXUST', 'tBCHABC:USD', 'tBCHN:USD', 'tXDCUSD', 'tXDCUST', 'tPLUUSD', 'tSUNUSD', 'tSUNUST', 'tEUTUST', 'tXMRUST', 'tXRPUST', 'tB21X:USD', 'tB21X:UST', 'tSUSHI:USD', 'tSUSHI:UST', 'tDOTBTC', 'tETH2X:USD', 'tETH2X:UST', 'tETH2X:ETH', 'tAAVE:USD', 'tAAVE:UST', 'tXLMUST', 'tCTKUSD', 'tCTKUST', 'tSOLUSD', 'tSOLUST', 'tBEST:USD', 'tALBT:USD', 'tALBT:UST', 'tCELUSD', 'tCELUST', 'tSUKU:USD', 'tSUKU:UST', 'tBMIUSD', 'tBMIUST', 'tMOBUSD', 'tMOBUST', 'tNEAR:USD', 'tNEAR:UST', 'tBOSON:USD', 'tBOSON:UST', 'tLUNA:USD', 'tLUNA:UST', 'tICEUSD', 'tDOGE:USD', 'tDOGE:UST', 'tOXYUSD', 'tOXYUST', 't1INCH:USD', 't1INCH:UST', 'tIDXUSD', 'tFORTH:USD', 'tFORTH:UST', 'tIDXUST', 'tCHEX:USD', 'tQTFUSD', 'tQTFBTC', 'tOCEAN:USD', 'tOCEAN:UST', 'tPLANETS:USD', 'tPLANETS:UST', 'tFTMUSD', 'tFTMUST', 'tNEXO:USD', 'tNEXO:BTC', 'tNEXO:UST', 'tVELO:USD', 'tVELO:UST', 'tICPUSD', 'tICPBTC', 'tICPUST', 'tFCLUSD', 'tFCLUST', 'tTERRAUST:USD', 'tTERRAUST:UST', 'tMIRUSD', 'tMIRUST', 'tGRTUSD', 'tGRTUST', 'tWAVES:USD', 'tWAVES:UST', 'tREEF:USD', 'tREEF:UST', 'tBTCEUT', 'tCHSB:USD', 'tCHSB:BTC', 'tCHSB:UST', 'tXRDUSD', 'tXRDBTC', 'tEXOUSD', 'tROSE:USD', 'tROSE:UST', 'tDOGE:BTC', 'tETCUST', 'tNEOUST', 'tATOUST', 'tXTZUST', 'tBATUST', 'tVETUST', 'tTRXUST', 'tETHEUT', 'tEURUST', 'tMATIC:USD', 'tMATIC:BTC', 'tMATIC:UST', 'tAXSUSD', 'tAXSUST', 'tHMTUSD', 'tHMTUST', 'tDORA:USD', 'tDORA:UST', 'tBTC:XAUT', 'tETH:XAUT', 'tSOLBTC', 'tAVAX:BTC', 'tJASMY:USD', 'tJASMY:UST', 'tANCUSD', 'tANCUST', 'tAIXUSD', 'tAIXUST', 'tSHIB:USD', 'tSHIB:UST', 'tMIMUSD', 'tMIMUST', 'tQRDO:USD', 'tQRDO:UST', 'tBTCMIM', 'tMKRUST', 'tTLOS:USD', 'tTLOS:UST', 'tBOBA:USD', 'tBOBA:UST', 'tSPELL:USD', 'tSPELL:UST', 'tWNCG:USD', 'tWNCG:UST', 'tSPELL:MIM', 'tSRMUSD', 'tSRMUST', 'tCRVUSD', 'tCRVUST', 'tTHETA:USD', 'tTHETA:UST', 'tZMTUSD', 'tZMTUST', 'tWILD:USD', 'tWILD:UST', 'tDVFUSD', 'tPNGUSD', 'tPNGUST', 'tKAIUSD', 'tKAIUST', 'tWOOUSD', 'tWOOUST', 'tTRADE:USD', 'tTRADE:UST', 'tSGBUSD', 'tSGBUST', 'tSXXUSD', 'tSXXUST', 'tCCDUSD', 'tCCDUST', 'tCCDBTC', 'tGBPUST', 'tGBPEUT', 'tJPYUST', 'tBMNUSD', 'tBMNBTC', 'tSHFT:USD', 'tPOLC:USD', 'tHIXUSD', 'tSHFT:UST', 'tPOLC:UST', 'tHIXUST', 'tGALA:USD', 'tGALA:UST', 'tAPEUSD', 'tAPEUST', 'tSIDUS:USD', 'tSIDUS:UST', 'tSENATE:USD', 'tSENATE:UST', 'tB2MUSD', 'tB2MUST', 'tSTGUSD', 'tSTGUST', 'tLUXO:USD', 'tSAND:USD', 'tSAND:UST', 'tGMTUSD', 'tGMTUST', 'tGSTUSD', 'tGSTUST', 'tPOLIS:USD', 'tPOLIS:UST', 'tATLAS:USD', 'tATLAS:UST', 'tLUNA2:USD', 'tLUNA2:UST', 'tBTCF0:USTF0', 'tETHF0:USTF0', 'tXAUTF0:USTF0', 'tBTCDOMF0:USTF0', 'tTESTBTCF0:TESTUSDTF0', 'tAMPF0:USTF0', 'tEURF0:USTF0', 'tGBPF0:USTF0', 'tJPYF0:USTF0', 'tEUROPE50IXF0:USTF0', 'tGERMANY30IXF0:USTF0', 'tEOSF0:USTF0', 'tLTCF0:USTF0', 'tDOTF0:USTF0', 'tXAGF0:USTF0', 'tIOTF0:USTF0', 'tLINKF0:USTF0', 'tUNIF0:USTF0', 'tETHF0:BTCF0', 'tADAF0:USTF0', 'tXLMF0:USTF0', 'tDOTF0:BTCF0', 'tLTCF0:BTCF0', 'tXAUTF0:BTCF0', 'tDOGEF0:USTF0', 'tSOLF0:USTF0', 'tSUSHIF0:USTF0', 'tFILF0:USTF0', 'tAVAXF0:USTF0', 'tXRPF0:USTF0', 'tXMRF0:USTF0', 'tXRPF0:BTCF0', 'tALGF0:USTF0', 'tGERMANY40IXF0:USTF0', 'tAAVEF0:USTF0', 'tMATICF0:USTF0', 'tFTMF0:USTF0', 'tEGLDF0:USTF0', 'tAXSF0:USTF0', 'tCOMPF0:USTF0', 'tXTZF0:USTF0', 'tTRXF0:USTF0', 'tSOLF0:BTCF0', 'tAVAXF0:BTCF0', 'tATOF0:USTF0', 'tSHIBF0:USTF0', 'tOMGF0:USTF0', 'tBTCF0:EUTF0', 'tETHF0:EUTF0', 'tNEOF0:USTF0', 'tZECF0:USTF0', 'tCRVF0:USTF0', 'tNEARF0:USTF0', 'tICPF0:USTF0', 'tGALAF0:USTF0', 'tAPEF0:USTF0', 'tETCF0:USTF0', 'tANCF0:USTF0', 'tWAVESF0:USTF0', 'tJASMYF0:USTF0', 'tKNCF0:USTF0', 'tSTGF0:USTF0']
headers = [
    "BID",
    "BID_SIZE",
    "ASK",
    "ASK_SIZE",
    "DAILY_CHANGE",
    "DAILY_CHANGE_RELATIVE",
    "LAST_PRICE",
    "VOLUME",
    "HIGH",
    "LOW"
]


def write_logs(log):
    logging.info(log)


def split(data):
    return list(data.split(','))


def push(res, db, headers, markets):
    try:
        res = {headers[i]: res[i] for i in range(len(headers))}
        res['time'] = datetime.datetime.now()
        df = pd.DataFrame([res])
        df.to_sql(markets,
                  con=db.connection,
                  if_exists='append')
    except IndexError:
        pass


def on_message(ws, message):
    data = (message[9:-2])
    res = split(data)
    push(res, db, headers, markets)


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
        ws.send(json.dumps({"event": "subscribe",
                    "channel": channel,
                    "symbol": market}))



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
