from sockets import bitfinex
from sockets import binance
from sockets import bybit
from sockets import coinbase
from sockets import coinflex
from sockets import cryptocom
from sockets import ftx
from sockets import gateio
from sockets import gemini
from sockets import huobi
from sockets import poloniex


def selection():
    print("List of Exchanges")
    print("1. Binance")
    print("2. Bitfinex")
    print("3. Bybit")
    print("4. Coinbase")
    print("5. Coinflex")
    print("6. Crypto.com")
    print("7. FTX")
    print("8. Gate.IO")
    print("9. Gemini")
    print("10. Huobi")
    print("11. Poloniex")
    choice = int(input("Exchange No: "))

    if choice == 1:
        print("Connecting to Binance")
        binance.start()
    elif choice == 2:
        print("Connecting to Bitfinex")
        bitfinex.start()
    elif choice == 3:
        print("Connecting to Bybit")
        bybit.start()
    elif choice == 4:
        print("Connecting to Coinbase")
        coinbase.start()
    elif choice == 5:
        print("Connecting to Coinflex")
        coinflex.start()
    elif choice == 6:
        print("Connecting to Crypto.com")
        cryptocom.start()
    elif choice == 7:
        print("Connecting to FTX")
        ftx.start()
    elif choice == 8:
        print("Connecting to Gate.IO")
        gateio.start()
    elif choice == 9:
        print("Connecting to Gemini")
        gemini.start()
    elif choice == 10:
        print("Connecting to Huobi")
        huobi.start()
    elif choice == 11:
        print("Connecting to Poloniex")
        poloniex.start()
    else:
        print("Invalid Choice")


selection()