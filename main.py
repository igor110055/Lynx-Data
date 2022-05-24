# import os
from sockets.cryptocom import Crypto
# from sockets.bitfinex import Bitfinex
# from sockets.binance import Binance
# from sockets.coinbase import Coinbase
# from sockets.ftx import Ftx
# from sockets.huobi import Huobi
# from sockets.poloniex import Poloniex
#
#
# def delete():
#     path = r"logs/"
#     for file_name in os.listdir(path):
#         file = path + file_name
#         if os.path.isfile(file):
#             print('Deleting file:', file)
#             os.remove(file)
#
#
# def selection():
#     print("List of Exchanges")
#     print("1. Binance")
#     print("2. Bitfinex")
#     print("3. Coinbase")
#     print("4. FTX")
#     print("5. Huobi")
#     print("6. Poloniex")
#     choice = int(input("Exchange No: "))
#
#     if choice == 1:
#         print("Connecting to Binance")
#         binance = Binance()
#         binance.start()
#     elif choice == 2:
#         print("Connecting to Bitfinex")
#         bitfinex = Bitfinex()
#         bitfinex.start()
#     elif choice == 3:
#         print("Connecting to Coinbase")
#         coinbase = Coinbase()
#         coinbase.start()
#     elif choice == 4:
#         print("Connecting to FTX")
#         ftx = Ftx()
#         ftx.start()
#     elif choice == 5:
#         print("Connecting to Huobi")
#         huobi = Huobi()
#         huobi.start()
#     elif choice == 6:
#         print("Connecting to Poloniex")
#         poloniex = Poloniex()
#         poloniex.start()
#     else:
#         print("Invalid Choice")
#
#
# delete()
# selection()
# # bitfinex = Bitfinex()
# # binance = Binance()
# # coinbase = Coinbase()
# # ftx = Ftx()
# # huobi = Huobi()
# # poloniex = Poloniex()


crypto = Crypto()
crypto.start()