import binance.exceptions
from binance.client import Client
from binance.helpers import round_step_size
import time

keys = {
    "akey": 'X11id706QcDepdRirJDF8AbBuYgMIlET4b9ThkZ6dycVcA2Jdogi91cGFhluPbTY',
    "skey": 'kP3Uw1sHJDItAYpwXFoirSsd6rhnQRdzeNOhlLx0EwAQ48zBjwsXx2LrodyY2xKP'}
client = Client(keys["akey"], keys["skey"])


# place first order

def place_order(middle_pair, qty):
    # Initialize DB
    # get token equivalent of usdt
    balance = client.get_asset_balance(asset='USDT')
    start_bal = balance['free']
    print(f'Starting USDT Balance: {start_bal}')
    info = client.get_symbol_info(middle_pair)
    base_asset = str(info['baseAsset'])
    quote_asset = str(info['quoteAsset'])
    pair_1 = base_asset + 'USDT'
    pair_3 = quote_asset + 'USDT'
    avg_price = client.get_avg_price(symbol=pair_1)
    quant = qty / float(avg_price['price'])
    info = client.get_symbol_info(pair_1)
    step = info['filters'][2]['stepSize']
    quantity = round_step_size(quant, step)
    print(f'1. buying {quantity}{base_asset} for {qty} USDT')
    order1 = client.order_market_buy(
        symbol=pair_1,
        quantity=quantity)
    time.sleep(2)
    order_id = order1['orderId']
    # wait until order filled
    print('waiting for 1st order fulfilment')
    while order1['status'] != 'FILLED':
        order1 = client.get_order(
            symbol=pair_1,
            orderId=order_id)
    print("Order #1 Filled")
    print(order1)
    # separate asset and find step size for next pair
    balance = client.get_asset_balance(asset=base_asset)
    left_bal = balance['free']
    info = client.get_symbol_info(middle_pair)
    step = info['filters'][2]['stepSize']
    rounded_amount = round_step_size(left_bal, step)
    print(f'2. selling {rounded_amount}{base_asset} for {quote_asset}')
    # execute order 2
    order2 = client.order_market_sell(
        symbol=middle_pair,
        quantity=rounded_amount)
    time.sleep(2)
    order_id = order2['orderId']
    print('waiting for 2nd order fulfillment')
    while order2['status'] != 'FILLED':
        order2 = client.get_order(
            symbol=middle_pair,
            orderId=order_id)
        time.sleep(1)
    print("Order #2 Filled")
    print(order2)
    # separate asset and find step size for next pair
    balance = client.get_asset_balance(asset=quote_asset)
    left_bal = balance['free']
    info = client.get_symbol_info(pair_3)
    step = info['filters'][2]['stepSize']
    rounded_amount = round_step_size(left_bal, step)
    print(f'3. Selling {quote_asset} for USDT')
    # execute order 3
    order3 = client.order_market_sell(
        symbol=pair_3,
        quantity=rounded_amount)
    time.sleep(2)
    order_id = order3['orderId']
    print('Waiting for 3rd order fulfillment')
    while order3['status'] != 'FILLED':
        order3 = client.get_order(
            symbol=pair_3,
            orderId=order_id)
        time.sleep(1)
    print("Order #3 Filled")
    print(order3)
    trades = client.get_my_trades(symbol=pair_3)
    asset = trades[0]['commissionAsset']
    balance = client.get_asset_balance(asset=asset)
    final_bal = balance['free']
    print(f'Closing USDT Balance: {final_bal}')
    difference = float(final_bal) - float(start_bal)
    if final_bal > start_bal:
        print(f'Profit: {difference}')
    else:
        print(f'Loss: {difference}')


def unfinished(pair):
    info = client.get_symbol_info(pair)
    base_asset = str(info['baseAsset'])
    quote_asset = str(info['quoteAsset'])
    balance = client.get_asset_balance(asset=base_asset)
    left_bal = balance['free']
    info = client.get_symbol_info(pair)
    step = info['filters'][2]['stepSize']
    rounded_amount = round_step_size(left_bal, step)
    print(f'Selling {rounded_amount}{base_asset} for {quote_asset}')
    try:
        order = client.order_market_sell(
                    symbol=pair,
                    quantity=rounded_amount)
    except binance.exceptions.BinanceAPIException:
        print(f'Insufficient {base_asset}')
        return
    time.sleep(2)
    order_id = order['orderId']
    print('Waiting for order fulfillment')
    while order['status'] != 'FILLED':
        order= client.get_order(
            symbol=pair,
            orderId=order_id)
        time.sleep(1)
    print("Order #3 Fulfilled")
    print(order)


place_order('BELBTC', 30)
# unfinished('BTCUSDT')
