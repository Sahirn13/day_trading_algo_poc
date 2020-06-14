import os
from math import modf
import json
import alpaca_trade_api as tradeapi
from datetime import datetime, timedelta
from polygon import WebSocketClient, STOCKS_CLUSTER, RESTClient
from iexfinance.stocks import Stock
import threading, queue
import ast
import concurrent.futures
import time

thread_local = threading.local()
YESTERDAY_DATA = {}
MINUTE_TICKS = {}
SECOND_TICKS = {}
tickers_to_subscribe_per_second = []
tickers_to_subscribe_per_minute = []
buy_complete = False
analyzing = False
socket_client = WebSocketClient(STOCKS_CLUSTER, os.environ["APCA_API_KEY_ID_PT"])
buy_price = None
exit_price = None

alpaca_client = tradeapi.REST(
    os.environ["APCA_API_KEY_ID_PT"],
    os.environ["APCA_API_KEY_SECRET_PT"],
    os.environ["APCA_API_BASE_URL_PT"],
)


def filter_stonks_by_price(q, result, polygon_client):
    while not q.empty():
        work = q.get()
        try:
            close_price = polygon_client.stocks_equities_previous_close(work[1])
            close_price = close_price.results[0]
            if (close_price['c'] < 10) & (close_price['c'] > 2):
                share_float = Stock(close_price['T']).get_float()
                if share_float < 20000000:
                    close_price['share_float'] = share_float
                    result[work[0]] = close_price
                else:
                    result.remove({})
            else:
                result.remove({})
        except:
            result.remove({})
        q.task_done()
    return True


def look_for_exit(message):
    res = ast.literal_eval(message)[0]
    symbol = res['sym']
    bid_price = res['bp']
    global exit_price
    if bid_price < exit_price:
        alpaca_client.submit_order(symbol, 10, 'buy', 'limit', 'ioc', bid_price)
    else:
        exit_price = bid_price


def execute_trade(symbol, current_ask_price, current_bid_price):
    try:
        order = alpaca_client.submit_order(symbol, 10, 'buy', 'limit', 'ioc', current_ask_price)
    except Exception:
        print('failed')
    order_filled = False

    while not order_filled:
        status = alpaca_client.get_order(order['id'])
        if status['status'] == 'filled' or status['status'] == 'partially_filled':
            order_filled = True

    global exit_price
    exit_price = current_bid_price
    socket_client.process_message = look_for_exit


def process_am(res):
    MINUTE_TICKS[res['sym']].append(res)


def process_a(res):
    SECOND_TICKS[res['sym']].append(res)


def calculate_gap(res):
    symbol = res['sym']
    diff = res['op'] - YESTERDAY_DATA[symbol]['c']
    gap_percentage = 100 * (diff / float(YESTERDAY_DATA[symbol]['c']))
    print('calculate gap')
    if gap_percentage < 30:
        print('gap less than 30')
        socket_client.unsubscribe('A.' + symbol)
        socket_client.unsubscribe('AM.' + symbol)
        MINUTE_TICKS.pop(symbol, None)
    return gap_percentage


def stream_single_stock(message):
    res = ast.literal_eval(message)[0]
    if res['ev'] != 'status':
        current_ask_price = res['ap']
        current_bid_price = res['bp']
        tup = modf(current_ask_price)
        distance_to_resistance_lvl = 100 * ((25 - (int(100 * tup[0]) % 25)) / 25.0)
        if distance_to_resistance_lvl > 10:
            execute_trade(res['sym'], current_ask_price, current_bid_price)


def analyze_buy_opp(res):
    volume = res['v']
    m_1 = MINUTE_TICKS[res['sym']][-2]
    m_2 = MINUTE_TICKS[res['sym']][-1]
    global analyzing
    if (volume > 250000) & (m_2 - m_1) > .2 & (not analyzing):
        socket_client.close_connection()
        socket_client.run_async()
        socket_client.process_message = stream_single_stock
        socket_client.subscribe('Q' + res['sym'])
        analyzing = True
        while analyzing:
            time.sleep(.1)


def my_customer_process_message(message):
    res = ast.literal_eval(message)[0]
    if res['ev'] != 'status':
        gap = calculate_gap(res)
        print(gap)

        symbol = res['sym']
        if res['ev'] == 'AM':
            process_am(res)

        if res['ev'] == 'A':
            process_a(res)

        # we want at least 2 minutes of data to analyze before making the trade
        if (calculate_gap(res) > 30) & (len(MINUTE_TICKS[symbol]) > 1) & (len(SECOND_TICKS[symbol]) > 0):
            analyze_buy_opp(res)


def main():
    polygon_client = RESTClient(os.environ["APCA_API_KEY_ID_PT"])
    socket_client.process_message = my_customer_process_message
    asset_tickers = alpaca_client.list_assets(status='active', asset_class='us_equity')
    ticker_list = []

    count = 0
    for ticker in asset_tickers:
        if count == 8000:
            break
        ticker_list.append(ticker.symbol)
        count += 1
        print(count)

    q = queue.Queue()
    num_threads = 50
    results = [{} for x in ticker_list]
    for i in range(len(ticker_list)):
        q.put((i, ticker_list[i]))

    for i in range(num_threads):
        worker = threading.Thread(target=filter_stonks_by_price, args=(q, results, polygon_client))
        worker.setDaemon(True)
        worker.start()

    q.join()

    for result in results:
        if not result:
            continue
        tickers_to_subscribe_per_second.append('A.' + result['T'])
        tickers_to_subscribe_per_minute.append('AM.' + result['T'])
        YESTERDAY_DATA[result['T']] = result
        MINUTE_TICKS[result['T']] = []
        SECOND_TICKS[result['T']] = []

    socket_client.run_async()
    socket_client.subscribe(*tickers_to_subscribe_per_minute)
    socket_client.subscribe(*tickers_to_subscribe_per_second)


if __name__ == '__main__':
    main()
