import datetime
import json
import websocket

from dotenv import dotenv_values
from kafka import KafkaProducer
from pymongo import MongoClient

config = dotenv_values(".env")
socket = config['BINANCE_SOCKET_ADDR']

price_list_dict = {}
producer = KafkaProducer(acks=0,
                         compression_type='gzip',
                         bootstrap_servers=[f"{config['KAFKA_HOST']}:{config['KAFKA_PORT']}"],
                         )
mongo_client = MongoClient(f"{config['MONGO_DB_HOST']}:{config['MONGO_DB_PORT']}/",
                           username=config['MONGO_DB_USER'],
                           password=config['MONGO_DB_PASSWORD'])


def on_open(web_socket):
    print("opened")
    subscribe_message = {
        "method": "SUBSCRIBE",
        "params": [f'{market}@trade' for market in config['SUBSCRIPTION_LIST'].split(',')],
        "id": 1
    }

    web_socket.send(json.dumps(subscribe_message))


def update_queue(queue, size_in_hours):
    if len(queue) > size_in_hours:
        queue.pop(0)


# def on_message(web_socket, message):
#     result = json.loads(message)
#     print(result)
#     print(str(datetime.fromtimestamp(result['k']['t'] / 1000)))
#     print(str(datetime.fromtimestamp(result['k']['T'] / 1000)))
#     update_price_list_dict(result)
#
#
#     # print(str(datetime.utcfromtimestamp(result['E']/1000)))
#     result = result['k']
#     # if 'result' not in result:
#     high_price = float(result['h'])
#     low_price = float(result['l'])
#     current_price = float(result['c'])
#     prev_price = float(result['o'])
#     #cur_time = str(datetime.utcfromtimestamp(result['E']/1000))
#     # print(cur_time)
#
#     val = buy_volatility_breakout(high_price, low_price, prev_price, current_price, x=0.5)
#
#     print("High: ", high_price)
#     print("Low: ", low_price)
#     print("Current: ", current_price)
#     print("Prev: ", prev_price)
#
#     print("Result: ", val)

highs = {}
lows = {}


def alert_highs_lows(web_socket, message):
    result = json.loads(message)

    highs[result['k']['i']] = result['k']['h']
    lows[result['k']['i']] = result['k']['l']
    print(highs)
    print(lows)


def on_close(web_socket):
    print("closed connection")


def add_price_list(coin_currency):
    price_list_dict[coin_currency] = {'time': [],
                                      'price': []}


def update_price_list_dict(received_data, cycle_hours=12, interval_second=2):
    coin_currency = received_data['s']
    if coin_currency not in price_list_dict:
        add_price_list(received_data['s'])
        price_list_dict[coin_currency]['time'].append(received_data['E'])
        price_list_dict[coin_currency]['price'].append(received_data['p'])
        return True

    if received_data['E'] / 1000 - price_list_dict[coin_currency]['time'][-1] / 1000 < interval_second:
        return False

    price_list_dict[coin_currency]['time'].append(received_data['E'])
    price_list_dict[coin_currency]['price'].append(received_data['p'])

    while price_list_dict[coin_currency]['time'][-1] / 1000 - price_list_dict[coin_currency]['time'][0] / 1000 \
            > cycle_hours * 60 * 60:
        del price_list_dict[coin_currency]['time'][0]
        del price_list_dict[coin_currency]['price'][0]
    return True


def update_prices(ws, data):
    data = json.loads(data)
    if not update_price_list_dict(data):
        return
    if 'E' not in data:
        return None

    parsed_data = {
        "market": data['s'],
        "price": float(data['p']),
        "timestamp": data['E'],
        "datetime": str(datetime.datetime.now())
    }
    print(parsed_data)

    producer.send(config['EXCHANGE'], value=json.dumps(parsed_data).encode('utf-8'))
    producer.flush()
    exec(f"mongo_client.{config['EXCHANGE']}.{parsed_data['market']}.insert_one({parsed_data})")


ws = websocket.WebSocketApp(socket,
                            on_open=on_open,
                            on_message=update_prices,
                            on_close=on_close)
ws.run_forever(ping_interval=1000)

