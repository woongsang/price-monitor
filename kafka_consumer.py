import sys
import asyncio
import time
import requests

from kafka import KafkaConsumer
import json

from pymongo import MongoClient
from dotenv import dotenv_values

from buy_strategies import volatility_breakout


def background(f):
    def wrapped(*args, **kwargs):
        return asyncio.get_event_loop().run_in_executor(None, f, *args, **kwargs)

    return wrapped


def retrieve_mongo_data(mongo_client, time_start, market):
    results = mongo_client['binance'][market].find({'timestamp': {'$gte': time_start}})
    timestamp_list = []
    price_list = []
    for result in results:
        timestamp_list.append(result['timestamp'])
        price_list.append(result['price'])

    return timestamp_list, price_list


def larry_signal(tracker_info, current_data, buy_config, url):
    val = volatility_breakout(tracker_info['high'],
                              tracker_info['low'],
                              tracker_info['last_price'],
                              current_data['price'],
                              buy_config['x'])
    if val != 0:
        meta_data = {
            'buy_config': buy_config,
            'tracker_info': tracker_info,
            'purchase_price': current_data['price'],
            'signal': 'long' if val == 1 else 'short'
        }
        res = requests.post(url, data=json.dumps(meta_data))
        if res.status_code == 200:
            tracker_info['sent_signal'] = True
        print(meta_data)
    return val


def update_tracker_info(tracker_info, timestamp, mongo_client, market, cycle_hours, diff_tolerance=5):
    cycle_start_time = timestamp - cycle_hours * 60 * 60 * 1000
    mongo_timestamps, mongo_prices = retrieve_mongo_data(mongo_client, cycle_start_time, market)

    time_diff = abs(mongo_timestamps[0] - cycle_start_time)
    if time_diff > diff_tolerance * 1000:
        print(f'Sleep for {time_diff / 1000} seconds.')
        time.sleep(time_diff / 1000)
        return False

    if tracker_info['cycle_end_time'] is None or \
            (timestamp - tracker_info['cycle_end_time']) / (60 * 60 * 1000) > cycle_hours:
        tracker_info['high'] = max(mongo_prices)
        tracker_info['low'] = min(mongo_prices)
        tracker_info['last_price'] = mongo_prices[-1]
        tracker_info['update_time'] = timestamp
        tracker_info['cycle_end_time'] = mongo_timestamps[-1]
        tracker_info['sent_signal'] = False

    return True


@background
def start_buy_signal(market, cycle_hours):
    market = market.upper()

    config = dotenv_values(".env")

    with open("larry_config.json") as json_file:
        larry_configs = json.load(json_file)

    mongo_client = MongoClient(f"{config['MONGO_DB_HOST']}:{config['MONGO_DB_PORT']}/",
                               username=config['MONGO_DB_USER'],
                               password=config['MONGO_DB_PASSWORD'])

    consumer = KafkaConsumer(config['EXCHANGE'],
                             bootstrap_servers=[f"{config['KAFKA_HOST']}:{config['KAFKA_PORT']}"],
                             auto_offset_reset='latest',
                             enable_auto_commit=True,
                             )

    tracker_info_list = [
                            {'high': None,
                             'low': None,
                             'last_price': None,
                             'update_time': None,
                             'cycle_end_time': None,
                             'sent_signal': False,

                             }
                        ] * len(larry_configs)

    for message in consumer:
        current_data = json.loads(message.value)

        for i, buy_config in enumerate(larry_configs):
            if buy_config['cycle_hours'] != cycle_hours:
                continue
            update_time = tracker_info_list[i]['update_time']
            if update_time is None or current_data['timestamp'] - update_time > cycle_hours:
                success = update_tracker_info(tracker_info_list[i],
                                              current_data['timestamp'],
                                              mongo_client,
                                              market,
                                              cycle_hours)
                if not success:
                    continue
            if not tracker_info_list[i]['sent_signal']:
                signal = larry_signal(tracker_info_list[i], current_data, buy_config,
                                      url=config['larry_signal_url'])
                print(f'{market}: {signal}')


def main(markets, cycle_hours):
    for market in markets:
        start_buy_signal(market, int(cycle_hours))


if __name__ == '__main__':
    config = dotenv_values(".env")
    main(config['SUBSCRIPTION_LIST'].split(','), sys.argv[1])
