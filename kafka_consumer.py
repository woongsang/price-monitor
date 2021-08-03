from kafka import KafkaConsumer
import json

TOPIC_NAME = 'BTCUSDT'

consumer = KafkaConsumer(TOPIC_NAME,
                         bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='latest',
                         enable_auto_commit=True,
                         )

for message in consumer:
    print(json.loads(message.value))

