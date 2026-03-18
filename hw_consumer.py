import json

from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'green-trips',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    consumer_timeout_ms=10000,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

count = 0
total = 0

for message in consumer:
    trip = message.value
    total += 1
    if trip['trip_distance'] > 5.0:
        count += 1

print(f'Total messages: {total}')
print(f'Trips with distance > 5: {count}')

consumer.close()
