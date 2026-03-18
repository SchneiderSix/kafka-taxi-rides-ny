import json
from time import time

import pandas as pd
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"

columns = [
    'lpep_pickup_datetime', 'lpep_dropoff_datetime',
    'PULocationID', 'DOLocationID',
    'passenger_count', 'trip_distance',
    'tip_amount', 'total_amount'
]

df = pd.read_parquet(url, columns=columns)

df['lpep_pickup_datetime'] = df['lpep_pickup_datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
df['lpep_dropoff_datetime'] = df['lpep_dropoff_datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')

t0 = time()

for _, row in df.iterrows():
    message = row.to_dict()
    producer.send('green-trips', value=message)

producer.flush()

t1 = time()
print(f'took {(t1 - t0):.2f} seconds')
print(f'sent {len(df)} messages')
