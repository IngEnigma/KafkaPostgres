from kafka import KafkaProducer
import json

producer = KafkaProducer(
   bootstrap_servers='localhost:9092',
   value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def on_send_success(record_metadata):
   print(record_metadata.topic)
   print(record_metadata.partition)
   print(record_metadata.offset)

def on_send_error(excp):
   print('I am an errback', exc_info=excp)


url='https://raw.githubusercontent.com/adsoftsito/bigdata/refs/heads/main/results/data.json'
import pandas as pd

df = pd.read_json(url, orient='columns')

for index, value in df.head(3).iterrows():
   dict_data = dict(value)
   producer.send('people', value=dict_data[0])
   print(dict_data[0])

producer.close()
