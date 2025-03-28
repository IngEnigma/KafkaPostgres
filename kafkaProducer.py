from kafka import KafkaProducer
import pandas as pd
import json

producer = KafkaProducer(
   bootstrap_servers='localhost:9092',
   value_serializer=lambda v: json.dumps(v).encode('utf-8')  
)

def on_send_success(record_metadata):
   print(f'Topic: {record_metadata.topic}, Partition: {record_metadata.partition}, Offset: {record_metadata.offset}')

def on_send_error(excp):
   print('Error al enviar mensaje:', excp)

url = 'https://raw.githubusercontent.com/IngEnigma/StreamlitSpark/refs/heads/master/results/male_crimes/part-00000-8b3d0568-ab40-4980-a6e8-e7e006621725-c000.json'

df = pd.read_json(url, lines=True)

for _, row in df.iterrows():  
   data = row.to_dict()  
   producer.send('crime', value=data).add_callback(on_send_success).add_errback(on_send_error)
   print(f'Enviado: {data}') 

producer.close()
