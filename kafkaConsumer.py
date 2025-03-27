from kafka import KafkaConsumer
import json
import psycopg2
print('connecting pg ...')
try:
    conn = psycopg2.connect(database = "defaultdb", 
                        user = "avnadmin", 
                        host= 'pg-adsoftsito-adsoft.l.aivencloud.com',
                        password = "XXXXXXX",
                        port = 13078)
    cur = conn.cursor()
    print("PosgreSql Connected successfully!")
except:
    print("Could not connect to PosgreSql")
