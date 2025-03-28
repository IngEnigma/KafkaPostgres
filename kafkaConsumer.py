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

consumer = KafkaConsumer('people',bootstrap_servers=['localhost:9092'])

for msg in consumer:
    record = json.loads(msg.value).replace('"','').split(',')
    name = record[0].split(':')[1]
    birth = record[1].split(':')[1]
    try:
       sql = "INSERT INTO people(name, birth) VALUES('" + name + "', '" +  birth + "')"
       print(sql)
       cur.execute(sql)
       conn.commit()
  except:
       print("Could not insert into PostgreSql")
conn.close()

