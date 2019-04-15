# kafka consumer
# db write and delete consumer
from kafka import KafkaConsumer
from postgres_functions import postgres_insert,postgres_delete
import json
import time
import datetime
import psycopg2
print('consumer')
consumer = KafkaConsumer('reddit-spark-topic',
                         group_id='cons_postgre_1',
                         bootstrap_servers=['10.0.0.5:9092'],
                         auto_offset_reset = 'latest')
conn_string = "host='host' port='port' dbname='dbname' user='user' password='password'"
conn = psycopg2.connect(conn_string)
conn.autocommit = False
cursor = conn.cursor()

flag = 0
start_time = time.time()
count = 0
postgres_count = 0
for message in consumer:
    count+=1
    if count%5000==0 and count!=0:
        print('rate of kafka consumer messages is {}'.format(count/(time.time()-start_time)))
    msg = json.loads(message.value.decode())
    if flag ==0:
        postgres_count+=1
        postgres_insert(msg,postgres_count,conn,cursor)
        flag =1
        s = msg['window']['end'].replace('T',' ').replace('Z','')[:-4]
        timestamp = time.mktime(datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S").timetuple())
    else:
        postgres_count+=1
        s = msg['window']['end'].replace('T',' ').replace('Z','')[:-4]
        new_timestamp = time.mktime(datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S").timetuple())
        if new_timestamp>timestamp-30:
            postgres_delete(msg,conn,cursor)
            postgres_insert(msg,postgres_count,conn,cursor)
            timestamp = new_timestamp
        else:
            postgres_insert(msg,postgres_count,conn,cursor)
