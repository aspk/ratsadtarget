from boto.s3.connection import S3Connection
import datetime
import json
import bz2
from kafka import KafkaProducer
from kafka.errors import KafkaError
import time
import pytz
import random

conn = S3Connection()
key = conn.get_bucket('aspk-reddit-posts').get_key('comments/RC_2017-10.bz2')

producer = KafkaProducer(bootstrap_servers=['10.0.0.9:9092'])
count = 0
decomp = bz2.BZ2Decompressor()


CHUNK_SIZE = 5000*1024
timezone = pytz.timezone("America/Los_Angeles")
start_time = time.time()
while True:
    print('in')
    chunk = key.read(CHUNK_SIZE)
    if not chunk:
        break
    data = decomp.decompress(chunk).decode()
    history = []
    for i in data.split('\n'):

        try:
            count += 1
            if count % 10000 == 0 and count != 0:
                print('rate of kafka producer messages is {}'.format(count/(time.time()-start_time)))
            comment = json.loads(i)
            reddit_event = {}
            reddit_event['post'] = comment['permalink'].split('/')[-3]
            reddit_event['subreddit'] = comment['subreddit']
            reddit_event['timestamp'] = str(datetime.datetime.fromtimestamp(time.time()))

            reddit_event['body'] = comment['body']
            reddit_event['author'] = comment['author']
            reddit_event['views'] = int(100*random.random())

            event = json.dumps(reddit_event)
            history.append(event)
            if len(history) > 40:
                history.pop(0)
            producer.send('reddit-stream-topic', bytes(event, 'utf-8'))
            producer.flush()
            producer.send('reddit-stream-topic', bytes(history[0], 'utf-8'))
            producer.flush()

        except:
            print('Incomplete string ... skipping this comment')
