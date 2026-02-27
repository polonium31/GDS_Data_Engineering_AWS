from dotenv import load_dotenv
import os
import random as rand
import datetime
import time
import json
from confluent_kafka import Producer

load_dotenv()

def random_data_generator():
    ad_id = ["12332","24123","12232"]
    ts = datetime.datetime.now().timestamp()
    data = {
        "ad_id": rand.choice(ad_id),
        "timestamp": datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        "clicks": rand.randrange(1,100),
        "views": rand.randrange(1,8000),
        "cost": round(rand.uniform(1.0,8000.0),2)
        }
    return data

conf = {
        'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': os.getenv('KAFKA_SASL_USERNAME'),
        'sasl.password': os.getenv('KAFKA_SASL_PASSWORD')
        }

producer = Producer(conf)

try:
    while True:
        data = random_data_generator()
        msg_key = data['timestamp'].encode('utf-8')
        binary_data = json.dumps(data).encode('utf-8')
        producer.produce('ads_data', 
                         key=msg_key, 
                         value=binary_data)
        producer.poll(0) 
        
        print(f"Produced: {data['ad_id']} at {data['timestamp']}")
        time.sleep(2)
except KeyboardInterrupt:
    print("Shutdown signal received.")
finally:
    print("Flushing remaining messages...")
    producer.flush()