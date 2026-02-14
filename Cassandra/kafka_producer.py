# pip3 install confluent_kafka dotenv certifi httpx authlib cachetools attrs fastavro
import pandas as pd
import os
import math
import time
from dotenv import load_dotenv
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

load_dotenv()

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
        return
    print(f"Record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

# Kafka and Schema Registry configuration
kafka_config = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': os.getenv('KAFKA_SASL_USERNAME'),
    'sasl.password': os.getenv('KAFKA_SASL_PASSWORD')
}

schema_registry_client = SchemaRegistryClient({
    'url': os.getenv('SCHEMA_REGISTRY_URL'),
    'basic.auth.user.info': os.getenv('SCHEMA_REGISTRY_AUTH')
})

def get_latest_schema(subject):
    schema = schema_registry_client.get_latest_version(subject).schema.schema_str
    return AvroSerializer(schema_registry_client, schema)

# Producer Initialization
ecommerce_orders_producer = SerializingProducer({
    **kafka_config,
    'key.serializer': StringSerializer('utf_8'),
    'value.serializer': get_latest_schema('ecommerce-orders-value')
})

# Load and Preprocess Data
df = pd.read_csv("olist_orders_dataset.csv",quotechar='"')
df = df.fillna("UNKNOWN")

# print("Starting production...")

for index, row in df.iterrows():
    try:
        k = f"{row['order_id']}-{row['customer_id']}"
        v = row.to_dict()

        ecommerce_orders_producer.produce(
            topic="ecommerce-orders",
            key=k,
            value=v,
            on_delivery=delivery_report
        )
    except Exception as e:
        print(f"Error on row {index}: {e}")

    ecommerce_orders_producer.poll(0)
    time.sleep(5)

# Final flush
ecommerce_orders_producer.flush()
print('All messages processed.')