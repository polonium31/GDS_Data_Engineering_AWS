from decimal import *
import time
import mysql.connector
import os
from dotenv import load_dotenv

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import pandas as pd

load_dotenv()

def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.

    """
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))
    print("=====================")

# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': os.getenv('KAFKA_SASL_USERNAME'),
    'sasl.password': os.getenv('KAFKA_SASL_PASSWORD')
}

# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
    'url': os.getenv('SCHEMA_REGISTRY_URL'),
    'basic.auth.user.info': os.getenv('SCHEMA_REGISTRY_AUTH')
})

# Fetch the latest Avro schema for the value
subject_name = 'product_updates-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str
print("------Schema from Registery------")
print(schema_str)
print("=====================")

# Database configuration
config = {
    "host": os.getenv('DB_HOST'),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
    "database": os.getenv('DB_NAME')
}

# Create Avro Serializer for the value
key_serializer = StringSerializer('utf_8')
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# Define the SerializingProducer
producer = SerializingProducer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.serializer': key_serializer, 
    'value.serializer': avro_serializer 
})

last_read_timestamp = None

while True:
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()

    # Fetch product data updated after the last read timestamp
    if last_read_timestamp:
        cursor.execute(
            "SELECT id, name, category, price, last_updated FROM product WHERE last_updated > %s ORDER BY last_updated ASC",
            (last_read_timestamp,)
        )
    else:
        cursor.execute(
            "SELECT id, name, category, price, last_updated FROM product ORDER BY last_updated ASC"
        )

    rows = cursor.fetchall()

    if rows:
        for row in rows:
            # Convert the row to a dictionary
            product_data = {
                "id": row[0],
                "name": row[1],
                "category": row[2],
                "price": float(row[3]), 
                "last_updated": row[4].isoformat() 
            }

            # Produce the message to Kafka with Key as id
            producer.produce(
                topic="product_updates",
                key=str(product_data["id"]),
                value=product_data,
                on_delivery=delivery_report
            )

            print(product_data)

        # Update the last read timestamp to the latest row's timestamp
        last_read_timestamp = rows[-1][4]

        producer.flush()
    else:
        print("No new product data found.")

    cursor.close()
    conn.close()

    time.sleep(1)
