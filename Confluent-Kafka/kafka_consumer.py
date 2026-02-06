import threading
import json
import os
from dotenv import load_dotenv
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

load_dotenv()

# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': os.getenv('KAFKA_SASL_USERNAME'),
    'sasl.password': os.getenv('KAFKA_SASL_PASSWORD'),
    'group.id': 'group1',
    'auto.offset.reset': 'latest'
}

# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
    'url': os.getenv('SCHEMA_REGISTRY_URL'),
    'basic.auth.user.info': os.getenv('SCHEMA_REGISTRY_AUTH')
})

# Fetch the latest Avro schema for the value
subject_name = 'product_updates-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Deserializer for the value
key_deserializer = StringDeserializer('utf_8')
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

# Data transformation and business logic
def transform_and_process_data(record):
    # Transform the category to uppercase
    if 'category' in record:
        record['category'] = record['category'].upper()

    # Apply a discount if the category is "ELECTRONICS"
    if record.get('category') == 'ELECTRONICS' and 'price' in record:
        record['price'] = round(record['price'] * 0.9, 2)  # Apply a 10% discount

    # Convert the record to a JSON string
    json_record = json.dumps(record)

    # Append the JSON string to a file
    with open('transformed_records.json', 'a') as file:
        file.write(json_record + '\n')

    print(f"Transformed Record: {record}")

# Define a function to create and run a consumer
def run_consumer():
    consumer = DeserializingConsumer({
        'bootstrap.servers': kafka_config['bootstrap.servers'],
        'security.protocol': kafka_config['security.protocol'],
        'sasl.mechanisms': kafka_config['sasl.mechanisms'],
        'sasl.username': kafka_config['sasl.username'],
        'sasl.password': kafka_config['sasl.password'],
        'key.deserializer': key_deserializer,
        'value.deserializer': avro_deserializer,
        'group.id': kafka_config['group.id'],
        'auto.offset.reset': kafka_config['auto.offset.reset']
    })

    consumer.subscribe(['product_updates'])

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print('Consumer error: {}'.format(msg.error()))
                continue

            record = msg.value()
            if record:
                transform_and_process_data(record)

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

# Create and start 5 consumer threads
threads = []
for i in range(5):
    thread = threading.Thread(target=run_consumer)
    threads.append(thread)
    thread.start()

# Wait for all threads to finish
for thread in threads:
    thread.join()
