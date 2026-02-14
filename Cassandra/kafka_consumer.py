import pandas as pd
import os
import uuid
from dotenv import load_dotenv
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra import ConsistencyLevel
load_dotenv()

# --- 1. CONFIGURATION ---

kafka_config = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': os.getenv('KAFKA_SASL_USERNAME'),
    'sasl.password': os.getenv('KAFKA_SASL_PASSWORD'),
    'group.id': 'ecommerce_consumer_group',
    'auto.offset.reset': 'earliest'
}

schema_registry_client = SchemaRegistryClient({
    'url': os.getenv('SCHEMA_REGISTRY_URL'),
    'basic.auth.user.info': os.getenv('SCHEMA_REGISTRY_AUTH')
})

# --- 2. CASSANDRA CONNECTION ---

try:
    cloud_config = {'secure_connect_bundle': 'secure-connect-cassandra.zip'}
    auth_provider = PlainTextAuthProvider(os.getenv('CASSANDRA_CLIENT_ID'), os.getenv('CASSANDRA_SECRET'))
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()
    
    # Ensure we are using the correct keyspace
    session.set_keyspace('ecommerce') 
    print("Connected to Cassandra ✅")
except Exception as e:
    print(f"Unable to connect to Cassandra ❌: {e}")
    exit()

# --- 3. PREPARED STATEMENT ---
# Preparing the statement once improves performance and handles data types safely
insert_stmt = session.prepare("""
    INSERT INTO orders (
        order_id, 
        customer_id, 
        order_status, 
        order_purchase_timestamp, 
        order_approved_at, 
        order_delivered_carrier_date, 
        order_delivered_customer_date, 
        order_estimated_delivery_date,
        OrderHour, 
        OrderDayOfWeek
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

insert_stmt.consistency_level = ConsistencyLevel.QUORUM

# --- 4. TRANSFORMATION LOGIC ---

def transform_consumed_ecommerce_orders(record):
    if not record:
        return None

    # Clean "UNKNOWN" and handle UUID conversion
    for key, value in record.items():
        if value == "UNKNOWN":
            record[key] = None
    
    # Cast IDs to Python UUID objects for Cassandra driver compatibility
    record['order_id'] = uuid.UUID(record['order_id'])
    record['customer_id'] = uuid.UUID(record['customer_id'])

    # Feature Engineering with Pandas
    purchase_ts = pd.to_datetime(record['order_purchase_timestamp'])
    record['OrderHour'] = int(purchase_ts.hour)
    record['OrderDayOfWeek'] = str(purchase_ts.day_name())

    # Ensure all timestamp fields are Python datetime objects (not strings)
    time_cols = [
        'order_purchase_timestamp', 'order_approved_at', 
        'order_delivered_carrier_date', 'order_delivered_customer_date', 
        'order_estimated_delivery_date'
    ]
    
    for col in time_cols:
        if record.get(col):
            record[col] = pd.to_datetime(record[col]).to_pydatetime()

    return record

# --- 5. KAFKA CONSUMER SETUP ---

def get_latest_schema(subject):
    schema = schema_registry_client.get_latest_version(subject).schema.schema_str
    return AvroDeserializer(schema_registry_client, schema)

ecommerce_orders_consumer = DeserializingConsumer({
    **kafka_config,
    'key.deserializer': StringDeserializer('utf_8'),
    'value.deserializer': get_latest_schema('ecommerce-orders-value')
})

ecommerce_orders_consumer.subscribe(['ecommerce-orders'])

# --- 6. MAIN EXECUTION LOOP ---

try:
    print("Awaiting messages...")
    while True:
        msg = ecommerce_orders_consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print(f'Consumer error: {msg.error()}')
            continue

        record = msg.value()
        if record:
            processed_data = transform_consumed_ecommerce_orders(record)
            
            try:
                # Execute using the prepared statement and a tuple of values
                session.execute(insert_stmt, (
                    processed_data["order_id"],
                    processed_data["customer_id"],
                    processed_data["order_status"],
                    processed_data["order_purchase_timestamp"],
                    processed_data["order_approved_at"],
                    processed_data["order_delivered_carrier_date"],
                    processed_data["order_delivered_customer_date"],
                    processed_data["order_estimated_delivery_date"],
                    processed_data["OrderHour"],
                    processed_data["OrderDayOfWeek"]
                ))
                print(f"Order {processed_data['order_id']} synced successfully.")
            except Exception as err:
                print(f"Database Insertion Error: {err}")

except KeyboardInterrupt:
    print("Stopping Consumer...")
finally:
    cluster.shutdown()
    ecommerce_orders_consumer.close()