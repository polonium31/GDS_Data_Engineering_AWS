import pandas as pd
import os
import math
from dotenv import load_dotenv
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
import pymongo

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
    'sasl.password': os.getenv('KAFKA_SASL_PASSWORD'),
    'group.id': 'group1',
    'auto.offset.reset': 'earliest'
}

schema_registry_client = SchemaRegistryClient({
    'url': os.getenv('SCHEMA_REGISTRY_URL'),
    'basic.auth.user.info': os.getenv('SCHEMA_REGISTRY_AUTH')
})
user = os.getenv('MONGODB_USER')
password = os.getenv('MONGODB_PASSWORD')
uri = f"mongodb+srv://{user}:{password}@mongodb-cluster.nfykfyf.mongodb.net/?appName=mongodb-cluster"
mongodbclient = pymongo.MongoClient(uri)

logisticsdb = mongodbclient["logistics"]
logisticscol = logisticsdb["logistics_truck_data"]

try:
    # Trigger a connection
    mongodbclient.admin.command('ping')
    print("✅ Connection successful!")
except Exception as e:
    print(f"❌ Connection failed: {e}")

# Define the fields exactly as they appear in your Avro Schema
SCHEMA_FIELDS = [
    "GpsProvider", "BookingID", "MarketType", "BookingID_Date", "vehicle_no",
    "Origin_Location", "Destination_Location", "Org_lat_lon", "Des_lat_lon",
    "Data_Ping_time", "Planned_ETA", "Current_Location", "actual_eta",
    "Curr_lat", "Curr_lon", "ontime", "delay", "OriginLocation_Code",
    "DestinationLocation_Code", "trip_start_date", "trip_end_date",
    "TRANSPORTATION_DISTANCE_IN_KM", "vehicleType", "Driver_Name",
    "Driver_MobileNo", "customerID", "supplierID", "Material_Shipped"
]

def transform_consumed_logistics(record):
    """
    Cleans and prepares data received from Kafka/Avro for final use.
    
    Args:
        record (dict): The dictionary returned by AvroDeserializer
        
    Returns:
        dict: A cleaned dictionary with actual Python None/NaN instead of placeholders.
    """
    if not record:
        return None

    # 1. Restore actual Nulls from placeholders
    # This makes it easier for databases to handle the data correctly
    for key, value in record.items():
        if value == "Unknown":
            record[key] = None

    # 2. Handle Coordinate Placeholders
    # If both are 0.0, it's likely a missing GPS signal, not "Null Island"
    if record.get('Curr_lat') == 0.0 and record.get('Curr_lon') == 0.0:
        record['gps_valid'] = False
        record['Curr_lat'] = None
        record['Curr_lon'] = None
    else:
        record['gps_valid'] = True

    # 3. Business Logic: Calculate 'Delayed' flag
    # Helping downstream apps by pre-calculating status
    if record.get('ontime') == 'G':
        record['status'] = 'On Time'
    elif record.get('delay') == 'R':
        record['status'] = 'Delayed'
    else:
        record['status'] = 'Unknown'

    return record

def get_latest_schema(subject):
    schema = schema_registry_client.get_latest_version(subject).schema.schema_str
    return AvroDeserializer(schema_registry_client, schema)

# Consumer Initialization
logistics_consumer = DeserializingConsumer({
    **kafka_config,
    'key.deserializer': StringDeserializer('utf_8'),
    'value.deserializer': get_latest_schema('logistics_data-value')
})

logistics_consumer.subscribe(['logistics_data'])

try:
    while True:
        msg = logistics_consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print('Consumer error: {}'.format(msg.error()))
            continue

        record = msg.value()
        if record:
            results = transform_consumed_logistics(record)
            x = logisticscol.insert_one(results)
            print(x)

except KeyboardInterrupt:
    pass
finally:
    logistics_consumer.close()
    mongodbclient.close()