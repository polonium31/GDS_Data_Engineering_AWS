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

# Fields that are "type": "string" (Not Nullable)
MANDATORY_STRING_FIELDS = [
    "GpsProvider", "BookingID", "MarketType", "BookingID_Date", "vehicle_no",
    "Origin_Location", "Destination_Location", "Org_lat_lon", "Des_lat_lon",
    "Data_Ping_time", "Planned_ETA", "Current_Location", "OriginLocation_Code",
    "DestinationLocation_Code", "customerID", "supplierID", "Material_Shipped"
]

def get_latest_schema(subject):
    schema = schema_registry_client.get_latest_version(subject).schema.schema_str
    return AvroSerializer(schema_registry_client, schema)

# Producer Initialization
logistics_producer = SerializingProducer({
    **kafka_config,
    'key.serializer': StringSerializer('utf_8'),
    'value.serializer': get_latest_schema('logistics_data-value')
})

# Load and Preprocess Data
df = pd.read_csv("delivery_trip_truck_data.csv")

# 1. Fix column name mismatch
df.rename(columns={"Material Shipped": "Material_Shipped"}, inplace=True)

# 2. Clean literal NULLs
df.replace(["NULL", "NA", ""], None, inplace=True)

# 3. Numeric conversions
df["TRANSPORTATION_DISTANCE_IN_KM"] = pd.to_numeric(df["TRANSPORTATION_DISTANCE_IN_KM"], errors="coerce")
df["Curr_lat"] = pd.to_numeric(df["Curr_lat"], errors="coerce").fillna(0.0)
df["Curr_lon"] = pd.to_numeric(df["Curr_lon"], errors="coerce").fillna(0.0)

print("Starting production...")

for index, row in df.iterrows():
    # Only keep columns defined in the schema to avoid "extra field" errors
    full_record = row.to_dict()
    record = {k: full_record[k] for k in SCHEMA_FIELDS if k in full_record}

    for k, v in record.items():
        # Handle NaN/None values based on Schema requirements
        if v is None or (isinstance(v, float) and math.isnan(v)):
            if k in MANDATORY_STRING_FIELDS:
                record[k] = "Unknown"  # Avro 'string' cannot be None
            else:
                record[k] = None       # Avro '["null", "string"]' can be None
        
        # Ensure ID/Code fields are strings (handles numeric codes in CSV)
        if k in ["Driver_MobileNo", "DestinationLocation_Code", "OriginLocation_Code"]:
            if record[k] is not None:
                record[k] = str(record[k])

    try:
        logistics_producer.produce(
            topic="logistics_data",
            key=str(record["vehicle_no"]),
            value=record,
            on_delivery=delivery_report
        )
        time.sleep(5)
    except Exception as e:
        print(f"Error on row {index}: {e}")

    logistics_producer.poll(0)

# Final flush
logistics_producer.flush()
print('All messages processed.')