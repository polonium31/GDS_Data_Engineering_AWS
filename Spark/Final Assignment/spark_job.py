import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit, window, sum, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType

# --- 1. Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# --- 2. Spark Session Configuration ---
spark = SparkSession.builder \
    .appName("StatefulAdsData") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0," \
            "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.cassandra.connection.host", "node-0.aws-us-east-1.3d14f0ed92b243eb1fc6.clusters.scylla.cloud") \
    .config("spark.cassandra.auth.username", "scylla") \
    .config("spark.cassandra.auth.password", "phqd32REiQ6vynO") \
    .config("spark.cassandra.connection.localDC", "AWS_US_EAST_1") \
    .getOrCreate()

# --- 3. Schema Definition ---
ads_schema = StructType([
    StructField("ad_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("clicks", IntegerType(), True),
    StructField("views", IntegerType(), True),
    StructField("cost", DoubleType(), True)
])

# --- 4. Read from Kafka ---
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "") \
    .option("subscribe", "ads_data") \
    .option("startingOffsets", "latest") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='' password='';") \
    .load()

# Deserialize JSON
ads_data = raw_stream.selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), ads_schema).alias("data")) \
    .select("data.*")

logger.info("Stream source initialized...")

# --- 5. Windowing Based Aggregation ---
aggregated_df = ads_data \
    .withWatermark("timestamp", "2 minutes") \
    .groupBy(
        window(col("timestamp"), "1 minute", "30 seconds"),
        col("ad_id")
    ).agg(
        sum("clicks").alias("total_clicks"),
        sum("views").alias("total_views"),
        avg("cost").alias("avg_cost_per_view")
    ).select(
        col("ad_id"),
        col("total_clicks"),
        col("total_views"),
        col("avg_cost_per_view")
    )


# --- 6. Write to ScyllaDB and Console ---
def write_to_scylla(batch_df, batch_id):
    logger.info(f"Processing batch: {batch_id}")
    
    print(f"--- Batch ID: {batch_id} Data ---")
    batch_df.show(truncate=False)
    
    batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="ads_metrics", keyspace="advertising") \
        .mode("append") \
        .save()

# Start the query
query = aggregated_df.writeStream \
    .foreachBatch(write_to_scylla) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/spark_checkpoints/ads_data") \
    .start()

query.awaitTermination()