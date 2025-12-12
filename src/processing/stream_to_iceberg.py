import logging
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('stream_to_iceberg')

def create_spark_session():
    return SparkSession.builder \
        .appName("SmartFarm-Streaming-ETL") \
        .getOrCreate()

def main():
    spark = create_spark_session()
    logger.info("✅ Spark Session Created")

    try:
        logger.info("🛠 Checking/Creating Namespace 'iceberg.smartfarm'...")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.smartfarm")
        logger.info("✅ Namespace 'iceberg.smartfarm' is ready.")
    except Exception as e:
        logger.error(f"❌ Failed to create namespace: {e}")

    schema = StructType([
        StructField("sensor_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("co2_level", IntegerType(), True),
        StructField("battery_voltage", DoubleType(), True),
        StructField("status", StringType(), True)
    ])

    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "smartfarm-sensor") \
        .option("startingOffsets", "latest") \
        .load()
    df_parsed = df_raw.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    logger.info("🛠 Creating Table 'iceberg.smartfarm.sensor_data' if not exists...")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.smartfarm.sensor_data (
            sensor_id STRING,
            timestamp TIMESTAMP,
            temperature DOUBLE,
            humidity DOUBLE,
            co2_level INT,
            battery_voltage DOUBLE,
            status STRING
        )
        USING iceberg
        PARTITIONED BY (sensor_id)
    """)

    logger.info("✅ Table is ready.")
    logger.info("🚀 Starting Streaming Query...")

    query = df_parsed.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime="5 seconds") \
        .option("path", "iceberg.smartfarm.sensor_data") \
        .option("checkpointLocation", "s3://lakehouse/checkpoints/sensor_data") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()