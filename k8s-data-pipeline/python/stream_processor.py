from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.streaming import StreamingQuery
import zlib
import json
import logging
from typing import Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)

class StreamProcessor:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.spark = self._create_spark_session()
        
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session optimized for streaming"""
        return SparkSession.builder \
            .appName("2TB-Stream-Processor") \
            .config("spark.streaming.kafka.maxRatePerPartition", "100000") \
            .config("spark.sql.streaming.checkpointLocation", self.config['checkpoint_dir']) \
            .config("spark.sql.streaming.stateStore.rocksdb.changelog", "true") \
            .config("spark.sql.streaming.metricsEnabled", "true") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.mongodb.output.uri", self.config['mongodb_uri']) \
            .getOrCreate()
    
    def compress_batch(self, df, batch_id):
        """Compress and write each streaming batch"""
        logger.info(f"Processing batch {batch_id} with {df.count()} records")
        
        # Define compression UDF
        @udf(returnType=BinaryType())
        def compress_data(data):
            if data:
                return zlib.compress(data.encode('utf-8'), level=9)
            return None
        
        @udf(returnType=IntegerType())
        def get_size(data):
            return len(data) if data else 0
        
        # Parse JSON and apply compression
        parsed_df = df.select(
            col("key").cast("string").alias("key"),
            col("value").cast("string").alias("raw_value"),
            from_json(col("value").cast("string"), self._get_schema()).alias("data"),
            col("timestamp")
        )
        
        # Apply compression
        compressed_df = parsed_df.select(
            col("key"),
            col("data"),
            col("timestamp"),
            compress_data(col("raw_value")).alias("compressed_data"),
            get_size(col("raw_value")).alias("original_size"),
            get_size(compress_data(col("raw_value"))).alias("compressed_size")
        ).withColumn(
            "compression_ratio",
            col("original_size") / col("compressed_size")
        ).withColumn(
            "batch_id",
            lit(batch_id)
        ).withColumn(
            "process_timestamp",
            current_timestamp()
        )
        
        # Write to MongoDB
        compressed_df.write \
            .format("mongodb") \
            .mode("append") \
            .option("database", "compressed_stream") \
            .option("collection", "documents") \
            .save()
        
        # Log batch statistics
        stats = compressed_df.agg(
            count("*").alias("count"),
            avg("compression_ratio").alias("avg_ratio"),
            sum("original_size").alias("total_original"),
            sum("compressed_size").alias("total_compressed")
        ).collect()[0]
        
        logger.info(f"""
        Batch {batch_id} Complete:
        - Records: {stats['count']}
        - Avg Compression: {stats['avg_ratio']:.2f}:1
        - Original Size: {stats['total_original'] / (1024**2):.2f} MB
        - Compressed Size: {stats['total_compressed'] / (1024**2):.2f} MB
        """)
    
    def _get_schema(self) -> StructType:
        """Define schema for incoming data"""
        return StructType([
            StructField("timestamp", StringType(), True),
            StructField("sensor_id", StringType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("humidity", DoubleType(), True),
            StructField("pressure", DoubleType(), True),
            StructField("data_array", ArrayType(DoubleType()), True),
            StructField("metadata", StructType([
                StructField("location", StringType(), True),
                StructField("type", StringType(), True),
                StructField("quality", IntegerType(), True)
            ]), True)
        ])
    
    def start_streaming(self) -> StreamingQuery:
        """Start the streaming pipeline"""
        logger.info(f"Starting stream from {self.config['topic']}")
        
        # Read from Kafka
        stream_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.config['kafka_servers']) \
            .option("subscribe", self.config['topic']) \
            .option("startingOffsets", "earliest") \
            .option("maxOffsetsPerTrigger", 1000000) \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Process stream with error handling
        query = stream_df \
            .writeStream \
            .foreachBatch(self.compress_batch) \
            .outputMode("append") \
            .trigger(processingTime='30 seconds') \
            .option("checkpointLocation", self.config['checkpoint_dir']) \
            .start()
        
        return query
