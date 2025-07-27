from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pymongo
import zlib
import boto3
import pyarrow.parquet as pq
from typing import Iterator, Dict, Any
import logging

logger = logging.getLogger(__name__)

class BatchProcessor:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.spark = self._create_spark_session()
        self.mongo_client = pymongo.MongoClient(config['mongodb_uri'])
        self.db = self.mongo_client.compressed_data
        
    def _create_spark_session(self) -> SparkSession:
        """Create optimized Spark session for 2TB processing"""
        return SparkSession.builder \
            .appName("2TB-Batch-Processor") \
            .config("spark.executor.memory", "8g") \
            .config("spark.executor.cores", "4") \
            .config("spark.executor.instances", "10") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.shuffle.partitions", str(self.config['partitions'])) \
            .config("spark.mongodb.output.uri", self.config['mongodb_uri']) \
            .config("spark.mongodb.output.database", "compressed_data") \
            .config("spark.mongodb.output.collection", "documents") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .getOrCreate()
    
    def compress_partition(self, iterator: Iterator[Row]) -> Iterator[Dict]:
        """Compress data in each partition with optimal settings"""
        batch = []
        batch_size = self.config['batch_size']
        compression_level = self.config['compression_level']
        
        for row in iterator:
            # Convert row to dict
            data = row.asDict()
            
            # Serialize and compress
            serialized = json.dumps(data).encode('utf-8')
            compressed = zlib.compress(serialized, level=compression_level)
            
            # Calculate metrics
            original_size = len(serialized)
            compressed_size = len(compressed)
            
            record = {
                '_id': data.get('id', str(uuid.uuid4())),
                'original_size': original_size,
                'compressed_size': compressed_size,
                'compression_ratio': original_size / compressed_size,
                'data': compressed,
                'metadata': {
                    'timestamp': datetime.now().isoformat(),
                    'compression_level': compression_level,
                    'algorithm': 'zlib'
                }
            }
            
            batch.append(record)
            
            # Yield batch when full
            if len(batch) >= batch_size:
                yield from batch
                batch = []
        
        # Yield remaining records
        if batch:
            yield from batch
    
    def process_batch(self, input_path: str):
        """Main batch processing function"""
        logger.info(f"Processing batch from: {input_path}")
        
        # Detect file format
        if input_path.endswith('.parquet'):
            df = self.spark.read.parquet(input_path)
        elif input_path.endswith('.json'):
            df = self.spark.read.json(input_path)
        elif input_path.endswith('.csv'):
            df = self.spark.read.csv(input_path, header=True, inferSchema=True)
        else:
            # Assume it's a directory of files
            df = self.spark.read.parquet(input_path)
        
        # Log initial stats
        total_partitions = df.rdd.getNumPartitions()
        logger.info(f"Initial partitions: {total_partitions}")
        
        # Repartition for optimal processing (100MB per partition)
        optimal_partitions = self.config['partitions']
        df = df.repartition(optimal_partitions)
        
        # Add processing timestamp
        df = df.withColumn("process_timestamp", current_timestamp())
        
        # Process and compress
        compressed_rdd = df.rdd.mapPartitions(self.compress_partition)
        
        # Write to MongoDB with progress tracking
        total_records = 0
        
        def write_partition(partition):
            nonlocal total_records
            batch = []
            partition_records = 0
            
            for record in partition:
                batch.append(record)
                partition_records += 1
                
                if len(batch) >= self.config['batch_size']:
                    self.db.documents.insert_many(batch, ordered=False)
                    batch = []
            
            # Insert remaining
            if batch:
                self.db.documents.insert_many(batch, ordered=False)
            
            total_records += partition_records
            logger.info(f"Partition complete. Records: {partition_records}")
        
        compressed_rdd.foreachPartition(write_partition)
        
        # Create indexes for better query performance
        self._create_indexes()
        
        # Log final stats
        self._log_stats()
    
    def _create_indexes(self):
        """Create MongoDB indexes for performance"""
        logger.info("Creating MongoDB indexes")
        
        self.db.documents.create_index([("compression_ratio", -1)])
        self.db.documents.create_index([("metadata.timestamp", -1)])
        self.db.documents.create_index([("original_size", -1)])
        self.db.documents.create_index([("compressed_size", -1)])
    
    def _log_stats(self):
        """Log processing statistics"""
        stats = self.db.command("collStats", "documents")
        
        total_docs = stats.get("count", 0)
        total_size = stats.get("size", 0)
        storage_size = stats.get("storageSize", 0)
        
        avg_compression = list(self.db.documents.aggregate([
            {"$group": {
                "_id": None,
                "avg_ratio": {"$avg": "$compression_ratio"},
                "total_original": {"$sum": "$original_size"},
                "total_compressed": {"$sum": "$compressed_size"}
            }}
        ]))[0]
        
        logger.info(f"""
        Processing Complete:
        - Total Documents: {total_docs:,}
        - Collection Size: {total_size / (1024**3):.2f} GB
        - Storage Size: {storage_size / (1024**3):.2f} GB
        - Average Compression Ratio: {avg_compression['avg_ratio']:.2f}:1
        - Total Original Size: {avg_compression['total_original'] / (1024**3):.2f} GB
        - Total Compressed Size: {avg_compression['total_compressed'] / (1024**3):.2f} GB
        """)
