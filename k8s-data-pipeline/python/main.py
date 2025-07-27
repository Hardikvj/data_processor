import os
import sys
from typing import Optional
import click
import logging
from batch_processor import BatchProcessor
from stream_processor import StreamProcessor
from data_generator import DataGenerator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@click.group()
def cli():
    """2TB Data Processing Pipeline for Kubernetes"""
    pass

@cli.command()
@click.option('--input-path', required=True, help='S3/MinIO path to input data')
@click.option('--compression-level', default=9, help='Compression level (1-9)')
@click.option('--batch-size', default=1000, help='MongoDB batch insert size')
@click.option('--partitions', default=200, help='Number of Spark partitions')
def batch(input_path: str, compression_level: int, batch_size: int, partitions: int):
    """Run batch processing mode"""
    logger.info(f"Starting batch processing from {input_path}")
    
    config = {
        'compression_level': compression_level,
        'batch_size': batch_size,
        'partitions': partitions,
        'mongodb_uri': os.getenv('MONGODB_URI', 'mongodb://mongodb-mongos:27017/')
    }
    
    processor = BatchProcessor(config)
    processor.process_batch(input_path)

@cli.command()
@click.option('--kafka-servers', default='kafka:9092', help='Kafka broker addresses')
@click.option('--topic', default='data-feed', help='Kafka topic name')
@click.option('--checkpoint-dir', default='/checkpoint', help='Checkpoint directory')
def stream(kafka_servers: str, topic: str, checkpoint_dir: str):
    """Run streaming mode"""
    logger.info(f"Starting stream processing from {topic}")
    
    config = {
        'kafka_servers': kafka_servers,
        'topic': topic,
        'checkpoint_dir': checkpoint_dir,
        'mongodb_uri': os.getenv('MONGODB_URI', 'mongodb://mongodb-mongos:27017/')
    }
    
    processor = StreamProcessor(config)
    query = processor.start_streaming()
    query.awaitTermination()

@cli.command()
@click.option('--rate-mb', default=10, help='Data generation rate in MB/s')
@click.option('--kafka-servers', default='kafka:9092', help='Kafka broker addresses')
@click.option('--duration', default=0, help='Duration in seconds (0 for continuous)')
def generate(rate_mb: int, kafka_servers: str, duration: int):
    """Generate sample data for testing"""
    logger.info(f"Starting data generation at {rate_mb} MB/s")
    
    generator = DataGenerator(kafka_servers)
    
    if duration > 0:
        generator.generate_for_duration(rate_mb, duration)
    else:
        generator.continuous_feed(rate_mb)

if __name__ == '__main__':
    cli()
