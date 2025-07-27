from kafka import KafkaProducer
import json
import random
import time
import numpy as np
from datetime import datetime
import logging
from typing import Optional
import threading

logger = logging.getLogger(__name__)

class DataGenerator:
    def __init__(self, kafka_servers: str = 'kafka:9092'):
        self.kafka_servers = kafka_servers
        self.producer = self._create_producer()
        self._stop_event = threading.Event()
        
    def _create_producer(self) -> KafkaProducer:
        """Create Kafka producer with optimal settings"""
        return KafkaProducer(
            bootstrap_servers=self.kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            compression_type='gzip',
            batch_size=16384 * 4,  # 64KB batches
            linger_ms=100,
            buffer_memory=33554432,  # 32MB buffer
            retries=3,
            acks='all'
        )
    
    def generate_sample_record(self) -> dict:
        """Generate a single sample record"""
        # Generate large array to increase record size
        array_size = random.randint(500, 2000)
        
        return {
            'timestamp': datetime.now().isoformat(),
            'sensor_id': f"sensor_{random.randint(1, 10000)}",
            'temperature': round(random.uniform(20, 30), 2),
            'humidity': round(random.uniform(40, 80), 2),
            'pressure': round(random.uniform(990, 1020), 2),
            'data_array': np.random.rand(array_size).tolist(),
            'metadata': {
                'location': random.choice(['US-EAST', 'US-WEST', 'EU-CENTRAL', 'ASIA-PACIFIC']),
                'type': random.choice(['IOT', 'INDUSTRIAL', 'WEATHER', 'ENVIRONMENTAL']),
                'quality': random.randint(1, 10),
                'tags': [f"tag_{i}" for i in range(random.randint(5, 15))],
                'measurements': {
                    f"metric_{i}": random.random() * 100 
                    for i in range(random.randint(10, 30))
                }
            },
            'raw_data': ''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=1000)),
            'processing_info': {
                'version': '1.0.0',
                'pipeline': 'streaming',
                'source': 'simulator'
            }
        }
    
    def generate_batch(self, size_mb: int = 100) -> list:
        """Generate a batch of records totaling approximately size_mb"""
        records = []
        current_size = 0
        target_size = size_mb * 1024 * 1024
        
        while current_size < target_size:
            record = self.generate_sample_record()
            record_size = len(json.dumps(record).encode('utf-8'))
            current_size += record_size
            records.append(record)
            
            # Log progress for large batches
            if len(records) % 1000 == 0:
                logger.debug(f"Generated {len(records)} records, {current_size / (1024*1024):.2f} MB")
        
        return records
    
    def send_batch(self, records: list, topic: str = 'data-feed'):
        """Send a batch of records to Kafka"""
        futures = []
        
        for i, record in enumerate(records):
            # Use sensor_id as key for partitioning
            key = record.get('sensor_id', f"default_{i}")
            future = self.producer.send(topic, key=key, value=record)
            futures.append(future)
            
            # Flush periodically
            if i % 1000 == 0:
                self.producer.flush()
        
        # Final flush
        self.producer.flush()
        
        # Check for errors
        errors = 0
        for future in futures:
            try:
                future.get(timeout=10)
            except Exception as e:
                logger.error(f"Failed to send message: {e}")
                errors += 1
        
        if errors > 0:
            logger.warning(f"{errors} messages failed to send")
        
        return len(records) - errors
    
    def continuous_feed(self, rate_mb_per_sec: int = 10):
        """Generate continuous data feed at specified rate"""
        logger.info(f"Starting continuous feed at {rate_mb_per_sec} MB/s")
        
        total_sent = 0
        start_time = time.time()
        
        while not self._stop_event.is_set():
            batch_start = time.time()
            
            # Generate and send batch
            records = self.generate_batch(rate_mb_per_sec)
            sent = self.send_batch(records)
            total_sent += sent
            
            # Calculate timing
            batch_duration = time.time() - batch_start
            total_duration = time.time() - start_time
            
            # Log statistics
            logger.info(f"""
            Batch sent:
            - Records: {sent}
            - Rate: {rate_mb_per_sec / batch_duration:.2f} MB/s
            - Total sent: {total_sent:,}
            - Avg rate: {(total_sent * rate_mb_per_sec) / total_duration:.2f} MB/s
            """)
            
            # Maintain rate
            if batch_duration < 1:
                time.sleep(1 - batch_duration)
    
    def generate_for_duration(self, rate_mb_per_sec: int, duration_seconds: int):
        """Generate data for a specific duration"""
        logger.info(f"Generating data at {rate_mb_per_sec} MB/s for {duration_seconds} seconds")
        
        # Run in thread
        feed_thread = threading.Thread(
            target=self.continuous_feed,
            args=(rate_mb_per_sec,)
        )
        feed_thread.start()
        
        # Wait for duration
        time.sleep(duration_seconds)
        
        # Stop generation
        self.stop()
        feed_thread.join()
        
        logger.info("Data generation complete")
    
    def stop(self):
        """Stop data generation"""
        self._stop_event.set()
        self.producer.close()
