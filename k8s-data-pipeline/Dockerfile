# Dockerfile
FROM apache/spark:3.5.0-python3

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    gcc \
    g++ \
    make \
    libssl-dev \
    libffi-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy application files
COPY requirements.txt /opt/spark/apps/
COPY *.py /opt/spark/apps/

# Install Python dependencies
RUN pip3 install --upgrade pip && \
    pip3 install -r /opt/spark/apps/requirements.txt

# Download necessary JARs for Spark
RUN cd /opt/spark/jars && \
    wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    wget -q https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.2.0/mongo-spark-connector_2.12-10.2.0.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# Set permissions
RUN chmod -R 755 /opt/spark/apps/

# Switch back to spark user
USER ${spark_uid}

# Set working directory
WORKDIR /opt/spark/apps

# Default command
CMD ["python3", "main.py"]

---
# docker-compose.yaml (for local testing)
version: '3.8'

services:
  # MongoDB Sharded Cluster
  mongos1:
    image: mongo:7.0
    command: mongos --configdb cfgrs/mongocfg1:27019,mongocfg2:27019,mongocfg3:27019 --bind_ip_all
    ports:
      - "27017:27017"
    networks:
      - mongo-network

  mongocfg1:
    image: mongo:7.0
    command: mongod --configsvr --replSet cfgrs --port 27019 --dbpath /data/db
    volumes:
      - mongocfg1:/data/db
    networks:
      - mongo-network

  mongoshard1:
    image: mongo:7.0
    command: mongod --shardsvr --replSet shard1rs --port 27018 --dbpath /data/db
    volumes:
      - mongoshard1:/data/db
    networks:
      - mongo-network

  mongoshard2:
    image: mongo:7.0
    command: mongod --shardsvr --replSet shard2rs --port 27018 --dbpath /data/db
    volumes:
      - mongoshard2:/data/db
    networks:
      - mongo-network

  mongoshard3:
    image: mongo:7.0
    command: mongod --shardsvr --replSet shard3rs --port 27018 --dbpath /data/db
    volumes:
      - mongoshard3:/data/db
    networks:
      - mongo-network

  # MinIO Object Storage
  minio:
    image: minio/minio:latest
    command: server /data --console-address ":9001"
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    volumes:
      - minio-data:/data
    networks:
      - mongo-network

  # Kafka
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    networks:
      - mongo-network

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
      - "29092:29092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    networks:
      - mongo-network

  # Spark Master
  spark-master:
    build: .
    image: spark-processor:latest
    command: /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master
    ports:
      - "8080:8080"
      - "7077:7077"
    environment:
      SPARK_MODE: master
      SPARK_MASTER_HOST: spark-master
      SPARK_MASTER_PORT: 7077
      SPARK_MASTER_WEBUI_PORT: 8080
    networks:
      - mongo-network

  # Spark Worker
  spark-worker:
    build: .
    image: spark-processor:latest
    command: /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
    depends_on:
      - spark-master
    environment:
      SPARK_MODE: worker
      SPARK_MASTER_URL: spark://spark-master:7077
      SPARK_WORKER_CORES: 2
      SPARK_WORKER_MEMORY: 4g
      SPARK_DRIVER_MEMORY: 2g
      SPARK_EXECUTOR_MEMORY: 2g
    networks:
      - mongo-network

  # Airflow
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres-db:/var/lib/postgresql/data
    networks:
      - mongo-network

  airflow-init:
    image: apache/airflow:2.8.0
    depends_on:
      - postgres
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__CORE__FERNET_KEY: ''
      AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
      AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
      _AIRFLOW_DB_UPGRADE: 'true'
      _AIRFLOW_WWW_USER_CREATE: 'true'
      _AIRFLOW_WWW_USER_USERNAME: admin
      _AIRFLOW_WWW_USER_PASSWORD: admin
    volumes:
      - ./airflow/dags:/opt/airflow/dags
    entrypoint: /bin/bash
    command: |
      -c "
      airflow db init &&
      airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin
      "
    networks:
      - mongo-network

  airflow-webserver:
    image: apache/airflow:2.8.0
    depends_on:
      - airflow-init
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__CORE__FERNET_KEY: ''
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./airflow/logs:/opt/airflow/logs
    ports:
      - "8088:8080"
    command: webserver
    networks:
      - mongo-network

  airflow-scheduler:
    image: apache/airflow:2.8.0
    depends_on:
      - airflow-init
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__CORE__FERNET_KEY: ''
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./airflow/logs:/opt/airflow/logs
    command: scheduler
    networks:
      - mongo-network

  # Prometheus
  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus/prometheus.yml:/etc/prometheus/prometheus.yml
      - prometheus-data:/prometheus
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.path=/prometheus'
    networks:
      - mongo-network

  # Grafana
  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin123
      GF_USERS_ALLOW_SIGN_UP: "false"
    volumes:
      - grafana-data:/var/lib/grafana
      - ./grafana/dashboards:/etc/grafana/provisioning/dashboards
      - ./grafana/datasources:/etc/grafana/provisioning/datasources
    networks:
      - mongo-network

networks:
  mongo-network:
    driver: bridge

volumes:
  mongocfg1:
  mongoshard1:
  mongoshard2:
  mongoshard3:
  minio-data:
  postgres-db:
  prometheus-data:
  grafana-data:

---
# Makefile for easy operations
.PHONY: build up down logs clean

# Build Docker image
build:
	docker build -t spark-processor:latest .

# Start all services
up:
	docker-compose up -d
	@echo "Waiting for services to start..."
	@sleep 30
	@echo "Creating MongoDB shards..."
	docker-compose exec mongos1 mongosh --eval 'sh.addShard("shard1rs/mongoshard1:27018")'
	docker-compose exec mongos1 mongosh --eval 'sh.addShard("shard2rs/mongoshard2:27018")'
	docker-compose exec mongos1 mongosh --eval 'sh.addShard("shard3rs/mongoshard3:27018")'
	@echo "Creating Kafka topics..."
	docker-compose exec kafka kafka-topics --create --topic data-feed --partitions 10 --replication-factor 1 --bootstrap-server localhost:9092 || true
	@echo "Services are ready!"
	@echo "Access points:"
	@echo "  - Spark Master: http://localhost:8080"
	@echo "  - Airflow: http://localhost:8088 (admin/admin)"
	@echo "  - MinIO: http://localhost:9001 (minioadmin/minioadmin)"
	@echo "  - Grafana: http://localhost:3000 (admin/admin123)"
	@echo "  - Prometheus: http://localhost:9090"

# Stop all services
down:
	docker-compose down

# View logs
logs:
	docker-compose logs -f

# Clean everything
clean:
	docker-compose down -v
	docker rmi spark-processor:latest || true

# Run batch processor
run-batch:
	docker-compose run --rm spark-worker python /opt/spark/apps/main.py batch \
		--input-path s3a://data-bucket/input/sample-data.parquet

# Run stream processor
run-stream:
	docker-compose run --rm spark-worker python /opt/spark/apps/main.py stream \
		--kafka-servers kafka:9092 --topic data-feed

# Run data generator
run-generator:
	docker-compose run --rm spark-worker python /opt/spark/apps/main.py generate \
		--rate-mb 10 --kafka-servers kafka:9092

---
# prometheus/prometheus.yml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'spark'
    static_configs:
      - targets: ['spark-master:8080', 'spark-worker:8081']
    metrics_path: '/metrics/prometheus'

  - job_name: 'kafka'
    static_configs:
      - targets: ['kafka:9092']

  - job_name: 'mongodb'
    static_configs:
      - targets: ['mongos1:27017']

---
# .env file for environment variables
# MongoDB Configuration
MONGO_INITDB_ROOT_USERNAME=admin
MONGO_INITDB_ROOT_PASSWORD=admin123
MONGODB_URI=mongodb://mongos1:27017/

# MinIO Configuration
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_ENDPOINT=http://minio:9000

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=kafka:9092

# Spark Configuration
SPARK_MASTER_URL=spark://spark-master:7077
SPARK_EXECUTOR_MEMORY=4g
SPARK_EXECUTOR_CORES=2

# Airflow Configuration
AIRFLOW_USERNAME=admin
AIRFLOW_PASSWORD=admin

---
# scripts/init-minio.sh
#!/bin/bash
# Initialize MinIO buckets

# Wait for MinIO to be ready
until mc alias set minio http://minio:9000 minioadmin minioadmin; do
  echo "Waiting for MinIO..."
  sleep 5
done

# Create buckets
mc mb minio/data-bucket --ignore-existing
mc mb minio/checkpoint-bucket --ignore-existing

# Set bucket policies
mc anonymous set download minio/data-bucket
mc anonymous set download minio/checkpoint-bucket

echo "MinIO initialization complete!"

---
# scripts/init-mongodb.js
// Initialize MongoDB sharding

// Connect to mongos
db = db.getSiblingDB('admin');

// Add shards
sh.addShard("shard1rs/mongoshard1:27018");
sh.addShard("shard2rs/mongoshard2:27018");
sh.addShard("shard3rs/mongoshard3:27018");

// Enable sharding on database
sh.enableSharding("compressed_data");
sh.enableSharding("compressed_stream");

// Shard collections
sh.shardCollection("compressed_data.documents", { "_id": "hashed" });
sh.shardCollection("compressed_stream.documents", { "_id": "hashed" });

print("MongoDB sharding configuration complete!");

---
# airflow/dags/batch_processing_dag.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'batch_2tb_processing_local',
    default_args=default_args,
    description='Process 2TB compressed data to MongoDB (Docker Compose)',
    schedule_interval='@daily',
    catchup=False
)

# Submit Spark job
submit_spark = BashOperator(
    task_id='submit_batch_processor',
    bash_command="""
    docker exec spark-worker spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --driver-memory 2g \
        --executor-memory 4g \
        --executor-cores 2 \
        --num-executors 2 \
        /opt/spark/apps/main.py batch \
        --input-path s3a://data-bucket/input/sample-data.parquet
    """,
    dag=dag
)

submit_spark
