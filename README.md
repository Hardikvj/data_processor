# Kubernetes-Native 2TB Data Processing Pipeline

A production-ready, scalable data processing system built on Kubernetes that handles 2TB of data with 10:1 compression using Apache Spark, Airflow, and MongoDB.

## 🏗️ Architecture Overview

This system provides two processing modes:
- **Batch Processing**: Process static 2TB compressed files
- **Stream Processing**: Handle continuous data feeds in real-time

### Components

1. **Apache Spark**: Distributed data processing engine
2. **Apache Airflow**: Workflow orchestration
3. **MongoDB Sharded**: Scalable document storage
4. **MinIO**: S3-compatible object storage
5. **Apache Kafka**: Streaming data platform
6. **Prometheus + Grafana**: Monitoring and visualization

## 📋 Prerequisites

- Docker Desktop with Kubernetes enabled
- Kind (Kubernetes in Docker)
- Helm 3.x
- kubectl
- Python 3.8+
- 16GB+ RAM recommended
- 500GB+ available disk space

## 🚀 Quick Start

### 1. Clone the Repository
```bash
git clone <repository-url>
cd k8s-data-pipeline
```

### 2. Run the Setup Script
```bash
chmod +x setup-pipeline.sh
./setup-pipeline.sh
```

This script will:
- Create a Kind cluster with 3 worker nodes
- Install all required Helm charts
- Configure storage and networking
- Build and load Docker images
- Set up RBAC and service accounts

### 3. Access the Services
```bash
./port-forward.sh
```

Then access:
- **Interactive Dashboard**: Open `k8s-data-pipeline-dashboard.html` in your browser
- **Airflow UI**: http://localhost:8080 (admin/admin)
- **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin)
- **Grafana**: http://localhost:3000 (admin/admin123)
- **Prometheus**: http://localhost:9090

## 📊 Processing Scenarios

### Scenario 1: Batch Processing

Process static 2TB compressed files:

```bash
# Generate sample data (scaled down for testing)
./generate-sample-data.sh

# Submit Spark batch job
kubectl apply -f k8s-configs/spark-batch-application.yaml

# Monitor progress
kubectl logs -f spark-batch-processor-2tb-driver
```

### Scenario 2: Stream Processing

Handle continuous data feeds:

```bash
# Start data generator
kubectl apply -f k8s-configs/data-generator-deployment.yaml

# Start stream processor
kubectl apply -f k8s-configs/spark-streaming-application.yaml

# Monitor stream
kubectl logs -f spark-stream-processor-2tb-driver
```

## 🎯 Key Features

### Data Compression
- Achieves 10:1 compression ratio using zlib level 9
- Processes data in optimal partition sizes
- Stores compressed data with metadata in MongoDB

### Scalability
- Horizontal scaling with Spark executors
- MongoDB sharding for distributed storage
- Dynamic resource allocation based on workload

### Fault Tolerance
- Checkpoint recovery for streaming
- Automatic retry mechanisms
- Persistent storage for critical data

### Monitoring
- Real-time metrics with Prometheus
- Custom Grafana dashboards
- Spark UI for job monitoring
- Airflow for workflow visibility

## 📁 Project Structure

```
k8s-data-pipeline/
├── k8s-configs/               # Kubernetes YAML configurations
│   ├── spark-batch-application.yaml
│   ├── spark-streaming-application.yaml
│   ├── airflow-dags-configmap.yaml
│   └── ...
├── python/                    # Python application code
│   ├── main.py               # CLI entry point
│   ├── batch_processor.py    # Batch processing logic
│   ├── stream_processor.py   # Stream processing logic
│   └── data_generator.py     # Test data generation
├── Dockerfile                # Docker image for Spark
├── setup-pipeline.sh         # Main setup script
├── port-forward.sh          # Service access script
└── k8s-data-pipeline-dashboard.html  # Interactive UI

```

## 🔧 Configuration

### Spark Configuration
Edit `spark-batch-application.yaml`:
```yaml
sparkConf:
  "spark.executor.memory": "8g"
  "spark.executor.cores": "4"
  "spark.executor.instances": "10"
```

### MongoDB Sharding
Configure in Helm values:
```yaml
shards: 3
shardsvr:
  dataNode:
    replicas: 3
```

### Kafka Topics
```bash
# Create additional topics
kubectl exec -it kafka-cp-kafka-0 -- kafka-topics \
  --create --topic my-topic \
  --partitions 20 \
  --replication-factor 3
```

## 📈 Performance Tuning

### Batch Processing
- Optimal partition size: 100-200MB per partition
- Compression level: 9 for maximum compression
- Batch insert size: 1000 documents

### Stream Processing
- Max rate per partition: 100,000 records/sec
- Checkpoint interval: 30 seconds
- Trigger interval: processingTime='30 seconds'

### MongoDB
- Sharding key: Based on data distribution
- Indexes: Created on compression_ratio, timestamp
- Write concern: Majority for durability

## 🔍 Monitoring & Debugging

### Check Component Status
```bash
./check-status.sh
```

### View Spark Logs
```bash
# List Spark applications
kubectl get sparkapplications

# View driver logs
kubectl logs <spark-app-name>-driver

# View executor logs
kubectl logs -l spark-role=executor
```

### Airflow DAG Management
```bash
# Trigger DAG manually
kubectl exec -n airflow deployment/airflow-webserver -- \
  airflow dags trigger batch_2tb_processing
```

### MongoDB Operations
```bash
# Connect to MongoDB
kubectl exec -it mongodb-mongos-0 -- mongosh

# Check compression stats
db.compressed_data.documents.aggregate([
  { $group: { 
    _id: null, 
    avgRatio: { $avg: "$compression_ratio" },
    totalDocs: { $sum: 1 }
  }}
])
```

## 🛠️ Troubleshooting

### Common Issues

1. **Insufficient Resources**
   ```bash
   # Increase Kind cluster resources
   docker system prune -af
   # Edit kind config to add more workers
   ```

2. **Spark Job Failures**
   ```bash
   # Check events
   kubectl describe sparkapplication <app-name>
   # Increase memory/cores in spark config
   ```

3. **MongoDB Connection Issues**
   ```bash
   # Verify service endpoints
   kubectl get svc | grep mongo
   # Check MongoDB pod status
   kubectl get pods -l app.kubernetes.io/name=mongodb-sharded
   ```

## 🔐 Security Considerations

- Enable authentication for all services in production
- Use network policies to restrict pod communication
- Implement RBAC with least privilege principle
- Encrypt data at rest and in transit
- Regular security updates for all components

## 📊 Performance Metrics

Expected performance with recommended configuration:
- Batch processing: ~100-200 MB/s throughput
- Stream processing: ~1M records/minute
- Compression ratio: 8-12:1 depending on data
- MongoDB write speed: ~50k documents/second

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## 📜 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- Apache Spark community
- Kubernetes SIG Big Data
- MongoDB engineering team
- Open source contributors

---

**Note**: This is a demonstration system. For production use, ensure proper security, backup, and disaster recovery mechanisms are in place.
