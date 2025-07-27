#!/bin/bash
# setup-pipeline.sh - Complete setup script for 2TB data processing pipeline

set -e

echo "🚀 Setting up Kubernetes Data Processing Pipeline"
echo "================================================"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to wait for pods
wait_for_pods() {
    namespace=$1
    label=$2
    expected_count=$3
    
    echo "⏳ Waiting for $expected_count pods with label $label in namespace $namespace..."
    while [ $(kubectl get pods -n $namespace -l $label -o json | jq '.items | length') -lt $expected_count ]; do
        sleep 5
    done
    
    # Wait for pods to be ready
    kubectl wait --for=condition=ready pod -l $label -n $namespace --timeout=300s
}

# Check prerequisites
echo -e "${YELLOW}Checking prerequisites...${NC}"
for cmd in kubectl helm docker kind; do
    if command_exists $cmd; then
        echo -e "${GREEN}✓${NC} $cmd is installed"
    else
        echo -e "${RED}✗${NC} $cmd is not installed. Please install it first."
        exit 1
    fi
done

# Create kind cluster if not exists
if ! kubectl cluster-info >/dev/null 2>&1; then
    echo -e "${YELLOW}Creating Kind cluster...${NC}"
    cat <<EOF | kind create cluster --name data-pipeline --config=-
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
  kubeadmConfigPatches:
  - |
    kind: InitConfiguration
    nodeRegistration:
      kubeletExtraArgs:
        node-labels: "ingress-ready=true"
  extraPortMappings:
  - containerPort: 80
    hostPort: 80
    protocol: TCP
  - containerPort: 443
    hostPort: 443
    protocol: TCP
- role: worker
  extraMounts:
  - hostPath: /tmp/data
    containerPath: /data
- role: worker
  extraMounts:
  - hostPath: /tmp/data
    containerPath: /data
- role: worker
  extraMounts:
  - hostPath: /tmp/data
    containerPath: /data
EOF
fi

# Add Helm repositories
echo -e "${YELLOW}Adding Helm repositories...${NC}"
helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator
helm repo add apache-airflow https://airflow.apache.org
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo add minio https://charts.min.io/
helm repo add confluentinc https://confluentinc.github.io/cp-helm-charts/
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

# Create namespaces
echo -e "${YELLOW}Creating namespaces...${NC}"
kubectl create namespace spark-operator --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace airflow --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace monitoring --dry-run=client -o yaml | kubectl apply -f -

# Install Spark Operator
echo -e "${YELLOW}Installing Spark Operator...${NC}"
helm upgrade --install spark-operator spark-operator/spark-operator \
    --namespace spark-operator \
    --set webhook.enable=true \
    --set sparkJobNamespace=default \
    --set serviceAccounts.spark.create=true \
    --wait

# Install MinIO
echo -e "${YELLOW}Installing MinIO...${NC}"
helm upgrade --install minio minio/minio \
    --namespace default \
    --set mode=distributed \
    --set replicas=4 \
    --set persistence.enabled=true \
    --set persistence.size=1Ti \
    --set rootUser=minioadmin \
    --set rootPassword=minioadmin \
    --wait

# Configure MinIO buckets
echo -e "${YELLOW}Configuring MinIO buckets...${NC}"
kubectl run --rm -i --tty minio-client --image=minio/mc --restart=Never -- /bin/sh -c "
mc alias set minio http://minio:9000 minioadmin minioadmin
mc mb minio/data-bucket
mc mb minio/checkpoint-bucket
exit 0
" || true

# Install MongoDB Sharded Cluster
echo -e "${YELLOW}Installing MongoDB Sharded Cluster...${NC}"
helm upgrade --install mongodb bitnami/mongodb-sharded \
    --namespace default \
    --set shards=3 \
    --set configsvr.replicas=3 \
    --set mongos.replicas=2 \
    --set shardsvr.dataNode.replicas=3 \
    --set auth.enabled=false \
    --set persistence.size=200Gi \
    --wait

# Install Kafka
echo -e "${YELLOW}Installing Kafka...${NC}"
helm upgrade --install kafka confluentinc/cp-helm-charts \
    --namespace default \
    --set cp-kafka.brokers=3 \
    --set cp-zookeeper.servers=3 \
    --set cp-kafka.imageTag=7.5.0 \
    --set cp-kafka.persistence.enabled=true \
    --set cp-kafka.persistence.size=100Gi \
    --wait

# Wait for Kafka to be ready
wait_for_pods default "app=cp-kafka" 3

# Create Kafka topic
echo -e "${YELLOW}Creating Kafka topics...${NC}"
kubectl exec -it kafka-cp-kafka-0 -- kafka-topics \
    --create \
    --topic data-feed \
    --partitions 10 \
    --replication-factor 3 \
    --bootstrap-server localhost:9092 \
    --if-not-exists

# Install Apache Airflow
echo -e "${YELLOW}Installing Apache Airflow...${NC}"
helm upgrade --install airflow apache-airflow/airflow \
    --namespace airflow \
    --set executor=KubernetesExecutor \
    --set images.airflow.repository=apache/airflow \
    --set images.airflow.tag=2.8.0 \
    --set dags.persistence.enabled=true \
    --set dags.persistence.size=10Gi \
    --set logs.persistence.enabled=true \
    --set logs.persistence.size=10Gi \
    --set config.core.dags_are_paused_at_creation=false \
    --set config.kubernetes.delete_worker_pods=true \
    --set config.kubernetes.worker_container_repository=apache/airflow \
    --set config.kubernetes.worker_container_tag=2.8.0 \
    --wait

# Install Prometheus & Grafana
echo -e "${YELLOW}Installing Prometheus & Grafana...${NC}"
helm upgrade --install monitoring prometheus-community/kube-prometheus-stack \
    --namespace monitoring \
    --set grafana.adminPassword=admin123 \
    --set prometheus.prometheusSpec.storageSpec.volumeClaimTemplate.spec.resources.requests.storage=50Gi \
    --set prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues=false \
    --wait

# Build and load Docker image
echo -e "${YELLOW}Building Spark processor Docker image...${NC}"
if [ -f "Dockerfile" ]; then
    docker build -t spark-processor:latest .
    kind load docker-image spark-processor:latest --name data-pipeline
else
    echo -e "${RED}Warning: Dockerfile not found. Please build the image manually.${NC}"
fi

# Apply Kubernetes configurations
echo -e "${YELLOW}Applying Kubernetes configurations...${NC}"
for file in k8s-configs/*.yaml; do
    if [ -f "$file" ]; then
        kubectl apply -f "$file"
    fi
done

# Create service accounts and RBAC
kubectl apply -f - <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: spark
  namespace: default
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: spark-role
rules:
  - apiGroups: [""]
    resources: ["pods", "services", "configmaps", "persistentvolumeclaims"]
    verbs: ["*"]
  - apiGroups: ["apps"]
    resources: ["deployments", "replicasets"]
    verbs: ["*"]
  - apiGroups: ["batch"]
    resources: ["jobs"]
    verbs: ["*"]
  - apiGroups: ["sparkoperator.k8s.io"]
    resources: ["sparkapplications", "scheduledsparkapplications"]
    verbs: ["*"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: spark-rolebinding
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: spark-role
subjects:
  - kind: ServiceAccount
    name: spark
    namespace: default
EOF

# Configure Airflow connections
echo -e "${YELLOW}Configuring Airflow connections...${NC}"
AIRFLOW_POD=$(kubectl get pods -n airflow -l component=webserver -o jsonpath='{.items[0].metadata.name}')

kubectl exec -n airflow $AIRFLOW_POD -- airflow connections add \
    'mongodb_default' \
    --conn-type 'mongo' \
    --conn-host 'mongodb-mongos.default.svc.cluster.local' \
    --conn-port 27017 || true

kubectl exec -n airflow $AIRFLOW_POD -- airflow connections add \
    'kubernetes_default' \
    --conn-type 'kubernetes' || true

# Port forwarding setup
echo -e "${YELLOW}Setting up port forwarding...${NC}"
cat > port-forward.sh <<'EOF'
#!/bin/bash
# Port forwarding script for accessing services

echo "Starting port forwarding for all services..."

# Kill existing port-forward processes
pkill -f "kubectl port-forward" || true

# Airflow UI
kubectl port-forward -n airflow svc/airflow-webserver 8080:8080 &

# Spark UI (when jobs are running)
# kubectl port-forward -n default svc/spark-ui 4040:4040 &

# MinIO Console
kubectl port-forward -n default svc/minio-console 9001:9001 &

# Grafana
kubectl port-forward -n monitoring svc/monitoring-grafana 3000:80 &

# Prometheus
kubectl port-forward -n monitoring svc/monitoring-kube-prometheus-prometheus 9090:9090 &

echo "Port forwarding started:"
echo "  - Airflow UI: http://localhost:8080 (username: admin, password: admin)"
echo "  - MinIO Console: http://localhost:9001 (username: minioadmin, password: minioadmin)"
echo "  - Grafana: http://localhost:3000 (username: admin, password: admin123)"
echo "  - Prometheus: http://localhost:9090"

wait
EOF

chmod +x port-forward.sh

# Generate sample data script
echo -e "${YELLOW}Creating sample data generation script...${NC}"
cat > generate-sample-data.sh <<'EOF'
#!/bin/bash
# Generate sample data for testing

echo "Generating sample data..."

# Create a Python script for data generation
cat > generate_data.py <<'PYTHON'
import os
import json
import random
import gzip
from datetime import datetime

def generate_sample_file(size_gb=1, output_dir="/tmp/data"):
    os.makedirs(output_dir, exist_ok=True)
    
    filename = f"sample_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json.gz"
    filepath = os.path.join(output_dir, filename)
    
    target_size = size_gb * 1024 * 1024 * 1024  # Convert GB to bytes
    current_size = 0
    
    with gzip.open(filepath, 'wt', encoding='utf-8') as f:
        while current_size < target_size:
            record = {
                "id": random.randint(1, 1000000),
                "timestamp": datetime.now().isoformat(),
                "sensor_data": {
                    "temperature": random.uniform(20, 30),
                    "humidity": random.uniform(40, 80),
                    "pressure": random.uniform(990, 1020)
                },
                "location": random.choice(["US", "EU", "ASIA"]),
                "data": "x" * random.randint(1000, 5000)  # Variable size data
            }
            
            line = json.dumps(record) + "\n"
            f.write(line)
            current_size += len(line.encode('utf-8'))
            
            if current_size % (100 * 1024 * 1024) == 0:  # Progress every 100MB
                print(f"Generated {current_size / (1024 * 1024 * 1024):.2f} GB")
    
    print(f"Generated file: {filepath} ({size_gb} GB)")
    return filepath

if __name__ == "__main__":
    # Generate 10GB sample file (scale up to 2TB as needed)
    generate_sample_file(10)
PYTHON

python3 generate_data.py

# Upload to MinIO
mc alias set minio http://localhost:9000 minioadmin minioadmin
mc mb minio/data-bucket/input/2tb-dataset --ignore-existing
mc cp /tmp/data/*.gz minio/data-bucket/input/2tb-dataset/
EOF

chmod +x generate-sample-data.sh

# Status check script
echo -e "${YELLOW}Creating status check script...${NC}"
cat > check-status.sh <<'EOF'
#!/bin/bash
# Check status of all components

echo "=== Kubernetes Data Pipeline Status ==="
echo

echo "📦 Namespaces:"
kubectl get namespaces | grep -E "(default|spark-operator|airflow|monitoring)"
echo

echo "🚀 Spark Operator:"
kubectl get pods -n spark-operator
echo

echo "💾 MongoDB:"
kubectl get pods -l app.kubernetes.io/name=mongodb-sharded
echo

echo "📁 MinIO:"
kubectl get pods -l app=minio
echo

echo "📊 Kafka:"
kubectl get pods | grep kafka
echo

echo "🔄 Airflow:"
kubectl get pods -n airflow
echo

echo "📈 Monitoring:"
kubectl get pods -n monitoring | grep -E "(prometheus|grafana)"
echo

echo "✨ Spark Applications:"
kubectl get sparkapplications
echo

echo "📊 Storage Usage:"
kubectl get pvc --all-namespaces
EOF

chmod +x check-status.sh

# Final summary
echo
echo -e "${GREEN}✅ Setup complete!${NC}"
echo
echo "📝 Next steps:"
echo "1. Run ./port-forward.sh to access web UIs"
echo "2. Run ./generate-sample-data.sh to create test data"
echo "3. Run ./check-status.sh to verify all components"
echo "4. Access the interactive dashboard by opening the HTML file in a browser"
echo
echo "🌐 Service URLs (after port-forward):"
echo "  - Airflow: http://localhost:8080"
echo "  - MinIO: http://localhost:9001"
echo "  - Grafana: http://localhost:3000"
echo "  - Prometheus: http://localhost:9090"
echo
echo "🚀 To submit a Spark job:"
echo "  kubectl apply -f spark-batch-application.yaml"
echo
echo "📊 To start the data generator:"
echo "  kubectl apply -f data-generator-deployment.yaml"
