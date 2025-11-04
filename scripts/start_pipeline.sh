#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# Pipeline Startup Script
# ============================================================================
# Deploys Kafka, MinIO, Producer, and Spark to Kubernetes
# Usage: ./scripts/start_pipeline.sh [--no-port-forward] [--debug]

# ----------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
K8S_DIR="$ROOT_DIR/k8s"
NAMESPACE="hugedata"

KAFKA_TOPIC="traffic"
KAFKA_PARTITIONS=3
KAFKA_REPLICATION=1

MINIO_ALIAS="local"
MINIO_URL="http://localhost:9000"
MINIO_CLUSTER_URL="http://minio.${NAMESPACE}.svc.cluster.local:9000"
MINIO_USER="minioadmin"
MINIO_PASS="minioadmin"
BUCKETS=("traffic-data")

PORT_FORWARD=true
DEBUG=false

LOCAL_MINIO_PORT="${LOCAL_MINIO_PORT:-9000}"
LOCAL_MINIO_CONSOLE_PORT="${LOCAL_MINIO_CONSOLE_PORT:-9001}"
LOCAL_KAFKA_PORT="${LOCAL_KAFKA_PORT:-9094}"

# Detect platform
IS_WINDOWS=false
if command -v powershell.exe >/dev/null 2>&1 || [[ "$(uname -s 2>/dev/null || echo '')" == *NT* ]]; then
  IS_WINDOWS=true
fi

# Validate ports
validate_port() {
  local port="$1" default="$2"
  if [[ -z "$port" ]]; then
    echo "$default"
    return
  fi
  if [[ "$port" =~ ^[0-9]+$ ]] && ((port >= 1 && port <= 65535)); then
    echo "$port"
  else
    log_warn "Invalid port value '$port'; using default $default"
    echo "$default"
  fi
}

LOCAL_MINIO_PORT=$(validate_port "$LOCAL_MINIO_PORT" 9000)
LOCAL_MINIO_CONSOLE_PORT=$(validate_port "$LOCAL_MINIO_CONSOLE_PORT" 9001)
LOCAL_KAFKA_PORT=$(validate_port "$LOCAL_KAFKA_PORT" 9094)

# ----------------------------------------------------------------------------
# Parse Arguments
# ----------------------------------------------------------------------------
for arg in "$@"; do
  case "$arg" in
    --no-port-forward) PORT_FORWARD=false ;;
    --debug) DEBUG=true ;;
    *) echo "Unknown option: $arg"; exit 1 ;;
  esac
done

# ----------------------------------------------------------------------------
# Logging Functions
# ----------------------------------------------------------------------------
log_info()  { echo -e "\033[0;32m[INFO]\033[0m  $*"; }
log_warn()  { echo -e "\033[0;33m[WARN]\033[0m  $*" >&2; }
log_error() { echo -e "\033[0;31m[ERROR]\033[0m $*" >&2; }
log_debug() { [[ "$DEBUG" == true ]] && echo -e "\033[0;36m[DEBUG]\033[0m $*"; }
log_section() { echo -e "\n\033[1;34m==>\033[0m \033[1m$*\033[0m"; }

[[ "$DEBUG" == true ]] && set -x

# ----------------------------------------------------------------------------
# Utility Functions
# ----------------------------------------------------------------------------
check_command() {
  command -v "$1" >/dev/null 2>&1 || {
    log_error "Required command '$1' not found"
    exit 1
  }
}

wait_for_pods() {
  local selector="$1"
  local timeout="${2:-120}"

  log_info "Waiting for pods: $selector (timeout: ${timeout}s)"

  if kubectl wait --for=condition=ready pod -l "$selector" \
     -n "$NAMESPACE" --timeout="${timeout}s" 2>/dev/null; then
    log_info "Pods ready: $selector"
    return 0
  else
    log_warn "Timeout waiting for pods: $selector"
    return 1
  fi
}

get_kafka_pod() {
  kubectl get pods -n "$NAMESPACE" -l app=kafka \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo ""
}

safe_kubectl_apply() {
  local file="$1"
  if [[ ! -f "$file" ]]; then
    log_error "Manifest not found: $file"
    return 1
  fi

  local exitcode=0
  set +e
  kubectl apply -f "$file" -n "$NAMESPACE"
  exitcode=$?
  set -e

  if [[ $exitcode -ne 0 ]]; then
    log_warn "kubectl apply failed for $file (exit $exitcode)"
  else
    log_info "Applied $file"
  fi
  return $exitcode
}

# ----------------------------------------------------------------------------
# Kafka Topic Management
# ----------------------------------------------------------------------------
create_kafka_topic() {
  local pod_name="$1"

  if [[ -z "$pod_name" ]]; then
    log_warn "No Kafka pod available for topic creation"
    return 0
  fi

  log_info "Creating Kafka topic: $KAFKA_TOPIC"

  local kafka_cmd="kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --topic $KAFKA_TOPIC \
    --partitions $KAFKA_PARTITIONS \
    --replication-factor $KAFKA_REPLICATION \
    --if-not-exists"

  if kubectl exec -n "$NAMESPACE" "$pod_name" -- bash -c "$kafka_cmd" 2>/dev/null; then
    log_info "Topic '$KAFKA_TOPIC' ready"
  else
    log_warn "Failed to create topic (may already exist)"
  fi
}

# ----------------------------------------------------------------------------
# MinIO Bucket Management
# ----------------------------------------------------------------------------
create_buckets_local() {
  if ! command -v mc >/dev/null 2>&1; then
    log_debug "MinIO client 'mc' not found locally"
    return 1
  fi

  log_info "Configuring local MinIO client"
  mc alias set "$MINIO_ALIAS" "$MINIO_URL" "$MINIO_USER" "$MINIO_PASS" --api S3v4 2>/dev/null || true

  for bucket in "${BUCKETS[@]}"; do
    log_info "Creating bucket: $bucket"
    mc mb "$MINIO_ALIAS/$bucket" 2>/dev/null || log_debug "Bucket may already exist"
  done

  return 0
}

create_buckets_cluster() {
  log_info "Creating buckets in-cluster"

  local pod_name="mc-bucket-creator-$(date +%s)"
  local manifest="/tmp/${pod_name}.yaml"

  local bucket_cmds=""
  for bucket in "${BUCKETS[@]}"; do
    bucket_cmds+="mc mb --ignore-existing local/$bucket; "
  done

  local commands="mc alias set local $MINIO_CLUSTER_URL $MINIO_USER $MINIO_PASS --api S3v4 && ${bucket_cmds} mc ls local"

  cat > "$manifest" <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $pod_name
  namespace: $NAMESPACE
spec:
  restartPolicy: Never
  containers:
  - name: mc
    image: minio/mc
    imagePullPolicy: IfNotPresent
    command: ["/bin/sh", "-c", "$commands"]
EOF

  log_debug "Applying bucket creation pod"
  kubectl apply -f "$manifest" >/dev/null 2>&1

  if kubectl wait --for=condition=complete pod/$pod_name -n "$NAMESPACE" --timeout=120s >/dev/null 2>&1; then
    log_info "Buckets created successfully"
    kubectl logs -n "$NAMESPACE" "$pod_name" 2>/dev/null | grep -E "Bucket|local/" || true
  else
    log_warn "Bucket creation timed out"
  fi

  kubectl delete pod -n "$NAMESPACE" "$pod_name" --ignore-not-found >/dev/null 2>&1
  rm -f "$manifest"
}

# ----------------------------------------------------------------------------
# Port Forwarding Management
# ----------------------------------------------------------------------------
wait_for_service() {
  local service="$1"
  local timeout="${2:-30}"

  log_debug "Waiting for service: $service"

  local elapsed=0
  while [[ $elapsed -lt $timeout ]]; do
    if kubectl get svc -n "$NAMESPACE" "$service" >/dev/null 2>&1; then
      local endpoints
      endpoints=$(kubectl get endpoints -n "$NAMESPACE" "$service" -o jsonpath='{.subsets[*].addresses[*].ip}' 2>/dev/null)
      if [[ -n "$endpoints" ]]; then
        log_debug "Service $service is ready with endpoints"
        return 0
      fi
    fi
    sleep 2
    ((elapsed+=2))
  done

  log_warn "Service $service not ready after ${timeout}s"
  return 1
}

start_port_forward() {
  local resource="$1"
  local remote_port="$2"
  local local_port="${3:-$remote_port}"
  local svc_name="${resource#svc/}"

  if [[ "$resource" == svc/* ]]; then
    wait_for_service "$svc_name" 30 || {
      log_warn "Skipping port-forward for $resource (service not ready)"
      return 1
    }
  fi

  log_info "Port-forward: $resource -> localhost:$local_port"

  local log_file="/tmp/pf-${resource//\//-}-${local_port}.log"
  local pid_file="/tmp/pf-${resource//\//-}-${local_port}.pid"

  if command -v lsof >/dev/null 2>&1; then
    if lsof -ti:$local_port >/dev/null 2>&1; then
      log_debug "Port $local_port is in use (lsof), killing existing processes..."
      lsof -ti:$local_port | xargs -r kill -9 2>/dev/null || true
      sleep 1
      if lsof -ti:$local_port >/dev/null 2>&1; then
        log_warn "Port $local_port still in use after cleanup attempt"
        lsof -i:$local_port 2>/dev/null | sed 's/^/    /' || true
        return 1
      fi
    fi
  elif command -v powershell.exe >/dev/null 2>&1; then
    local pids
    pids=$(powershell.exe -NoProfile -Command "Get-NetTCPConnection -LocalPort ${local_port} -ErrorAction SilentlyContinue | Where-Object { \$_.State -eq 'Listen' } | Select-Object -ExpandProperty OwningProcess" 2>/dev/null | tr -d '\r') || true
    if [[ -n "$pids" ]]; then
      log_debug "Port $local_port is in use (PowerShell), stopping PIDs: $pids"
      for pid in $pids; do
        powershell.exe -NoProfile -Command "Stop-Process -Id ${pid} -Force" 2>/dev/null || true
      done
      sleep 1
    fi
  fi

  kubectl port-forward -n "$NAMESPACE" "$resource" "$local_port:$remote_port" > "$log_file" 2>&1 &

  local pid=$!
  echo $pid > "$pid_file"
  sleep 1.5

  if kill -0 $pid 2>/dev/null; then
    log_info "✓ Port-forward active (PID: $pid)"
    return 0
  else
    log_warn "✗ Port-forward failed - check $log_file"
    tail -5 "$log_file" 2>/dev/null | sed 's/^/    /' || true
    return 1
  fi
}

cleanup_all_port_forwards() {
  log_info "Cleaning up all existing port-forwards..."
  for pid_file in /tmp/pf-*.pid; do
    [[ -f "$pid_file" ]] || continue
    local pid
    pid=$(<"$pid_file")
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
    rm -f "$pid_file"
  done

  if command -v pkill >/dev/null 2>&1; then
    pkill -f "kubectl port-forward" 2>/dev/null || true
  fi
  sleep 2
  log_info "Port cleanup complete"
}

# ----------------------------------------------------------------------------
# Deployment Functions
# ----------------------------------------------------------------------------
ensure_namespace() {
  if kubectl get namespace "$NAMESPACE" >/dev/null 2>&1; then
    log_debug "Namespace exists: $NAMESPACE"
  else
    log_info "Creating namespace: $NAMESPACE"
    kubectl create namespace "$NAMESPACE"
  fi
}

deploy_kafka() { log_section "Deploying Kafka"; kubectl apply -f "$K8S_DIR/kafka.yaml"; wait_for_pods "app=kafka" 180; }
deploy_producer() { log_section "Deploying Producer"; kubectl apply -f "$K8S_DIR/producer-deployment.yaml"; wait_for_pods "app=kafka-producer" 120; }
deploy_minio() { log_section "Deploying MinIO"; kubectl apply -f "$K8S_DIR/minio-deployment.yaml"; wait_for_pods "app=minio" 120; }

deploy_spark() {
  log_section "Deploying Spark Streaming"
  if [[ ! -f "$K8S_DIR/spark-deployment.yaml" ]]; then
    log_error "Spark deployment file not found: $K8S_DIR/spark-deployment.yaml"
    return 1
  fi
  safe_kubectl_apply "$K8S_DIR/spark-deployment.yaml" || log_warn "Continuing after failed apply"
  wait_for_pods "app=spark-streaming" 180 || log_warn "Spark streaming pods failed to become ready"
}

setup_port_forwards() {
  log_section "Setting up Port Forwards"
  start_port_forward "svc/minio" 9000 "$LOCAL_MINIO_PORT"
  start_port_forward "svc/minio" 9001 "$LOCAL_MINIO_CONSOLE_PORT"
  start_port_forward "svc/kafka" 9094 "$LOCAL_KAFKA_PORT"
}

# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
main() {
  log_section "Pipeline Startup"
  check_command kubectl
  ensure_namespace
  cleanup_all_port_forwards
  deploy_kafka
  create_kafka_topic "$(get_kafka_pod)"
  deploy_producer
  deploy_minio
  [[ "$PORT_FORWARD" == true ]] && setup_port_forwards
  create_buckets_local || create_buckets_cluster
  deploy_spark
  log_section "Pipeline Ready"
}

main "$@"
