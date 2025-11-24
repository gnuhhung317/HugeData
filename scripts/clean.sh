#!/usr/bin/env bash
set -euo pipefail

# clean.sh — Fully clean K8s resources and local Docker images related to the data pipeline.

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
K8S_DIR="$ROOT_DIR/k8s"
NAMESPACE="hugedata"

# Images
PRODUCER_IMAGE="gnuhhung317/kafka-producer:latest"
LOCAL_PRODUCER_IMAGE="kafka-producer:dev"

SPARK_IMAGE="gnuhhung317/spark-streaming:latest"
LOCAL_SPARK_IMAGE="spark-streaming:dev"

LOCAL_MINIO_IMAGE="minio-local:dev"

h2() { echo -e "\n==> $*"; }
log_error() { echo "ERROR: $*" >&2; }
check_cmd() { command -v "$1" >/dev/null || { log_error "Required command '$1' not found"; exit 1; } }

h2 "Preflight checks"
check_cmd kubectl
check_cmd docker

# -------------------------------------------------------------------
# 1️⃣ Stop background port-forwards
# -------------------------------------------------------------------
h2 "Stopping background port-forwards (if any)"
for f in /tmp/port-forward-*.pid; do
  [ -f "$f" ] || continue
  pid=$(cat "$f" 2>/dev/null || echo "")
  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    echo "Killing PID $pid"
    kill "$pid" || true
  fi
  rm -f "$f"
done

# -------------------------------------------------------------------
# 2️⃣ Delete Kubernetes resources
# -------------------------------------------------------------------
h2 "Checking if namespace '${NAMESPACE}' exists"
if ! kubectl get ns "$NAMESPACE" >/dev/null 2>&1; then
  echo "Namespace does not exist — skipping K8s delete"
else
  h2 "Deleting Kubernetes deployments"
  kubectl delete -f "$K8S_DIR/producer-deployment.yaml" -n ${NAMESPACE} --ignore-not-found
  kubectl delete -f "$K8S_DIR/spark-deployment.yaml" -n ${NAMESPACE} --ignore-not-found
  kubectl delete -f "$K8S_DIR/minio-deployment.yaml" -n ${NAMESPACE} --ignore-not-found
  kubectl delete -f "$K8S_DIR/kafka.yaml" -n ${NAMESPACE} --ignore-not-found

  h2 "Waiting for pods to terminate"
  set +e
  kubectl get pods -n ${NAMESPACE} 2>/dev/null | awk 'NR>1 {print $1}' | \
    xargs -r -I{} kubectl wait --for=delete pod/{} -n ${NAMESPACE} --timeout=45s

  remaining=$(kubectl get pods -n ${NAMESPACE} --no-headers 2>/dev/null | wc -l)
  if [ "$remaining" -gt 0 ]; then
    echo "Force deleting remaining pods..."
    kubectl delete pod --all -n ${NAMESPACE} --force --grace-period=0
  fi
  set -e

  h2 "Deleting namespace"
  kubectl delete -f "$K8S_DIR/namespace.yaml" --ignore-not-found || true
fi

# -------------------------------------------------------------------
# 3️⃣ Remove local Docker images
# -------------------------------------------------------------------
h2 "Removing local Docker images"
docker rmi -f $LOCAL_PRODUCER_IMAGE 2>/dev/null || true
docker rmi -f $LOCAL_SPARK_IMAGE 2>/dev/null || true
docker rmi -f $LOCAL_MINIO_IMAGE 2>/dev/null || true

h2 "Clean complete"
exit 0