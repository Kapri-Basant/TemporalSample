#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NAMESPACE="temporal-demo"
cd "$ROOT_DIR"

# Stop early when a needed tool is missing.
require() {
  command -v "$1" >/dev/null 2>&1 || { echo "Missing tool: $1" >&2; exit 1; }
}
require docker
require kubectl
require curl
require python3

docker info >/dev/null
CLUSTER_CONTEXT="$(kubectl config current-context)"
if [[ "$CLUSTER_CONTEXT" != "docker-desktop" ]]; then
  echo "This script uses Docker Desktop's local image store. Select its Kubernetes context first:" >&2
  echo "  kubectl config use-context docker-desktop" >&2
  exit 1
fi
kubectl get nodes --request-timeout=10s >/dev/null

TEMP_DIR="$(mktemp -d)"
API_FORWARD_PID=""
UI_FORWARD_PID=""

# Close this run's local forwards and remove its temporary files.
cleanup() {
  local pid label pid_file
  for label in api ui; do
    if [[ "$label" == api ]]; then pid="$API_FORWARD_PID"; else pid="$UI_FORWARD_PID"; fi
    if [[ -n "$pid" ]]; then
      kill "$pid" 2>/dev/null || true
      pid_file="/tmp/temporal-demo-$label.pid"
      if [[ -f "$pid_file" && "$(cat "$pid_file")" == "$pid" ]]; then
        rm -f "$pid_file"
      fi
    fi
  done
  rm -rf "$TEMP_DIR"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

APP_IMAGE="temporal-demo:local-$(date -u +%Y%m%d%H%M%S)-$$"
echo "Building Java 25 image: $APP_IMAGE"
docker build -t "$APP_IMAGE" .
kubectl apply -f k8s/namespace.yaml

# Preserve a running database that does not yet use a persistent volume.
apply_temporal_resources() {
  kubectl -n "$NAMESPACE" get deployment postgres --ignore-not-found -o json >"$TEMP_DIR/postgres.json"
  python3 - "$TEMP_DIR/postgres.json" "$ROOT_DIR/k8s/temporal-dev.yaml" "$TEMP_DIR/temporal.yaml" <<'PY'
import json, pathlib, re, sys
raw = pathlib.Path(sys.argv[1]).read_text().strip()
deployment = json.loads(raw) if raw else None
persistent = False
if deployment:
    spec = deployment['spec']['template']['spec']
    claims = {v['name'] for v in spec.get('volumes', []) if 'persistentVolumeClaim' in v}
    persistent = any(m['name'] in claims and m['mountPath'] == '/var/lib/postgresql/data'
                     for c in spec['containers'] if c['name'] == 'postgres'
                     for m in c.get('volumeMounts', []))
text = pathlib.Path(sys.argv[2]).read_text()
if deployment and not persistent:
    documents = re.split(r'(?m)^---\s*$', text)
    text = '\n---\n'.join(d for d in documents if not (
        re.search(r'(?m)^kind: Deployment\s*$', d) and
        re.search(r'(?m)^  name: postgres\s*$', d)))
    print('Existing PostgreSQL uses container storage. Keeping its Deployment unchanged.', file=sys.stderr)
    print('Its current data is still lost if that database pod is replaced.', file=sys.stderr)
    print('A PVC is created for migration, but existing data is NOT copied automatically.', file=sys.stderr)
    print('Read HELP.md before moving the database. Test recovery by crashing only the worker.', file=sys.stderr)
pathlib.Path(sys.argv[3]).write_text(text)
PY
  kubectl apply -f "$TEMP_DIR/temporal.yaml"
}

apply_temporal_resources
# A new tag makes Kubernetes use this build instead of a cached latest image.
python3 - "$APP_IMAGE" "$ROOT_DIR/k8s/app.yaml" "$TEMP_DIR/app.yaml" <<'PY'
import pathlib, sys
image, source, target = sys.argv[1:]
text = pathlib.Path(source).read_text()
old = 'image: temporal-demo:latest'
if text.count(old) != 2:
    raise SystemExit('Expected one API image and one worker image in k8s/app.yaml')
pathlib.Path(target).write_text(text.replace(old, 'image: ' + image))
PY
kubectl apply -f "$TEMP_DIR/app.yaml"

for deployment in postgres temporal temporal-ui temporal-demo-api temporal-demo-worker; do
  kubectl -n "$NAMESPACE" rollout status "deployment/$deployment" --timeout=240s
done

# Stop only our old port-forward, not an unrelated process with a reused PID.
stop_port_forward() {
  local label="$1" pid_file="/tmp/temporal-demo-$1.pid" pid command
  if [[ -f "$pid_file" ]]; then
    pid="$(cat "$pid_file")"
    if [[ "$pid" =~ ^[0-9]+$ ]]; then
      command="$(ps -p "$pid" -o command= 2>/dev/null || true)"
      if [[ "$command" == *"kubectl -n $NAMESPACE port-forward svc/temporal-demo-"* ||
            "$command" == *"kubectl -n $NAMESPACE port-forward svc/temporal-ui"* ]]; then
        kill "$pid" 2>/dev/null || true
      fi
    fi
    rm -f "$pid_file"
  fi
}
stop_port_forward api
stop_port_forward ui

kubectl -n "$NAMESPACE" port-forward svc/temporal-demo-api 8080:8080 \
  >/tmp/temporal-demo-api.log 2>&1 </dev/null &
API_FORWARD_PID=$!
echo "$API_FORWARD_PID" >/tmp/temporal-demo-api.pid
kubectl -n "$NAMESPACE" port-forward svc/temporal-ui 8233:8080 \
  >/tmp/temporal-demo-ui.log 2>&1 </dev/null &
UI_FORWARD_PID=$!
echo "$UI_FORWARD_PID" >/tmp/temporal-demo-ui.pid

# Report success only when this port-forward stays alive and its page responds.
wait_for_page() {
  local url="$1" pid="$2" label="$3"
  for _ in $(seq 1 30); do
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "$label port-forward stopped. Read /tmp/temporal-demo-$label.log" >&2
      return 1
    fi
    if curl --connect-timeout 1 --max-time 2 -fsS "$url" >/dev/null 2>&1; then
      sleep 1
      kill -0 "$pid" 2>/dev/null && return 0
      return 1
    fi
    sleep 1
  done
  echo "$label did not become ready at $url" >&2
  return 1
}
if ! wait_for_page http://localhost:8080/temporal-live-demo.html "$API_FORWARD_PID" api ||
   ! wait_for_page http://localhost:8233/ "$UI_FORWARD_PID" ui; then
  stop_port_forward api
  stop_port_forward ui
  exit 1
fi

echo "Workflow operations: http://localhost:8080/temporal-live-demo.html"
echo "Concepts:            http://localhost:8080/temporal-what-is.html"
echo "Temporal UI:         http://localhost:8233/"
echo "Keep this terminal open. Ctrl+C stops local access; pods keep running."

# Keep local access alive and report a failed forward instead of silently stopping.
while kill -0 "$API_FORWARD_PID" 2>/dev/null && kill -0 "$UI_FORWARD_PID" 2>/dev/null; do
  sleep 2
done
echo "A port-forward stopped. Read /tmp/temporal-demo-api.log and /tmp/temporal-demo-ui.log." >&2
exit 1
