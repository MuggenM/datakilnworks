#!/usr/bin/env bash
# Cluster smoke test of the Helm chart on a real (kind) Kubernetes cluster:
#   1. install the chart with the image loaded into the cluster; the init container, studio and a compute node must become ready and the volumes bound;
#   2. through the Service (port-forward): sign in, the forced first password change, SQL on the studio AND on the compute node pod, write a table
#      (the HTTP checks are ci/smoke_api.py);
#   3. delete the studio pod: the CHANGED password and the table must survive on the volumes;
#   4. helm upgrade with a changed setting: still healthy, the generated compute token is kept;
#   5. a second release with a broken bootstrap admin must stop in the init container with a readable message (Init:Error), not crash-loop the studio;
#   6. uninstall: the data volumes are kept on purpose.
# Needs: docker, kind (unless USE_CURRENT_CONTEXT=1), kubectl, helm, python3. Environment:
#   IMAGE                image to test (default localspark-lakehouse-notebook); tagged dkw-ci:smoke and loaded into the cluster
#   KEEP_CLUSTER=1       keep the kind cluster $CLUSTER afterwards
#   USE_CURRENT_CONTEXT=1  use the current kubectl context and load the image with $IMAGE_LOADER instead of `kind load` (the loader gets the image tag)
set -uo pipefail
cd "$(dirname "$0")/.."
IMAGE="${IMAGE:-localspark-lakehouse-notebook}"
CLUSTER="${CLUSTER:-dkw-smoke}"
NS=dkw
CHART=deploy/helm/datakilnworks
ADMIN_HASH='pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d'      # adminpassword123, the throwaway bootstrap password of every test here
FAILED=0
PF_PID=""
step() { echo; echo "=== $*"; }
ok()   { echo "  [PASS] $*"; }
bad()  { echo "  [FAIL] $*"; FAILED=1; }

dump() {
  echo "----- diagnostics -----"
  kubectl get pods,pvc,svc -A -o wide 2>&1 | tail -n 40
  kubectl describe pods -n "$NS" 2>&1 | tail -n 100
  for c in init studio; do echo "--- studio/$c"; kubectl logs -n "$NS" deploy/dkw-studio -c "$c" --tail=80 2>&1; done
  echo "--- compute"; kubectl logs -n "$NS" deploy/dkw-compute-node-01 --tail=60 2>&1
}
stop_pf() { [ -n "$PF_PID" ] && kill "$PF_PID" 2>/dev/null; PF_PID=""; }
start_pf() {
  stop_pf
  kubectl port-forward -n "$NS" svc/dkw 18891:80 >/tmp/dkw-pf.log 2>&1 &
  PF_PID=$!
  for _ in $(seq 1 60); do curl -fsS http://127.0.0.1:18891/healthz >/dev/null 2>&1 && return 0; sleep 1; done
  return 1
}
cleanup() {
  stop_pf
  [ "$FAILED" -ne 0 ] && dump
  if [ -z "${USE_CURRENT_CONTEXT:-}" ] && [ -z "${KEEP_CLUSTER:-}" ]; then kind delete cluster --name "$CLUSTER" >/dev/null 2>&1; fi
}
trap cleanup EXIT

step "Cluster and image"
docker tag "$IMAGE" dkw-ci:smoke || { echo "image $IMAGE not found: build it first (docker build -t $IMAGE .)"; exit 2; }
if [ -z "${USE_CURRENT_CONTEXT:-}" ]; then
  kind delete cluster --name "$CLUSTER" >/dev/null 2>&1
  kind create cluster --name "$CLUSTER" --config ci/kind-config.yaml --wait 180s || { bad "could not create the cluster"; exit 1; }
  kind load docker-image dkw-ci:smoke --name "$CLUSTER" || bad "could not load the image"
else
  ${IMAGE_LOADER:?set IMAGE_LOADER to a command that loads the image into your cluster} dkw-ci:smoke || bad "could not load the image"
fi
kubectl get nodes && ok "the cluster is up and the image is loaded"

step "helm lint and install"
helm lint "$CHART" -f ci/kind-values.yaml --set-string secrets.initAdminPasswordHash="$ADMIN_HASH" >/dev/null && ok "helm lint" || bad "helm lint"
kubectl create namespace "$NS" >/dev/null 2>&1
helm upgrade --install dkw "$CHART" -n "$NS" -f ci/kind-values.yaml --set-string secrets.initAdminPasswordHash="$ADMIN_HASH" --wait --timeout 8m >/dev/null && ok "helm install (waited for readiness)" || bad "helm install did not become ready"
kubectl rollout status deploy/dkw-studio -n "$NS" --timeout=300s >/dev/null && ok "the studio deployment is ready" || bad "the studio is not ready"
kubectl rollout status deploy/dkw-compute-node-01 -n "$NS" --timeout=300s >/dev/null && ok "the compute node is ready" || bad "the compute node is not ready"
PHASES="$(kubectl get pvc -n "$NS" -o jsonpath='{.items[*].status.phase}')"
[ "$PHASES" = "Bound Bound Bound" ] && ok "the three volumes are bound" || bad "volumes: $PHASES"
INIT_LOG="$(kubectl logs -n "$NS" deploy/dkw-studio -c init 2>&1)"
echo "$INIT_LOG" | tail -n 4 | sed 's/^/    /'
echo "$INIT_LOG" | grep -q "init finished: ready" && ok "the init container finished: ready" || bad "the init container did not report ready"

step "Sign in, SQL on studio and compute node, write a table (through the Service)"
start_pf && python3 ci/smoke_api.py first http://127.0.0.1:18891 || FAILED=1

step "Data survives a studio pod restart"
stop_pf
kubectl delete pod -n "$NS" -l app.kubernetes.io/name=datakilnworks-studio --wait=true >/dev/null
kubectl rollout status deploy/dkw-studio -n "$NS" --timeout=300s >/dev/null && ok "the studio came back" || bad "the studio did not come back"
start_pf && python3 ci/smoke_api.py again http://127.0.0.1:18891 || FAILED=1
stop_pf

step "helm upgrade"
TOKEN_BEFORE="$(kubectl get secret -n "$NS" dkw-secrets -o jsonpath='{.data.COMPUTE_TOKEN}' 2>/dev/null)"
helm upgrade dkw "$CHART" -n "$NS" -f ci/kind-values.yaml --set-string secrets.initAdminPasswordHash="$ADMIN_HASH" --set studio.env.SMOKE_TEST_MARKER=upgraded --wait --timeout 8m >/dev/null && ok "the upgrade completed" || bad "helm upgrade failed"
kubectl get deploy/dkw-studio -n "$NS" -o jsonpath='{.spec.template.spec.containers[0].env[*].name}' | grep -q SMOKE_TEST_MARKER && ok "the new setting reached the pod" || bad "the upgrade did not change the pod"
kubectl rollout status deploy/dkw-studio -n "$NS" --timeout=300s >/dev/null && ok "the upgraded studio is ready" || bad "the upgraded studio is not ready"
TOKEN_AFTER="$(kubectl get secret -n "$NS" dkw-secrets -o jsonpath='{.data.COMPUTE_TOKEN}' 2>/dev/null)"
[ -n "$TOKEN_BEFORE" ] && [ "$TOKEN_BEFORE" = "$TOKEN_AFTER" ] && ok "the generated compute token was kept by the upgrade" || bad "the compute token changed or is missing"
start_pf && python3 ci/smoke_api.py again http://127.0.0.1:18891 || FAILED=1
stop_pf

step "A misconfigured install fails readably in the init container"
kubectl create namespace dkw-bad >/dev/null 2>&1
helm upgrade --install bad "$CHART" -n dkw-bad -f ci/kind-values.yaml --set fullnameOverride=bad --set-string secrets.initAdminPasswordHash="not-a-hash" --set-json 'computeNodes=[]' >/dev/null 2>&1
ST=""
for _ in $(seq 1 60); do
  ST="$(kubectl get pods -n dkw-bad -o jsonpath='{.items[*].status.initContainerStatuses[*].state.terminated.reason}{.items[*].status.initContainerStatuses[*].lastState.terminated.reason}' 2>/dev/null)"
  echo "$ST" | grep -q Error && break; sleep 3
done
echo "$ST" | grep -q Error && ok "the pod stops in the init container instead of starting the studio" || bad "the bad install did not fail in the init container (state: '$ST')"
BAD_LOG="$(kubectl logs -n dkw-bad -l app.kubernetes.io/name=datakilnworks-studio -c init --tail=20 2>&1)"
echo "$BAD_LOG" | tail -n 3 | cut -c1-200 | sed 's/^/    /'
echo "$BAD_LOG" | grep -q "INIT_ADMIN_PASSWORD_HASH" && ok "its log names the problem (INIT_ADMIN_PASSWORD_HASH)" || bad "the init log does not explain the failure"
[ "$(kubectl get pods -n dkw-bad -o jsonpath='{.items[*].status.containerStatuses[*].ready}' 2>/dev/null | grep -c true)" = "0" ] && ok "the studio container never became ready" || bad "the studio container is running despite the failed init"
helm uninstall bad -n dkw-bad >/dev/null 2>&1

step "Uninstall keeps the data volumes"
helm uninstall dkw -n "$NS" >/dev/null && ok "uninstalled" || bad "uninstall failed"
[ "$(kubectl get pvc -n "$NS" --no-headers 2>/dev/null | wc -l)" -ge 3 ] && ok "the volumes were kept (helm.sh/resource-policy: keep)" || bad "the volumes are gone"

echo
if [ "$FAILED" -eq 0 ]; then echo "ALL PASS"; else echo "FAILED"; exit 1; fi
