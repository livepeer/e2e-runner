#!/usr/bin/env bash
# The caller's local gcloud identity creates and deletes a disposable VM.
set -euo pipefail
cd "$(dirname "$0")/.."
project=${E2E_GCP_PROJECT:-$(gcloud config get-value project 2>/dev/null)}
zone=${E2E_GCP_ZONE:-us-west1-b}
vm="e2e-compose-$(date +%Y%m%d-%H%M%S)-$$"
result="$(pwd)/artifacts/$vm"
mkdir -p "$result"
export COMPOSE_PROJECT_NAME="$vm-input"
python3 docker/prepare.py "$result/input"
inputs=(scope go-livepeer node clearinghouse python-runner)
if [[ -d "$result/input/lpms" ]]; then inputs+=(lpms); fi
if [[ -d "$result/input/golang-runner" ]]; then inputs+=(golang-runner); fi
# macOS extended attributes otherwise become ._ files inside Linux sources.
COPYFILE_DISABLE=1 tar -czf "$result/input.tar.gz" -C "$result/input" "${inputs[@]}"
created=false
cleanup() {
  code=$?
  trap - EXIT INT TERM
  if $created; then
    # Preserve only logs/provenance, never temporary wallets or credentials.
    gcloud compute ssh "$vm" --project="$project" --zone="$zone" --quiet \
      --command='cd /tmp/e2e-input/scope; tar -czf /tmp/e2e-results.tar.gz --ignore-failed-read artifacts/e2e-*/artifacts /tmp/e2e-validation.log' >/dev/null 2>&1 || true
    gcloud compute scp "$vm:/tmp/e2e-results.tar.gz" "$result/results.tar.gz" --project="$project" --zone="$zone" --quiet || true
    if ! gcloud compute instances delete "$vm" --project="$project" --zone="$zone" --quiet; then
      echo "VM deletion failed: $vm in $project/$zone; automatic deletion is limited to 4h." >&2
      code=1
    fi
  fi
  rm -rf "$result/input" "$result/input.tar.gz"
  echo "GCP validation artifacts: $result"
  exit "$code"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
gcloud compute instances create "$vm" --project="$project" --zone="$zone" \
  --machine-type="${E2E_GCP_MACHINE_TYPE:-e2-standard-8}" \
  --image-family=ubuntu-2404-lts-amd64 --image-project=ubuntu-os-cloud \
  --boot-disk-size=100GB --no-service-account --no-scopes \
  --max-run-duration=4h --instance-termination-action=DELETE --labels=purpose=e2e-compose --quiet
created=true
# SSH can become ready after Compute Engine reports RUNNING.
for attempt in $(seq 1 30); do
  if gcloud compute ssh "$vm" --project="$project" --zone="$zone" --quiet --command=true >/dev/null 2>&1; then break; fi
  if [[ $attempt == 30 ]]; then echo 'VM SSH readiness timed out' >&2; exit 1; fi
  sleep 5
done
gcloud compute ssh "$vm" --project="$project" --zone="$zone" --quiet \
  --command='sudo apt-get update -qq && sudo apt-get install -y -qq docker.io docker-compose-v2 python3 && sudo usermod -aG docker "$USER"'
gcloud compute scp "$result/input.tar.gz" "$vm:/tmp/e2e-input.tar.gz" --project="$project" --zone="$zone" --quiet
# Transfer test selection as a shell-quoted argv, not executable interpolation.
test_args=""
if (( $# )); then printf -v test_args '%q ' "$@"; fi
gcloud compute ssh "$vm" --project="$project" --zone="$zone" --quiet --command="
set -euo pipefail
mkdir -p /tmp/e2e-input
tar -xzf /tmp/e2e-input.tar.gz -C /tmp/e2e-input
cd /tmp/e2e-input/scope
export GO_LIVEPEER_DIR=/tmp/e2e-input/go-livepeer E2E_NODE_DIR=/tmp/e2e-input/node
export E2E_CLEARINGHOUSE_DIR=/tmp/e2e-input/clearinghouse PYTHON_RUNNER_DIR=/tmp/e2e-input/python-runner
if test -d /tmp/e2e-input/lpms; then export E2E_LPMS_DIR=/tmp/e2e-input/lpms; fi
if test -d /tmp/e2e-input/golang-runner; then export E2E_GOLANG_RUNNER_DIR=/tmp/e2e-input/golang-runner; fi
bash docker/run.sh $test_args 2>&1 | tee /tmp/e2e-validation.log
" 2>&1 | tee "$result/gcloud.log"
