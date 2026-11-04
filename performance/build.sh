#!/usr/bin/env bash
set -euo pipefail
umask 022
root=${PERF_ROOT:-/work}
cd "$root"
for target in $(python3 scope/performance/targets.py names) protocol; do
  git config --global --add safe.directory "$root/sources/$target"
done
for target in $(python3 scope/performance/targets.py names --backend node); do
  (cd sources/"$target"; go build -mod=readonly -o "$root/bin/$target/" ./cmd/livepeer ./cmd/livepeer-orchestrator ./cmd/livepeer-signer ./cmd/livepeer-chain)
done
(cd sources/go-livepeer; go build -mod=readonly -o "$root/bin/go-livepeer" ./cmd/livepeer; go build -mod=readonly -o "$root/bin/livepeer_cli" ./cmd/livepeer_cli)
(cd scope; go test -c -o "$root/bin/scope.test" .)
python3 "$root/scope/performance/run.py" manifest
