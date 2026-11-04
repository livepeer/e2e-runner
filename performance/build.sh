#!/usr/bin/env bash
set -euo pipefail
umask 022
cd /work
for target in node-current node-pr1 node-combined go-livepeer protocol; do
  git config --global --add safe.directory /work/sources/"$target"
done
for target in node-current node-pr1 node-combined; do
  (cd sources/"$target"; go build -mod=readonly -o /work/bin/"$target"/ ./cmd/livepeer ./cmd/livepeer-orchestrator ./cmd/livepeer-signer ./cmd/livepeer-chain)
done
(cd sources/go-livepeer; go build -mod=readonly -o /work/bin/go-livepeer ./cmd/livepeer; go build -mod=readonly -o /work/bin/livepeer_cli ./cmd/livepeer_cli)
(cd scope; go test -c -o /work/bin/scope.test .)
python3 /work/scope/performance/run.py manifest
