#!/usr/bin/env bash
set -euo pipefail
mkdir -p "$E2E_RUN_ROOT/bin"
cd "$E2E_NODE_DIR"
go build -mod=readonly -o "$E2E_RUN_ROOT/bin/node/" ./cmd/livepeer ./cmd/livepeer-orchestrator ./cmd/livepeer-signer ./cmd/livepeer-chain
cd "$E2E_CLEARINGHOUSE_DIR"
go build -mod=readonly -o "$E2E_RUN_ROOT/bin/clearinghouse" ./cmd/clearinghouse
cd "$GO_LIVEPEER_DIR"
go build -mod=readonly -o "$E2E_RUN_ROOT/bin/livepeer" ./cmd/livepeer
go build -mod=readonly -o "$E2E_RUN_ROOT/bin/livepeer_cli" ./cmd/livepeer_cli
cd "$E2E_RUN_ROOT/scope"
if [[ -n ${E2E_GOLANG_RUNNER_DIR:-} ]]; then
  go mod edit -replace "github.com/livepeer/golang-runner=$E2E_GOLANG_RUNNER_DIR"
fi
go build -o "$E2E_RUN_ROOT/bin/e2e-runner" ./cmd/e2e-runner
go build -o "$E2E_RUN_ROOT/bin/e2e-bridge" ./cmd/e2e-bridge
go build -o "$E2E_RUN_ROOT/bin/e2e-kafka-proxy" ./cmd/e2e-kafka-proxy
go test -c -o "$E2E_RUN_ROOT/bin/scope.test" .
cd "$PYTHON_RUNNER_DIR"
uv sync --frozen --extra examples
