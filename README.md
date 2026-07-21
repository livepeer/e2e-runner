# Live Runner E2E Scope

This module contains a small Go e2e harness for Livepeer LiveRunner behavior across:

- Livepeer orchestrator, started as the real binary in offchain mode.
- `python-runner`, exercised through its registration client.
- `golang-runner`, exercised through its public Go API where features are comparable.

## Requirements

- A local Livepeer binary exists and is executable.
- A local `python-runner` checkout can run `uv run python`.
- A local `golang-runner` checkout is available through the `go.mod` replace directive.

Path overrides:

- `GO_LIVEPEER_DIR`: directory containing the Livepeer checkout.
- `LIVEPEER_BINARY`: path to the Livepeer binary.
- `PYTHON_RUNNER_DIR`: directory containing the `python-runner` checkout.
- `LIVEPEER_ONCHAIN_ORCH_URL`: optional on-chain orchestrator live-runner URL for price validation tests.
- `LIVEPEER_ONCHAIN_ORCH_SECRET`: optional bootstrap secret for the on-chain orchestrator.
- `LIVEPEER_ONCHAIN_CLI_URL`: optional CLI URL for the on-chain orchestrator; required only for static-runner price validation.

## Run

```sh
env GOCACHE=/tmp/e2e-scope-gocache GOMODCACHE=/tmp/e2e-scope-gomodcache go test ./...
```

The tests bind localhost ports and start child processes, so restricted sandboxes may need local network/process permissions.

The on-chain price validation test is skipped unless it is pointed at an already-running on-chain orchestrator:

```sh
env \
  LIVEPEER_ONCHAIN_ORCH_URL=http://127.0.0.1:8935 \
  LIVEPEER_ONCHAIN_ORCH_SECRET=... \
  LIVEPEER_ONCHAIN_CLI_URL=http://127.0.0.1:7935 \
  GOCACHE=/tmp/e2e-scope-gocache \
  GOMODCACHE=/tmp/e2e-scope-gomodcache \
  go test -run TestOnchainPriceInfoRequired ./...
```

## Coverage

- Shared runtime implementation tests for `python-runner` and `golang-runner` covering register, application metadata discovery, capacity reporting, busy rejection, release, unregister, and post-unregister routing for persistent and single-shot modes.
- LiveRunner server surface coverage for heartbeat authorization lifecycle, proxied session headers, session token isolation, trickle channel token authorization, and static runner registration with label routing and health-based availability.
- Shared session-control API tests for `python-runner` and `golang-runner` covering proxied trickle channel create/delete, public session proxy creation/forwarding, and session stop.
- Optional on-chain price validation for python-runner runtime registration, golang-runner runtime registration, and static runner registration.

The default suite intentionally does not start a chain/devnet. On-chain coverage runs against an already-running on-chain orchestrator via the optional environment variables above.
