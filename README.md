# Live Runner parity and payment E2E harness

This repository tests Live Runner registration, application routes, payments, and Clearinghouse Batteries against go-livepeer and Livepeer Node. It runs the implementations as child processes or Docker containers, using the Python and Go runner SDKs and real payment signers.

## Running

Copy [test.env](test.env) to the ignored `test.local.env`, set the source checkout paths and any chain settings needed by your selected suites, and select it with `E2E_ENV_FILE`. Resource locations have no machine-specific defaults. See [configuration and prerequisites](#configuration-and-prerequisites) before running the suites.

```sh
cp test.env test.local.env
# Edit test.local.env before running the suites.
export E2E_ENV_FILE=test.local.env
make test-offchain
```

Choose a target for the coverage you need:

| Target | Coverage |
|---|---|
| `make test-offchain` | Runner registration, discovery, session controls, proxies, and media; no chain required |
| `make test-onchain` | Payments, streaming uploads, protocol CLI, recovery, and oracle cases |
| `make test-accounting` | Clearinghouse management, signing, accounting, and settlement; chain required except for management and Kafka proxy checks |
| `make test` | All suites and command-package checks |

Missing test prerequisites fail dependent tests with `INFRASTRUCTURE`; independent groups continue. On-chain coverage needs a disposable chain with the required capabilities, and runtime oracle coverage needs [compatible timeout settings](#oracle-timing).

Direct runs load the same configuration:

```sh
go test -count=1 -timeout 90m -v ./...
go test -race -count=1 -timeout 90m -v ./...
E2E_ORCHESTRATORS=node E2E_SIGNERS=node \
  go test -count=1 -timeout 90m -run '^TestOnchainPayments$' -v .
```

The Go runner SDK version is pinned in [go.mod](go.mod). The Python integration uses the checkout selected by `PYTHON_RUNNER_DIR`; streaming coverage requires `StreamPayload` and the `on_payment_session` callback. The harness records source revisions and working-tree fingerprints in its [run artifacts](#run-artifacts).

Run chain-changing suites serially; do not launch payment and accounting suites concurrently against the same chain. Go's slash-separated `-run` expressions select an orchestrator, signer, wallet mode, and case. For example, `-run '^TestOnchainPayments/go-livepeer/go-livepeer/split-wallet/challenge_and_admission$'` selects one payment case. Use `-run '^TestOnchainPayments/[^/]+/[^/]+/same-wallet$'` or `-run '^TestOnchainPayments/[^/]+/[^/]+/split-wallet$'` to select a wallet mode across all pairings.

## Docker Compose mode

Configure the same source checkout paths in `test.local.env`, then run:

```sh
# On a Linux Docker host:
E2E_ENV_FILE=test.local.env make test-compose
# On GCP, using the active gcloud identity:
E2E_ENV_FILE=test.local.env make test-compose-gcp
# Select a suite; these are compiled-test-binary flags:
E2E_ENV_FILE=test.local.env make test-compose-gcp \
  E2E_COMPOSE_TEST_ARGS="-test.run=^TestClearinghouseManagement$"
```

Compose requires a Linux host with Docker, Compose v2, Python 3, and all four source checkouts, including when selecting a subset of tests. It snapshots their working trees, builds the binaries and Python environment, and starts a fresh `livepeer/geth-with-livepeer-protocol:confluence` devnet. Controller and chain ID are discovered from that chain; external RPC, feed, Controller, and funding settings are ignored. `E2E_LPMS_DIR` supplies an optional local lpms replacement; a sibling checkout is detected automatically. Other local module replacements must be supplied explicitly.

Components and SDK runners execute in separate containers. Callback assertions remain in the harness behind TLS adapters. HTTPS Quick Tunnels advertise component endpoints; gateways reuse hostnames between fixtures. Provisioning retries HTTP 429 up to five attempts, honoring valid `Retry-After` headers with exponential backoff as the minimum and fallback. Kafka uses a WSS bridge with SCRAM authentication. Only local origin/adapter connections use plaintext. TLS verification stays enabled and application ports remain private to loopback.

Quick Tunnels use one hostname per endpoint, so Compose tests path templates (`https://…trycloudflare.com/e2e-proxy/{proxy}`); local mode tests hostname templates. The tunnel must preserve the selected tests' HTTP semantics. Current streaming validation exposes premature Cloudflare `100 Continue`; the upload assertions remain enabled. See [Quick Tunnel limits](https://developers.cloudflare.com/tunnel/get-started/quick-tunnels/).

The default devnet does not guarantee finalized tags, canonical Ethereum headers, or mining/snapshot controls. Select a compatible protocol-deployed chain with `E2E_GETH_IMAGE` for the dependent suites; missing capabilities fail infrastructure prerequisites.

`make test-compose-gcp` additionally requires an authenticated `gcloud` CLI with Compute Engine access. It creates an Ubuntu VM without a service account, installs Docker, runs Compose, collects results, and deletes the VM and boot disk. The VM also has a four-hour automatic deletion deadline. Override `E2E_GCP_PROJECT`, `E2E_GCP_ZONE` (default `us-west1-b`), and `E2E_GCP_MACHINE_TYPE` (default `e2-standard-8`) as needed. Failures return a nonzero status; failed deletion reports the VM identity. Cloud charges apply while it runs.

Local results are in `artifacts/e2e-*/artifacts`; GCP results are in `artifacts/e2e-compose-*/results.tar.gz`. They include component/tunnel logs, devnet identity, endpoint mappings, source provenance, and binary metadata. Temporary wallets and credentials are removed; build/module caches and runtime images are reusable, while chain state is fresh.

## Configuration and prerequisites

Local process mode requires Go compatible with [go.mod](go.mod) and the configured Node and Clearinghouse modules; `uv`; the Python runner's locked environment and media dependencies; and go-livepeer binaries built with Live Runner support, including `livepeer_cli`. Node's dispatcher, orchestrator, signer, and chain executables are built from the configured checkout unless prebuilt binaries are supplied. Compose builds these dependencies inside its runtime image.

`test.env` contains portable test defaults and comments for required resource settings. Existing process variables take precedence over values loaded from the file. `E2E_ENV_FILE` selects another configuration file; a missing file is an error. Store environment-specific values in the ignored `test.local.env` or export them in the process environment. Use disposable credentials for these tests.

| Setting | Purpose/default |
|---|---|
| `E2E_RPC_URL` | Required for on-chain suites in local process mode: the disposable chain's RPC endpoint |
| `E2E_ETH_CONTROLLER` | Required for on-chain suites in local process mode: the Controller address deployed on that chain |
| `E2E_EXPECTED_CHAIN_ID` | Required for on-chain suites in local process mode: its decimal chain ID; mismatches fail before funding |
| `E2E_ORCHESTRATORS`, `E2E_SIGNERS` | Comma-separated `go-livepeer,node`; defaults exercise both |
| `GO_LIVEPEER_DIR`, `LIVEPEER_BINARY` | Explicit go-livepeer checkout path; use its `livepeer` binary or supply a binary path directly |
| `E2E_GO_LIVEPEER_CLI_BINARY` | Actual interactive CLI; supply its path or use `livepeer_cli` in the configured `GO_LIVEPEER_DIR` |
| `E2E_NODE_DIR`, `E2E_NODE_BINARY` | Explicit Node checkout path; build from it unless supplying a binary path |
| `E2E_CLEARINGHOUSE_DIR`, `E2E_CLEARINGHOUSE_BINARY` | Explicit Clearinghouse checkout path; build from it unless supplying a binary path |
| `PYTHON_RUNNER_DIR` | Explicit Python runner checkout path, required for Python SDK and media cases |
| `E2E_GOLANG_RUNNER_DIR` | Optional Go runner checkout override for the Make launcher and Compose; otherwise use the module pinned in `go.mod` |
| `E2E_GO_BINARY` | Toolchain used to build Node/clearinghouse and inspect build provenance; default `go` |
| `E2E_FUNDER_ADDRESS`, `E2E_FUNDER_KEY_FILE`, `E2E_LPT_FUNDER_ADDRESS` | Disposable funding accounts or an owner-only private-key file; by default, use the first RPC account for ETH and the account with the largest LPT balance for stake funding |
| `E2E_ACCOUNT_ETH`, `E2E_PAYER_DEPOSIT_ETH`, `E2E_PAYER_RESERVE_ETH` | Fresh-account ETH and payer escrow amounts |
| `E2E_REDEEMER_ETH` | ETH-only gas allowance for each fresh split-wallet hot account; default `0.01` ETH |
| `E2E_BOND_LPT`, `E2E_BOND_MARGIN_LPT` | Minimum self-bond; when the pool is full, margin above its smallest stake |
| `E2E_AUTO_FAUCET` | `true`; request missing stake funding through go-livepeer's CLI |
| `E2E_FAUCET_TIMEOUT`, `E2E_FAUCET_MAX_REQUESTS` | Funding time/claim limits; defaults `45m` and `500` |
| `E2E_CLI_TIMEOUT` | Deadline for one CLI invocation; default `2m` |
| `E2E_PRICE_FEED_ADDRESS`, `E2E_ETH_USD` | Existing AggregatorV3 feed, or deploy a test-only feed at the configured ETH/USD observation |
| `E2E_WEI_PER_USD` | Node's fixed conversion rate; keep consistent with the feed for cross-implementation conversion comparisons |
| `E2E_ORACLE_MAX_AGE` | Node's maximum oracle observation age |
| `E2E_ORACLE_TEST_MAX_AGE` | Short age limit for independent oracle cache-expiry fixtures; default `45s` |
| `E2E_ORACLE_POLL_INTERVAL` | Expected runtime polling interval of the tested builds; default `1h`. Set the oracle and suite timeouts to cover it |
| `E2E_ORACLE_TIMEOUT` | Oracle assertion deadline; default `90s`. Runtime refresh cases require it to exceed the polling interval |
| `E2E_SIGNER_MAX_HOURLY_USD`, `E2E_SIGNER_MAX_FIXED_USD`, `E2E_GO_SIGNER_MAX_PRICE_WEI` | Configured signer price ceilings |
| `E2E_GO_BASE_PRICE_WEI` | go-livepeer's required base price; Live Runner prices are supplied separately |
| `E2E_TICKET_FACE_VALUE`, `E2E_TICKET_WIN_PROB`, `E2E_TICKET_EV` | Node face/probability and go-livepeer ticket EV fixtures |
| `E2E_KAFKA_TOPIC`, `E2E_CHAIN_CONFIRMATIONS`, `E2E_CHAIN_POLL_INTERVAL` | Accounting topic and listener parameters |
| `E2E_OUTBOX_MAX_BYTES` | Node signer outbox limit; default 268435456 bytes. Negative capacity fixtures override it locally |
| `E2E_JOB_TIMEOUT`, `E2E_MEDIA_TIMEOUT` | Python SDK job and media subprocess deadlines; default `30s` and `40s` |
| `E2E_TEST_TIMEOUT` | Make/launcher suite timeout; default `90m`. An explicit `go test -timeout` takes precedence |
| `E2E_BOOTSTRAP_SECRET_FILE` | Optional owner-only bootstrap credential file; otherwise generate a disposable secret |
| `E2E_ARTIFACT_DIR` | Child logs and binary/source provenance; default `artifacts` |

Defaults and timeout settings live in [test.env](test.env); durations use Go syntax. The Make launcher loads configuration before resolving dependencies, allowing `E2E_GOLANG_RUNNER_DIR` to select a checkout. Direct `go test` uses the pinned SDK unless an ignored Go workspace overrides it. Each child receives isolated configuration/databases and disposable, owner-only credential files; cleanup interrupts processes before a bounded forced stop.

### Chain requirements and fixture isolation

Local mode requires a disposable chain with the Livepeer protocol deployed. Preflight checks identity, contract resolution, canonical contract reads, and funding/activation prerequisites. Run chain-changing suites serially: tests consume funds and leave protocol transactions.

Paid fixtures share one registered recipient per chain within the run, checking activation and self-bonding before reuse. Payers, escrow, feeds, jobs, signer state, and databases remain fresh. Restarts preserve the state under test. CLI and staking cases use independent accounts; paid cases changing stake must request `isolatedRecipient`. Recipient keys survive fixture cleanup and are removed at harness exit. Recovery snapshots follow recipient setup.

Stake setup replenishes LPT only when needed. With `E2E_AUTO_FAUCET` enabled, the harness claims through go-livepeer's CLI using disposable wallets, checks actual balance changes, and transfers tokens to the funder with the Node CLI. Depletion or exhausted time/claim limits fail infrastructure prerequisites. Locally held accounts use `livepeer chain` for protocol transactions; RPC handles unlocked-account funding, test contracts, mining controls, and deliberate invalid transactions. `E2E_FUNDER_KEY_FILE` enables CLI signing for funder LPT transfers.

Expiry, reorg, and withdrawal cases require `evm_snapshot`, `evm_revert`, and `evm_mine`. Ordinary redemption/restart/outage cases can wait for natural advancement within `E2E_BLOCK_TIMEOUT`. Ticket values must cover redemption gas at the selected chain's gas price. Node redemption requires real `finalized` tags. Clearinghouse listeners require Ethereum headers whose RLP hashes match their reported hashes. Oracle mutation requires the harness-owned feed; external feeds are never modified.

### RPC diagnosis

A failed RPC connection is an infrastructure failure. With `E2E_RPC_URL` exported, check the configured endpoint:

```sh
curl -sS "$E2E_RPC_URL" -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}'
```

Compare the hexadecimal response with the decimal `E2E_EXPECTED_CHAIN_ID` you configured. When deliberately selecting a different disposable endpoint, update its Controller and expected chain ID together. Provision any required transport or port forwarding for that endpoint separately.

## Off-chain parity coverage

Both orchestrators exercise Python/Go registration, discovery and capacity, authorization and session isolation, persistent/single-shot lifecycles, proxy routing, static startup/runtime registration, and health failures. Media tests decode two video and two audio tracks and verify their colors, frequencies, sample/frame counts, and segment statistics. Python owns media verification and exits unsuccessfully on failure.

## On-chain cases

Paid payment, recovery, oracle, signing, and settlement tests exercise go-livepeer/Node orchestrators crossed with go-livepeer/Node signers in both wallet modes. Each job keeps one signer backend and key throughout replicas and restarts.

- `same-wallet`: the registered recipient also pays redemption gas.
- `split-wallet`: a fresh inactive redeemer receives only `E2E_REDEEMER_ETH`, no LPT. Orchestrators load its key and receive the recipient through Node's `OrchestratorAddress` or go-livepeer's `-ethOrchAddr`. Tickets and attribution identify the recipient; redemption transactions identify the redeemer.

The successful-job matrix also varies Python/Go runner SDK, persistent/single-shot mode, USD fixed/hourly price, and proxy setting: 128 combinations. Python performs successful payment flows; direct protocol requests supply invalid/conflicting inputs. CLI, infrastructure, off-chain, management-only, and listener-only tests run outside the wallet matrix.

### Streaming uploads

`TestOnchainStreamingUploads` covers paid single-shot uploads against Node with both signer implementations, wallet modes, runner SDKs, and proxy settings. It verifies the `Expect: 100-continue` payment handshake: unpaid challenges and rejected tickets must leave the input unread and send no `100 Continue`; accepted payments must permit continuation and deliver every byte, checked by size and SHA-256. Delayed collateral checks expose premature continuation by proxies. The Python client also exercises async iterators and seekable files starting at a nonzero position, using `StreamPayload(expect100=True, retry_buffer=False)`.

Compose applies the same assertions through its HTTPS tunnels and fixture adapters. Every intermediary must preserve the `Expect` header and defer continuation until payment is accepted. go-livepeer orchestrators are skipped because they do not support this payment handshake.

```sh
E2E_ENV_FILE=test.local.env E2E_ORCHESTRATORS=node \
  go test -count=1 -timeout 30m -run '^TestOnchainStreamingUploads$' -v .
E2E_ENV_FILE=test.local.env make test-compose-gcp \
  E2E_COMPOSE_TEST_ARGS='-test.run=^TestOnchainStreamingUploads/node/node/same-wallet$'
```

### Payment and recovery assertions

| Group | Checks |
|---|---|
| Registration and prices | Identity, filtering, normalized prices, valid/invalid dynamic and static registration, preserved agreed session prices |
| Admission and credit | Unpaid challenges, paid forwarding, capacity, fixed/live charging, replenishment, rounding, ceilings, and exhaustion |
| Credentials and concurrency | State binding/tampering, ticket policy and batch limits, refresh, signer continuity, replay, and independent concurrent jobs |
| Funding and authorization | Deposit/reserve/withdrawal limits, funding recovery, webhook decisions/cache/identity, and unusable credentials on rejection |
| Lifecycle and media | Channels/proxies, stop/unregister, health, cancellation, shutdown, and decoded multitrack media |
| Redemption | Persisted winners, recipient/payer/gas identities, expiry, escrow debit, invalid signatures before redemption, successful recovery, duplicate rejection, restarts, outages, fee caps, finality, and reorgs |

Infrastructure checks report canonical-header, finality, and chain-control requirements separately. Missing prerequisites fail dependent cases before their behavior can be validated.

### Oracle timing

Runtime oracle checks require `E2E_ORACLE_TIMEOUT` and `E2E_ORACLE_MAX_AGE` to exceed `E2E_ORACLE_POLL_INTERVAL`. Set the polling interval to match the tested builds; this setting describes their behavior and does not change their polling interval. The default `90s` oracle timeout is shorter than the default `1h` polling interval, so runtime refresh cases need a larger oracle timeout and a suite timeout covering all selected pairings. Restart checks verify that the same orchestrator/backend and signer key read the changed rate at startup.

Invalid/stale observation cases allow the last valid cached rate to live until its age limit, then require pricing and signing to become unavailable. Node uses a short configurable age limit for these fixtures. go-livepeer exposes no equivalent maximum-observation-age setting; oracle-expiry coverage checks the same rejection behavior for both implementations.

## Clearinghouse Batteries cases

`clearinghouse serve` runs isolated SQLite storage, management, authorization, accounting, and an optional chain listener. Both signers emit real usage. The anonymous go-livepeer producer uses `e2e-kafka-proxy`, which preserves records and acknowledges only upstream acceptance with Clearinghouse's SCRAM credentials. Compose also uses the WSS bridges.

Coverage includes ETH/USD budgets and keys; service permissions; authorization/revocation and validity windows; exact usage balances and ledger entries; exhaustion, funding, concurrent overdraw, late usage; malformed events, replay and conflicting IDs; and escrow/settlement attribution. Replay/quarantine cases create their own real USD event, so individual subtests can be selected directly.

Node additionally tests durable outbox capacity, unusable storage, broker outages, and restart/drain. go-livepeer has an in-memory producer queue. Management and Kafka proxy tests need no chain. Signing/usage retain every paid backend/wallet pairing; settlement requires compatible chain headers and controls.

`TestClearinghouseListenerRecovery` runs checkpoints/confirmations, RPC outage, reorg rollback/replay, and retained-history exhaustion once each using only a funded payer and Clearinghouse. These four cases need canonical headers and mining/snapshot controls, with no runner, signer, or Kafka fixture.

## Run artifacts

Each run writes child logs, `provenance.json`, and a timestamped provenance record under `E2E_ARTIFACT_DIR` (default `artifacts`), ignored by Git. Provenance includes configured checkout revisions/working changes and binary paths, SHA-256, and Go build metadata. Prebuilt binary revisions may differ from checkout revisions and are recorded separately. Temporary binaries and secrets are removed on ordinary completion.

A full-suite pass requires every product and infrastructure check to pass. Failed prerequisites leave dependent behavior unvalidated.
