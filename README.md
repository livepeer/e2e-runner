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

Each invocation snapshots the configured source working trees, builds Linux binaries and runner environments, and creates a unique Compose project with a fresh `livepeer/geth-with-livepeer-protocol:confluence` container. Compose requires all four source checkouts: go-livepeer, Node, Clearinghouse, and the Python runner, even when selecting a subset of tests. It discovers the deployed Controller from `SetContractInfo` logs and reads the chain ID, ignoring externally configured RPC, Controller, feed, and funding credentials. `E2E_LPMS_DIR` supplies an optional local lpms replacement; a sibling checkout is detected automatically. Other relative module replacements must be made available explicitly before building.

The devnet must support `finalized` block tags, canonical Ethereum headers, and `evm_snapshot`/`evm_revert`/`evm_mine` for the corresponding redemption, listener, and recovery cases. The default image does not guarantee these capabilities; incompatible chains fail the dependent infrastructure prerequisites. `E2E_GETH_IMAGE` can select a compatible devnet image with the Livepeer protocol already deployed and unlocked disposable funding accounts. Compose creates fresh chain state for each invocation.

Components run in separate containers, created as tests need them, including replicas and restart fixtures. Python and Go SDK runners execute inside their runner containers. Test callback state and assertions stay in the harness; runner application adapters forward requests through a TLS tunnel to it.

The mode requires a Linux host with Docker, Compose v2, and Python 3. Loopback origins use Linux host networking, with no application ports exposed on public VM interfaces. Component HTTP endpoints use HTTPS Quick Tunnel URLs. A gateway pool reuses public hostnames between fixtures, and a shared TLS control tunnel isolates callback routes by random IDs. Tunnel provisioning makes up to five attempts for HTTP 429 responses, honoring valid `Retry-After` headers with exponential backoff as a minimum and fallback. Exhausted retries are infrastructure failures. Kafka uses a binary WebSocket bridge over WSS with SCRAM authentication. Plaintext transport is limited to local origin and adapter connections; certificate verification remains enabled for TLS connections.

The tunnel path must support the HTTP behavior required by the selected tests, including streaming upload handshakes. See [Cloudflare's Quick Tunnel documentation](https://developers.cloudflare.com/tunnel/get-started/quick-tunnels/) for service limits.

Quick Tunnels allocate one hostname per endpoint. Compose therefore exercises proxy URL templates using a final path placeholder (`https://…trycloudflare.com/e2e-proxy/{proxy}`). Local process mode exercises hostname templates.

`make test-compose-gcp` requires Python 3 and an authenticated `gcloud` CLI with Compute Engine access. It uses the active gcloud project by default, creates an Ubuntu VM with no service account, installs Docker, uploads source snapshots, runs Compose, collects logs and provenance, and deletes the VM and boot disk. It also configures automatic deletion after four hours. Set `E2E_GCP_PROJECT`, `E2E_GCP_ZONE` (default `us-west1-b`), or `E2E_GCP_MACHINE_TYPE` (default `e2-standard-8`) to override provisioning. Failures return a nonzero exit status. A deletion failure is reported with the exact VM identity. Cloud charges apply while the VM runs.

Local Compose results live in `artifacts/e2e-*/artifacts`; GCP results are retained as `artifacts/e2e-compose-*/results.tar.gz`. Results include component/tunnel logs, `devnet.json`, endpoint mappings, source revisions/working changes, and binary hashes/build metadata. Temporary wallets and credential files are removed. Runtime images and external Go build/module cache volumes are reusable; chain state is never reused.

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

Timeout settings in `test.env` cover HTTP, RPC, builds, readiness, transactions, activation, discovery, oracle refresh, live exhaustion, accounting, and redemption. They are Go duration strings. The Make launcher also loads configuration before resolving the Go SDK dependency, so `E2E_GOLANG_RUNNER_DIR` can select a checkout without editing `go.mod`. Direct `go test` uses the pinned Go runner module; use the Make launcher or an ignored Go workspace to test a source checkout.

Child processes receive isolated databases and configuration, and cleanup interrupts them before a bounded forced stop. Disposable keystores, passwords, webhook credentials, and Kafka credentials are generated in temporary directories with owner-only file permissions.

### Chain requirements and fixture isolation

Local process mode requires an existing chain with the Livepeer protocol deployed; Compose provisions its own fresh devnet. Preflight verifies chain identity, Controller and protocol contract resolution, canonical block-hash contract reads, ETH/LPT funding resources, and round activation. Tests consume disposable-chain ETH/LPT and leave protocol transactions on that chain.

Ordinary paid fixtures share one registered recipient R within a harness invocation, keyed by chain ID, genesis hash, and Controller. R is funded, bonded, registered, and activated on first use, then checked for activation and self-bonding before reuse. Pool changes may require additional stake. Its temporary keystore survives fixture cleanup and is removed when the harness exits. Payers, escrow, feeds, signer state, jobs, and component databases are fresh per fixture; restart cases retain the state they exercise. CLI account-creation, faucet, registration, and staking tests use independent accounts, as do paid cases that deliberately change staking. Recovery snapshots occur after R setup, and activation is checked after rollback.

LPT is replenished only when stake setup needs it. If funding is short and `E2E_AUTO_FAUCET` is enabled, the harness claims from the Controller's faucet through the go-livepeer CLI and transfers the tokens to the selected funder using the Node CLI. Claims use disposable wallets and must produce actual balance changes. Faucet depletion, exhausted claim/time limits, or insufficient funds with automatic funding disabled are infrastructure failures.

Locally held protocol accounts use `livepeer chain` for transfers, bonding/approval, registration, round initialization, rewards and TicketBroker funding/unlock/cancel/withdrawal. Raw RPC is used for ETH funding and LPT transfers from accounts whose keys are held only by the RPC, test contract deployment/mutation, mining controls and intentionally invalid transactions. Configure `E2E_FUNDER_KEY_FILE` to use the Node CLI for funder LPT transfers too. CLI coverage compares account creation, encrypted keystores, overwrite protection, and resulting on-chain balances, stake, commission and service URI state. Faucet claims always use go-livepeer.

Controlled expiry, snapshots, deterministic reorgs, and withdrawals require a disposable endpoint exposing `evm_snapshot`, `evm_revert`, and `evm_mine`. Ordinary redemption/restart/outage cases can wait for natural chain advancement within `E2E_BLOCK_TIMEOUT`; they do not require mining controls. Redemption confirmation for Node requires a real `finalized` block tag. Clearinghouse listener cases require standard Ethereum headers whose RLP hashes equal their reported block hashes. Oracle mutation cases require the harness-owned feed; an externally configured feed is not modified.

### RPC diagnosis

A failed RPC connection is an infrastructure failure. With `E2E_RPC_URL` exported, check the configured endpoint:

```sh
curl -sS "$E2E_RPC_URL" -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}'
```

Compare the hexadecimal response with the decimal `E2E_EXPECTED_CHAIN_ID` you configured. When deliberately selecting a different disposable endpoint, update its Controller and expected chain ID together. Provision any required transport or port forwarding for that endpoint separately.

## Off-chain parity coverage

Off-chain scenarios run against both implementations: Python/Go dynamic registration; persistent/single-shot modes; proxy on/off; metadata and capacity discovery; busy/release/unregister behavior; bootstrap and heartbeat authorization; per-session token isolation; channels and public proxy creation/deletion/forwarding; domain proxy templates; startup static registration; runtime CLI registration; failed static health; session-control APIs; and Python media with two video and two audio tracks, verifying decoded colors/frequencies and segment statistics.

Startup static fixtures translate to go-livepeer JSON or Node TOML. Runtime CLI registration is tested separately from startup registration. Rejection checks compare status and behavior, without requiring identical error text.

## On-chain cases

Payment, recovery, oracle, Clearinghouse signing, and Clearinghouse chain-recovery coverage run the four pairings below in both `same-wallet` and `split-wallet` modes. Every job uses one signer implementation and key for its entire lifetime, including replica and restart scenarios. Attempts to use a different key exercise isolation checks.

| Orchestrator | Signer |
|---|---|
| go-livepeer | go-livepeer |
| go-livepeer | Node |
| Node | go-livepeer |
| Node | Node |

In `same-wallet` mode, recipient and redeemer are the run-scoped R, retaining the default recipient configuration. In `split-wallet` mode, recipient is still R, while a fresh, distinct, inactive hot account H receives only `E2E_REDEEMER_ETH` for redemption gas and no LPT. Each split fixture's orchestrator processes load only H's keystore; Node receives R through `OrchestratorAddress`, and go-livepeer receives `-ethOrchAddr`. “Cold” is relative to that split fixture: earlier same-wallet use of R is intentional, and the harness retains R's key for setup. Secondary processes and restarts preserve these roles. Challenges, signed/persisted tickets, redemption events, and Clearinghouse attribution are checked against R; redemption transaction signers are checked against R in same-wallet mode and H in split-wallet mode.

Successful-job coverage varies runner SDK (Python/Go), mode (persistent/single-shot), price unit (USD/hour or USD/fixed), and proxy setting across both wallet modes. The Python payment client performs the successful payment flow; direct protocol requests create malformed or conflicting inputs. Both wallet modes run serially. CLI, infrastructure, off-chain, and Clearinghouse management-only cases run independently of wallet mode.

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

| Group | Purpose and assertions |
|---|---|
| Identity and discovery | Recover the signer identity; check challenge and signed-ticket recipients, application/GPU filters, normalized wei prices, and capacity. Node advertises the ticket recipient; go-livepeer advertises its service signing account, which is the hot address in split mode. Each backend's identity signature format is verified explicitly. |
| Dynamic and static prices | Valid fixed/hourly startup and runtime registrations; reject missing, empty, zero, negative, malformed, unsupported-currency and unsupported-unit prices at initial registration, heartbeat update, startup and runtime CLI registration. Invalid updates must retain the current runner price. |
| Zero-price/session regression | Paid sessions retain agreed prices after rejected missing/zero updates and valid repricing; new sessions use updated prices. |
| Challenge/admission | Unpaid reservation returns parameters without forwarding work or consuming a slot; valid payment forwards the response and reserves capacity; stop releases it. |
| Fixed/live credit | Fixed per-request charging; fixed-session follow-up rejection; initial live credit, replenishment, duplicate recipient submission rejection, bounded exhaustion, capacity release and no-ticket-needed responses. |
| Ceilings/conversion | Below/equal/above request, webhook, and configured limits; exact conversion; one-ticket rounding. Changed/invalid/stale oracle observations exercise repricing or rejection. |
| Signed state | Sequence advancement, unchanged identity, tampering, app/type/manifest binding, invalid credentials, and separate replica/restart continuation on the same signer implementation/key. |
| Ticket validation | Invalid recipient, face value, probability, random hash, expiry, payer, signature, nonce, expected price, missing tickets, duplicate tickets, batch-count/exposure boundaries and refresh thresholds. |
| Replay/concurrency | One admission for concurrently replayed credentials; independent paid jobs cannot exceed available slots. Signer HTTP retries are not assumed idempotent. |
| Refresh | Valid admitted-session refresh, wrong sender rejection, nonce threshold, expiration, preserved session identity and new payment parameters. |
| Funding | Missing/insufficient deposit or claimable reserve, impending withdrawal, recovery after funding/cancel-unlock, and concurrent accounting liabilities. |
| Authorization | Approve/reject; unavailable/malformed webhook; cached and per-request decisions; changed authorization identity; price limits; rejection returns no usable payment credentials. |
| Media/lifecycle | Paid multitrack media and cleanup, stop/unregister, live exhaustion, channels/proxies, cancellation, failed health and shutdown. |
| Redemption/recovery | Observe actual winning tickets using bounded generation; wait beyond acceptance expiry; check persisted recipient, transaction gas payer, TicketBroker event payer/recipient, and escrow debit; reject duplicate/invalid redemption using the hot account after stopping its worker; pending restart, RPC interruption, fee limits, delayed confirmation and reorg recovery. Node confirms through `finalized`; go-livepeer uses mined receipts. |

Infrastructure checks report canonical-header, finality, and chain-control requirements separately. Recovery coverage includes expiry, redemption, pending restart, outage, failed transaction, fee cap, delayed finality and reorg scenarios per pairing. Cases needing chain controls fail before dependent setup if those controls are unavailable. Oracle coverage includes all four pairings using R, fresh funded payers and feeds, and isolated databases. Active-pool registration is required to obtain valid payment challenges, including for signing and discovery probes.

### Oracle timing

Runtime oracle checks require `E2E_ORACLE_TIMEOUT` and `E2E_ORACLE_MAX_AGE` to exceed `E2E_ORACLE_POLL_INTERVAL`. Set the polling interval to match the tested builds; this setting describes their behavior and does not change their polling interval. The default `90s` oracle timeout is shorter than the default `1h` polling interval, so runtime refresh cases need a larger oracle timeout and a suite timeout covering all selected pairings. Restart checks verify that the same orchestrator/backend and signer key read the changed rate at startup.

Invalid/stale observation cases allow the last valid cached rate to live until its age limit, then require pricing and signing to become unavailable. Node uses a short configurable age limit for these fixtures. go-livepeer exposes no equivalent maximum-observation-age setting; oracle-expiry coverage checks the same rejection behavior for both implementations.

## Clearinghouse Batteries cases

The all-in-one `clearinghouse serve` fixture starts isolated SQLite storage, management API, signing-authorization webhook, and an on-chain listener using the TicketBroker address resolved from the Controller. Both signer fixtures enable authenticated loopback Kafka and accounting. go-livepeer connects anonymously to the harness's minikafka-derived `e2e-kafka-proxy`, which forwards record keys, payloads, timestamps and headers to Clearinghouse using its SCRAM producer credentials. The proxy acknowledges Produce only after upstream acceptance and restricts forwarding to the configured single-partition topic. In Compose mode it runs in the `kafka-proxy` service and uses the Kafka transport bridges.

| Group | Purpose and assertions |
|---|---|
| Budgets/keys | ETH/USD grant and allocation creation/funding, API keys, exact balances, revocation refunds, allocation isolation, usage and ledger reporting. |
| Service permissions | Missing/wrong tokens, wrong roles, insufficient management permissions, authorized operations. |
| Signing authorization, both signers | Valid/malformed/revoked keys, paused/inactive grants or allocations, revoked allocations/sessions, validity windows, zero funding and binding changes. |
| Actual signer usage, both signers | Real signer events from jobs against each orchestrator; ETH/USD fixed/live charging, recipient/session identity, ticket count/job type, exact allocation balances and ledger entries. |
| Exhaustion/funding | Applied charges exhaust an allocation; later signing fails; additional funding restores eligibility; delayed/concurrent issuance permits recorded overdraw. |
| Injected event validation | Identical replay without another charge; conflicting event IDs; malformed envelopes; invalid fees/counts/times; missing USD amounts; unknown sessions and recipient/app/type mismatches; unknown event types ignored. |
| Persistence/outages, Node outbox | Broker interruption, pending signer outbox, signer restart/drain, clearinghouse restart, duplicate delivery, capacity failure withholding payment and unusable outbox storage failing startup. |
| Revocation/late usage | Return unused allocation funds while recording actual previously issued usage delivered after revocation. |
| Escrow, both signers | Deposit/reserve funding, claims, winning-ticket settlement, unlock/withdrawal and reconciliation with chain balances. |
| Attribution | Both signers' usage matches settlement without another grant charge; delayed and unmatched attribution; ambiguous attribution using injected events. |
| Listener recovery | Confirmations, persisted checkpoints, repeated logs, RPC outage, reorg rollback/replay and failure beyond retained reorg history. |

Management and Kafka proxy checks need no chain. Signing and usage coverage includes all four pairings and can run independently of listener compatibility. Escrow, settlement attribution and listener recovery require a compatible controlled chain.

go-livepeer usage charging, usage-based exhaustion and usage-to-settlement attribution use real signer events. Its in-memory producer queue has no durable outbox, so storage, capacity and restart/drain coverage applies to Node.

## Run artifacts

Each run retains child logs in `artifacts` (or `E2E_ARTIFACT_DIR`) and writes `provenance.json` plus a timestamped provenance record. These generated files are ignored by Git. Provenance records configured source checkout revisions and working changes, binary paths/SHA-256, and Go build metadata; unconfigured source checkouts are omitted. Temporary build binaries and secret material are removed on ordinary completion. A supplied prebuilt binary's build revision may differ from its configured source checkout and is recorded separately.

A successful full-suite run requires all product and infrastructure checks to pass. Failed prerequisites leave their dependent assertions unvalidated. Finality, canonical header decoding, and chain controls are reported separately to identify missing capabilities in the selected environment.
