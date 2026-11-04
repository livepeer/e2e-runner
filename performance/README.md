# Payment comparison

This opt-in harness measures the three-step payment path in [builder issue 89](https://github.com/NE-Build-Track/livepeer-builder-core/issues/89): unpaid challenge, remote signing, and paid forwarding. A minimal HTTP runner records arrival and echoes the request identity. It measures payment overhead, without GPU inference.

Matched signer/orchestrator pairs run sequentially on one disposable GCP `n2-standard-8` VM. Two baselines remain pinned in `bootstrap.sh`:

| Target | Source |
|---|---|
| `node-current` | livepeer/node `e2b331958a5704016310153a7d33b7a634758f38` |
| `go-livepeer` | Upstream snapshot `2a4f7c48239a3b3e6ce49ca793a41253ec664b26` |

Additional Node builds use the existing `test.env` format. Copy it to `test.local.env` and add, for example:

```dotenv
E2E_PERF_NODE_TARGETS=upstream-tip,experiment
E2E_PERF_NODE_UPSTREAM_TIP_REPO=https://github.com/livepeer/node.git
E2E_PERF_NODE_UPSTREAM_TIP_REF=refs/heads/main
E2E_PERF_NODE_EXPERIMENT_CHECKOUT=/absolute/path/to/clean/node-checkout
E2E_PERF_NODE_EXPERIMENT_REF=HEAD
```

The names are your own: add or remove comma-separated entries to change the comparison. An empty or unset list compares only the two fixed baselines. Names contain lowercase letters, digits, and single hyphens; their environment-variable keys uppercase the name and replace hyphens with underscores. `node-current`, `go-livepeer`, and `protocol` are reserved.

For each additional build, `REPO` defaults to `https://github.com/livepeer/node.git`. Supply `REF` as a commit, branch, tag, or full PR ref such as `refs/pull/43/head`. Alternatively, `CHECKOUT` uses a clean local repository, with `REF` defaulting to `HEAD`; other local refs work too. Checkout paths resolve from the invoking directory. Checkout provenance uses its origin URL when available. No specially named Git refs or shared history between builds are required.

The driver uses the same Go dotenv loader as the test launcher. `E2E_ENV_FILE` chooses the file, and process environment values take precedence. Each requested ref resolves to an exact commit and an independent source bundle **before** VM provisioning. The existing generated `manifest.json` retains target names, backends, repositories, requested refs, revisions, and later the binary/toolchain hashes. There is no additional input manifest format. Changing a branch after preparation cannot change the measured build.

## Run

With authenticated `gcloud`, Go available locally, Docker build access on the VM, and any configured checkouts clean:

```sh
E2E_ENV_FILE=test.local.env python3 performance/gcp.py
# Or select a subset of the fixed and configured targets:
E2E_ENV_FILE=test.local.env python3 performance/gcp.py --targets=node-current,experiment
```

The selection reaches correctness, controlled overlays, repository validation, and timing. Only selected additional builds are prepared and built. The two fixed builds remain available because the Go fixture also uses Node's tooling. Each selected target contributes 12 formal cells (three repetitions, two RPC delays, two client counts); reports compute the expected count from the actual matrix and compare every measured build against every other measured build.

The driver provisions a VM with an eight-hour auto-delete deadline, builds pinned binaries, deploys the real protocol on Anvil, validates chain capabilities, runs correctness and repository race suites, runs the timing matrix, collects results, and explicitly deletes the VM in `finally`. Results land in `artifacts/payment-perf-<UTC timestamp>/results.tar.gz`. No external service account is attached. Test-owned private keys remain outside the collected results.

`bootstrap.sh` and `build.sh` prepare `/tmp/payment-perf` on the VM, mounted as `/work` in the runtime. The runtime uses LPMS's required Livepeer FFmpeg revision, rather than the system FFmpeg ABI. For an already prepared VM, `run.py` supports `manifest`, `smoke`, `correctness`, `validate`, and `matrix`; `controlled.py` installs test-only overlays in disposable source clones. Product source checkouts are not patched by the overlays. `--quick` creates pilot cells that the formal report excludes. Use `--targets=name,another-name` to select any configured subset. Completed timing cells are reused only when their revision, binary, harness, and settings match; changed inputs require a fresh results directory.

## Measurements

- Three repetitions at 0 and 110 ms additional delay per RPC HTTP exchange, with 1 and 32 closed-loop clients. Batches retain their original body and incur one delay. Each serial cell attempts 128 jobs; each parallel cell attempts 1,024. Target order rotates by repetition.
- Every job obtains a fresh challenge. Eight warmup jobs precede measured warm jobs. The first job is retained separately, including failures and readiness retries. Worker processes, payer keys, and databases are fresh for every cell. A run-owned active recipient is shared to avoid repeatedly growing the protocol's active set.
- The first serial repetition includes separate 60-second idle windows before the first job and after known-payer load. RPC records retain worker role, window, method, contract function, batch status, duration, and cancellation. Window totals include background traffic; they are not exact foreground attribution.
- Requests retain challenge, signer, receive, runner arrival, paid-response first-byte, and total timings, HTTP errors, ticket parameters, and echoed identity. Goodput includes all attempted jobs in its duration and counts only successes. Quantiles use successes; failures remain explicit. A p99 is reported only with at least 1,000 successes.
- CPU uses Linux worker process counters. RSS is sampled every 100 ms, so it is a sampled peak. HTTP readiness is measured with the local provider before delay injection and does not imply payment readiness.
- Timing uses rare-winning tickets with expected value 10,000,000 wei and a one-ticket price. Protocol correctness uses frequent winners; recovery explicitly raises exposure limits to cover redemption gas. Neither changes the measured timing policy.

The optional shared controlled overlay compares frozen heads, failed reorg backfill, funding/round/registry changes, cancellation, partial failure isolation, a 32-key cold burst, and populations of 0, 16, 128, and 1,024 payers under idle, known-key, and churn workloads. To enable it, set `E2E_PERF_CONTROLLED_FIXTURE=/absolute/path/to/chainrpc.go` in the same env configuration. Supply a Go provider fixture implementing the `ChainRPC` API used in `controlled_test.go.txt`. The driver freezes that input, copies it to the disposable VM, and records its SHA-256 in the run manifest and overlay results. No provider fixture is bundled with the harness. Without one, controlled suites record an explicit skip; real-process correctness, repository race tests, and timing still run.

Controlled adapters are selected by source API rather than target name: direct reads, `NewChainView`, or `NewPaymentObserver`. To choose explicitly, set `E2E_PERF_NODE_<NAME>_CONTROLLED=direct|chain-view|payment-observer|none`; the default is `auto`. Unknown APIs or `none` also produce an explicit skip. Recognized adapters depend on private Go APIs; incompatible changes may require updating the adapter. Overlay compilation failures remain visible as failures, never passes. The overlay reports exchanges and contained calls separately; its race-instrumented fixture timings are not production latency estimates.

`diagnose_go_manifest.py` probes the pinned go-livepeer manifest-ID generator with 32 callers in an isolated source export under the race detector. It retains duplicate-ID counts and race traces separately from timed requests. A nonzero probe result is a recorded comparison finding and does not suppress the timing matrix. Projects retain their declared Go toolchains; embedded binary versions are recorded individually.

The real-chain fixture uses native finalized headers, canonical EIP-1898 reads, and actual snapshot/revert. Delayed-finality cases hold a previously observed genuine finalized header. Anvil mines naturally at one-second intervals; the protocol unlock period is two rounds to bound withdrawal tests. This exercises application behavior without establishing production consensus safety.

Reorg recovery verifies the original randomness reveal occurred after ticket expiry. If the identical already-public signed transaction is replayed on the replacement branch, its canonical mining block may precede that expiry after rollback. The fixture checks the original reveal, exact transaction identity, canonical receipt, and a single canonical settlement. A different transaction still has to satisfy the normal expiry check.

Run `python3 performance/report.py /path/to/results` after extraction, with `manifest.json` beside the results directory. It writes the report and machine-readable summaries. Earlier infrastructure/fixture failures stay retained with an explicit classification; they are separated from current product findings.

Local harness checks: `python3 -m unittest discover -s performance -p "test_*.py"`. These exercise configuration, Git snapshot preparation, target routing, result reuse, and reporting without provisioning a VM.
