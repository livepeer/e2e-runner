# Payment comparison

This opt-in harness measures the three-step payment path in [builder issue 89](https://github.com/NE-Build-Track/livepeer-builder-core/issues/89): unpaid challenge, remote signing, and paid forwarding. A minimal HTTP runner records arrival and echoes the request identity. It measures payment overhead, without GPU inference.

Four matched signer/orchestrator pairs run sequentially on one disposable GCP `n2-standard-8` VM:

| Target | Source |
|---|---|
| `node-current` | livepeer/node `e2b331958a5704016310153a7d33b7a634758f38` |
| `node-pr1` | BlueClawNetwork/livepeer-node PR 1, `bbe126e5eddbf59f9cc9f94d3c37703215c26850` |
| `node-combined` | Exact clean combined checkout supplied to the driver |
| `go-livepeer` | Upstream master snapshot `2a4f7c48239a3b3e6ce49ca793a41253ec664b26` |

The manifest records source revisions, binary hashes, harness source hashes, toolchains, and protocol settings. The original PR ref `refs/remotes/blueclaw-review/pr-1-original` must exist in the combined checkout; its history must contain the pinned current and PR snapshots.

## Run

With authenticated `gcloud`, Docker build access on the VM, and a clean reviewed combined checkout:

```sh
python3 performance/gcp.py --combined-checkout /absolute/path/to/combined --reviewed
```

The driver provisions a VM with an eight-hour auto-delete deadline, builds pinned binaries, deploys the real protocol on Anvil, validates chain capabilities, runs correctness and repository race suites, runs the timing matrix, collects results, and explicitly deletes the VM in `finally`. Results land in `artifacts/payment-perf-<UTC timestamp>/results.tar.gz`. No external service account is attached. Test-owned private keys remain outside the collected results.

`bootstrap.sh` and `build.sh` prepare `/tmp/payment-perf` on the VM, mounted as `/work` in the runtime. The runtime uses LPMS's required Livepeer FFmpeg revision, rather than the system FFmpeg ABI. For an already prepared VM, `run.py` supports `manifest`, `smoke`, `correctness`, `validate`, and `matrix`; `controlled.py` installs test-only overlays in disposable source clones. Product source checkouts are not patched by the overlays. `--quick` creates pilot cells that the formal report excludes. `--provisional-combined` permits a clearly labeled snapshot while source review continues; it does not mark review complete.

## Measurements

- Three repetitions at 0 and 110 ms additional delay per RPC HTTP exchange, with 1 and 32 closed-loop clients. Batches retain their original body and incur one delay. Each serial cell attempts 128 jobs; each parallel cell attempts 1,024. Target order rotates by repetition.
- Every job obtains a fresh challenge. Eight warmup jobs precede measured warm jobs. The first job is retained separately, including failures and readiness retries. Worker processes, payer keys, and databases are fresh for every cell. A run-owned active recipient is shared to avoid repeatedly growing the protocol's active set.
- The first serial repetition includes separate 60-second idle windows before the first job and after known-payer load. RPC records retain worker role, window, method, contract function, batch status, duration, and cancellation. Window totals include background traffic; they are not exact foreground attribution.
- Requests retain challenge, signer, receive, runner arrival, paid-response first-byte, and total timings, HTTP errors, ticket parameters, and echoed identity. Goodput includes all attempted jobs in its duration and counts only successes. Quantiles use successes; failures remain explicit. A p99 is reported only with at least 1,000 successes.
- CPU uses Linux worker process counters. RSS is sampled every 100 ms, so it is a sampled peak. HTTP readiness is measured with the local provider before delay injection and does not imply payment readiness.
- Timing uses rare-winning tickets with expected value 10,000,000 wei and a one-ticket price. Protocol correctness uses frequent winners; recovery explicitly raises exposure limits to cover redemption gas. Neither changes the measured timing policy.

The shared controlled overlay compares frozen heads, failed reorg backfill, funding/round/registry changes, cancellation, partial failure isolation, a 32-key cold burst, and populations of 0, 16, 128, and 1,024 payers under idle, known-key, and churn workloads. It reports exchanges and contained calls separately. Its race-instrumented fixture timings are not production latency estimates.

`diagnose_go_manifest.py` probes the pinned go-livepeer manifest-ID generator with 32 callers in an isolated source export under the race detector. It retains duplicate-ID counts and race traces separately from timed requests. A nonzero probe result is a recorded comparison finding and does not suppress the timing matrix. Projects retain their declared Go toolchains; embedded binary versions are recorded individually.

The real-chain fixture uses native finalized headers, canonical EIP-1898 reads, and actual snapshot/revert. Delayed-finality cases hold a previously observed genuine finalized header. Anvil mines naturally at one-second intervals; the protocol unlock period is two rounds to bound withdrawal tests. This exercises application behavior without establishing production consensus safety.

Reorg recovery verifies the original randomness reveal occurred after ticket expiry. If the identical already-public signed transaction is replayed on the replacement branch, its canonical mining block may precede that expiry after rollback. The fixture checks the original reveal, exact transaction identity, canonical receipt, and a single canonical settlement. A different transaction still has to satisfy the normal expiry check.

Run `python3 performance/report.py /path/to/results` after extraction, with `manifest.json` beside the results directory. It writes the report and machine-readable summaries. Earlier infrastructure/fixture failures stay retained with an explicit classification; they are separated from current product findings.
