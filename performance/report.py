#!/usr/bin/env python3
"""Summarize measured successes, failures, RPC work, and actual sample counts."""
import collections
import json
import math
import re
from pathlib import Path
import sys

HISTORICAL_RUNS = {
    "controlled-adapter-initial": ("adapter_error", "The overlay used SenderInfo after the combined stack renamed that adapter to PayerFunds; the corrected overlay was rerun."),
    "smoke-mainnet-fixture": ("fixture_error", "The signer defaulted to mainnet while the local Ethereum orchestrator used devenv; both now explicitly use devenv."),
    "smoke-initial": ("superseded_fixture", "Initial fixture run before bounded round-keeper convergence; replaced by the current smoke run."),
}


def quantile(values, q):
    return sorted(values)[max(0, math.ceil(len(values) * q) - 1)] if values else None


def summarize(root):
    rows = []
    for path in sorted(root.glob("*/delay-*/cell.json")):
        cell = json.loads(path.read_text())
        samples = [json.loads(line) for line in (path.parent / "requests.jsonl").read_text().splitlines()]
        warm = [s for s in samples if s["window"] == "warm"]
        successful = [s for s in warm if not s.get("error")]
        phases = {}
        for phase in ["challenge_ms", "signer_ms", "receive_ms", "dispatch_ms", "first_byte_ms", "total_ms"]:
            values = [s[phase] for s in successful]
            phases[phase] = {"p50": quantile(values, .5), "p95": quantile(values, .95)}
            if len(values) >= 1000:
                phases[phase]["p99"] = quantile(values, .99)
        rpc = collections.defaultdict(lambda: {"exchanges": 0, "calls": 0, "contract_calls": 0})
        for log in path.parent.glob("rpc-*.jsonl"):
            for line in log.read_text().splitlines():
                record = json.loads(line)
                row = rpc[record["window"]]
                row["exchanges"] += 1
                row["calls"] += sum(record["methods"].values())
                row["contract_calls"] += sum(record["contracts"].values())
        rows.append(cell | {"phases": phases, "tickets_per_request": dict(collections.Counter(s.get("tickets",0) for s in successful)),
                            "ticket_parameters": list({(s.get("face_value_wei"),s.get("win_probability")) for s in successful}),
                            "errors": dict(collections.Counter(s["error"] for s in warm if s.get("error"))),
                            "cold_samples": [s for s in samples if s["window"] == "cold"], "rpc": dict(rpc)})
    return rows


def write_report(root, rows):
    manifest_path = root.parent / "manifest.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.is_file() else {}
    lines = ["# Payment-path comparison", "", "Measured with real signer/orchestrator processes and a minimal echo runner on one disposable GCP VM. RPC delay is added once per HTTP exchange, including batches. These results do not measure GPU inference or Internet gateway latency.", "", "## Pinned inputs", "", "| Target | Commit | Embedded Go toolchain |", "|---|---|---|"]
    for name, info in manifest.get("sources", {}).items():
        repo = {"go-livepeer": "livepeer/go-livepeer", "protocol": "livepeer/protocol", "node-pr1": "BlueClawNetwork/livepeer-node"}.get(name, "livepeer/node")
        versions = ", ".join(sorted(set(info.get("binary_go_versions", {}).values()))) or "—"
        lines.append(f'| {name} | [`{info["revision"]}`](https://github.com/{repo}/commit/{info["revision"]}) | {versions} |')
    lines += ["", "Reference: [builder issue 89](https://github.com/NE-Build-Track/livepeer-builder-core/issues/89), [BlueClaw PR 1](https://github.com/BlueClawNetwork/livepeer-node/pull/1)."]
    lines += ["", "Each project's declared Go toolchain requirements are respected. The manifest records the toolchain embedded in every measured binary, separately from the runtime image's default Go version."]
    if not manifest.get("combined_review_complete",False):
        lines += ["", "**Provisional combined snapshot:** the source chat's final combined review was still pending when this revision was frozen. Results identify the exact tested commit and must not be treated as final-review approval."]
    if manifest.get("final_candidate_measured_after_baselines"):
        lines += ["", "The reviewed combined revision was measured after the frozen baseline matrix on the same VM. Baseline product binaries, payment-path instrumentation, provider-delay model, and recipient stayed unchanged. `manifest-provisional-timing.json` retains the baseline harness provenance. This ordering is a limitation when interpreting small differences between cached implementations."]
    lines += ["", "## Performance", "", f"{len(rows)}/48 formal cells produced measurement records. Each completed row should pool three repetitions; `aggregates.json` records the actual repetition counts and revisions. Dispatch latency includes the unpaid challenge, signing, paid request, and arrival at the runner. The load model is closed loop with 1 or 32 clients. Errors remain in attempted counts and goodput excludes them.", "", "| Target | RPC delay | Clients | Successful / attempted | Dispatch p50 / p95 / p99 ms | Success/s | Loaded worker exchanges / attempt |", "|---|---:|---:|---:|---:|---:|---:|"]
    groups = collections.defaultdict(list)
    for row in rows:
        groups[(row["target"], row["delay_ms"], row["workers"])].append(row)
    aggregates = {}
    fmt = lambda value: f"{value:.3f}" if value is not None else "—"
    for key, cells in sorted(groups.items()):
        target, delay, workers = key
        samples = []
        for cell in cells:
            label = f'delay-{delay:g}-workers-{workers}-repeat-{cell["repetition"]}'
            for line in (root / target / label / "requests.jsonl").read_text().splitlines():
                sample = json.loads(line)
                if sample["window"] == "warm" and not sample.get("error"):
                    samples.append(sample)
        values = [s["dispatch_ms"] for s in samples]
        attempts = sum(c["samples"] for c in cells)
        successes = sum(c["successes"] for c in cells)
        duration = sum(c["elapsed_seconds"] for c in cells)
        exchanges = sum(c["rpc"].get("warm", {}).get("exchanges", 0) for c in cells)
        p50, p95 = quantile(values,.5), quantile(values,.95)
        p99 = quantile(values,.99) if len(values) >= 1000 else None
        aggregates[key] = {"p50": p50, "p95": p95, "p99": p99, "successes": successes, "attempts": attempts,
                           "goodput": successes / duration, "repetitions": len(cells),
                           "revisions": sorted({c["revision"] for c in cells}),
                           "repeat_medians": [c["phases"]["dispatch_ms"]["p50"] for c in cells]}
        lines.append(f'| {target} | {delay:g} ms | {workers} | {successes}/{attempts} | {fmt(p50)} / {fmt(p95)} / {fmt(p99)} | {successes/duration:.2f} | {exchanges/attempts:.3f} |')
    (root / "aggregates.json").write_text(json.dumps([{"target": k[0], "delay_ms": k[1], "workers": k[2]} | v for k, v in sorted(aggregates.items())], indent=2) + "\n")
    request_errors = collections.Counter()
    for cell in rows:
        for error, count in cell["errors"].items():
            request_errors[(cell["target"], cell["delay_ms"], cell["workers"], error)] += count
    if request_errors:
        lines += ["", "### Request failures", "", "A completed cell can contain individual failed requests. They remain in attempted counts and reduce goodput; latency quantiles use successful requests.", "", "| Target | RPC delay | Clients | Count | Error |", "|---|---:|---:|---:|---|"]
        for (target, delay, workers, error), count in sorted(request_errors.items()):
            error = error.replace("\n", " ").replace("|", "\\|")
            lines.append(f'| {target} | {delay:g} ms | {workers} | {count} | {error} |')
    lines += ["", "### Payment phases", "", "Pooled p50 / p95 in milliseconds. Receive includes payment validation, dispatch, and the echo response. First byte starts at the paid POST; dispatch starts at the initial unpaid POST.", "", "| Target | RPC delay | Clients | Challenge | Signer | Paid receive | Paid first byte | Total |", "|---|---:|---:|---:|---:|---:|---:|---:|"]
    for (target, delay, workers), cells in sorted(groups.items()):
        samples = []
        for cell in cells:
            label = f'delay-{delay:g}-workers-{workers}-repeat-{cell["repetition"]}'
            samples += [s for line in (root / target / label / "requests.jsonl").read_text().splitlines()
                        if (s := json.loads(line))["window"] == "warm" and not s.get("error")]
        phases = []
        for phase in ["challenge_ms", "signer_ms", "receive_ms", "first_byte_ms", "total_ms"]:
            values = [s[phase] for s in samples]
            phases.append(f'{fmt(quantile(values, .5))} / {fmt(quantile(values, .95))}')
        lines.append(f'| {target} | {delay:g} ms | {workers} | ' + " | ".join(phases) + ' |')
    lines += ["", "Worker RPC totals include polling and redemption traffic occurring during the load window. They are not presented as exact foreground counts. The raw records and separate idle windows retain that distinction. Serial p99 is omitted when fewer than 1,000 successes were observed.", "", "### Actual ticket settings", "", "The timing policy targets 10,000,000 wei expected value and one ticket per job. go-livepeer chooses its own face value and winning probability; the raw protobuf fields are retained rather than assuming identical parameters.", "", "| Target | Observed face value, wei | Approximate winning probability | Approximate ticket EV, wei | Ticket counts in successful jobs |", "|---|---:|---:|---:|---|"]
    for target in sorted({c["target"] for c in rows}):
        selected = [c for c in rows if c["target"] == target]
        parameters = {tuple(p) for c in selected for p in c["ticket_parameters"]}
        counts = collections.Counter()
        for cell in selected:
            counts.update({int(k): v for k, v in cell["tickets_per_request"].items()})
        for face, probability in sorted(parameters):
            p = int(probability) / ((1 << 256) - 1)
            lines.append(f'| {target} | {face} | {p:.4g} | {int(face)*p:.8g} | {dict(counts)} |')
    lines += ["", "### Worker resources", "", "RSS is sampled every 100 ms during the measured window. CPU comes from Linux process counters; setup, the load generator, and the chain process are excluded.", "", "| Target | RPC delay | Clients | Worker CPU ms / attempted job | Orchestrator / signer sampled peak RSS MiB |", "|---|---:|---:|---:|---:|"]
    ticks = manifest.get("clock_ticks_per_second",100)
    for (target,delay,workers), cells in sorted(groups.items()):
        cpu = sum(sum(p["cpu_ticks"] for p in c["resources"]["processes"]) for c in cells) / ticks * 1000
        count = sum(c["samples"] for c in cells)
        peaks = [max(c["resources"]["processes"][i]["sampled_peak_rss_bytes"] for c in cells)/1048576 for i in range(2)]
        lines.append(f'| {target} | {delay:g} ms | {workers} | {cpu/count:.3f} | {peaks[0]:.1f} / {peaks[1]:.1f} |')
    lines += ["", "### Combined stack against each baseline", "", "| Baseline | RPC delay | Clients | Combined / baseline median dispatch | Median reduction |", "|---|---:|---:|---:|---:|"]
    for (target,delay,workers), result in sorted(aggregates.items()):
        if target == "node-combined":
            continue
        combined = aggregates.get(("node-combined",delay,workers))
        if combined and result["p50"] and combined["p50"] is not None:
            ratio = combined["p50"]/result["p50"]
            lines.append(f'| {target} | {delay:g} ms | {workers} | {ratio:.4f}× | {(1-ratio)*100:.2f}% |')
    lines += ["", "Ratios describe observed medians, not statistical confidence bounds. Per-repetition medians are retained in `aggregates.json`; small differences between cached implementations should be interpreted with that run-to-run variability."]
    lines += ["", "### Cold requests and startup", "", "Each cell uses fresh worker processes, payer, and databases. The recipient is shared and already active. Cold rows are the first request after registration; background observation can already run during startup and the optional 60-second pre-job idle. Startup uses the local provider without injected delay and measures HTTP readiness, not full payment readiness.", "", "| Target | RPC delay | Clients | Cold successes / attempts | Cold dispatch p50 ms | Orchestrator / signer HTTP ready p50 s |", "|---|---:|---:|---:|---:|---:|"]
    for (target, delay, workers), cells in sorted(groups.items()):
        cold = [s for c in cells for s in c["cold_samples"]]
        good = [s for s in cold if not s.get("error")]
        startup = [quantile([c["startup"][name] for c in cells], .5) for name in ["orchestrator_http_ready_seconds", "signer_http_ready_seconds"]]
        lines.append(f'| {target} | {delay:g} ms | {workers} | {len(good)}/{len(cold)} | {fmt(quantile([s["dispatch_ms"] for s in good], .5))} | {fmt(startup[0])} / {fmt(startup[1])} |')
    lines += ["", "### Idle worker RPC traffic", "", "One 60-second window before the first payment and one after known-payer load, at each delay. Counts include both workers. Windows are attributed when the proxy receives an exchange, so an in-flight exchange can finish after the boundary.", "", "| Target | RPC delay | Window | Exchanges / s | Contained calls / s |", "|---|---:|---|---:|---:|"]
    for cell in rows:
        if cell["idle_seconds"] <= 0:
            continue
        for name in ["idle-before-first-job", "idle-known"]:
            rpc = cell["rpc"].get(name, {})
            lines.append(f'| {cell["target"]} | {cell["delay_ms"]:g} ms | {name} | {rpc.get("exchanges", 0)/cell["idle_seconds"]:.3f} | {rpc.get("calls", 0)/cell["idle_seconds"]:.3f} |')
    lines += ["", "## Correctness and validation", "", "Nonzero current-suite exits remain findings; they do not remove a candidate from the comparison. Earlier fixture or adapter failures are retained separately. Controlled overlays run the same scenarios against each Node revision and retain the fixture and overlay hashes.", "", "| Target | Suite | Exit status | Classification |", "|---|---|---:|---|"]
    failures, controlled, historical = [], [], []
    for path in sorted(root.glob("*/*/exit.json")):
        if path.parent.name.startswith("pilot-") or path.parent.name == "prepare-recipient":
            continue
        status = json.loads(path.read_text())
        if path.parent.name.startswith("delay-") and status["exit_code"] == 0:
            continue
        if path.parent.name in HISTORICAL_RUNS and "classification" not in status:
            status["classification"], status["reason"] = HISTORICAL_RUNS[path.parent.name]
        classification = status.get("classification", "current")
        if classification != "current":
            historical.append({"target": path.parent.parent.name, "suite": path.parent.name, **status})
            continue
        lines.append(f'| {path.parent.parent.name} | {path.parent.name} | {status["exit_code"]} | {classification} |')
        log_path = path.parent / "test.log"
        if log_path.is_file():
            log = log_path.read_text(errors="replace")
            if status["exit_code"]:
                names = re.findall(r"--- FAIL: ([^\s]+)",log)
                failures.append({"target":path.parent.parent.name,"suite":path.parent.name,"failed_tests":names,"log":str(log_path.relative_to(root))})
            for line in log.splitlines():
                if "PERF_JSON " in line:
                    controlled.append(json.loads(line.split("PERF_JSON ",1)[1]))
                elif "GO_MANIFEST_JSON " in line:
                    controlled.append({"target":"go-livepeer","case":"manifest_id_concurrency","result":json.loads(line.split("GO_MANIFEST_JSON ",1)[1]) | {"race_detected":"WARNING: DATA RACE" in log}})
    (root / "controlled-observations.json").write_text(json.dumps(controlled,indent=2)+"\n")
    (root / "failures.json").write_text(json.dumps(failures,indent=2)+"\n")
    (root / "historical-runs.json").write_text(json.dumps(historical,indent=2)+"\n")
    if failures:
        lines += ["", "### Failed cases", ""]
        for failure in failures:
            cases = ", ".join(f'`{name}`' for name in failure["failed_tests"]) or "See the retained log; no completed test case was reported."
            lines.append(f'- **{failure["target"]} / {failure["suite"]}:** {cases} ([log]({failure["log"]})).')
    for observation in controlled:
        if observation["case"] == "manifest_id_concurrency" and observation["result"].get("race_detected"):
            r = observation["result"]
            revision = manifest.get("sources", {}).get("go-livepeer", {}).get("revision", "master")
            lines += ["", "### go-livepeer concurrent ID generation", "", f'The isolated 32-caller race probe confirmed a data race in [the pinned manifest-ID generator](https://github.com/livepeer/go-livepeer/blob/{revision}/common/util.go#L324) and observed {r["duplicates"]} duplicate values in {r["attempts"]} generations. This is consistent with the measured HTTP 409 reservation conflicts; the timing records do not establish a one-to-one causal trace for each conflict. Product source was unchanged; the test ran in an isolated export after timing finished.']
    lines += ["", "### Controlled observations", "", "These deterministic provider and concurrency cases isolate behavior. Frozen-head age rejection is asserted for cached implementations; current Node makes fresh direct RPC reads and does not promise an equivalent freshness-age policy. The cold-burst case reports load rejection as well as provider concurrency. Population timings include the in-process HTTP fixture and race instrumentation and are RPC cost measurements, not production latency estimates.", "", "| Target | Case | Observed result |", "|---|---|---|"]
    for observation in controlled:
        if observation["case"] == "payer_population":
            continue
        lines.append(f'| {observation["target"]} | {observation["case"]} | `{json.dumps(observation["result"], sort_keys=True)}` |')
    lines += ["", "### Payer population RPC cost", "", "Idle is eight advancing-head refreshes; known and churn are 128 reads each. Idle costs are per refresh and other costs per payer read. Batches count as one exchange and retain all contained calls.", "", "| Target | Tracked payers | Workload | Exchanges / operation | Contained calls / operation |", "|---|---:|---|---:|---:|"]
    for observation in controlled:
        if observation["case"] != "payer_population":
            continue
        r = observation["result"]
        lines.append(f'| {observation["target"]} | {r["population"]} | {r["workload"]} | {r["exchanges"]/r["operations"]:.3f} | {r["contained_calls"]/r["operations"]:.3f} |')
    if historical:
        lines += ["", "### Superseded harness runs", "", "These runs used superseded fixtures or preparation conditions. Their raw startup rejections remain evidence of readiness behavior; current scenario outcomes come from the reruns. `historical-runs.json` records each reason.", "", "| Target | Suite | Classification |", "|---|---|---|"]
        for h in historical:
            lines.append(f'| {h["target"]} | {h["suite"]} | {h["classification"]} |')
    current = aggregates.get(("node-current", 110, 1))
    combined = aggregates.get(("node-combined", 110, 1))
    if current and combined and current["p50"] and combined["p50"] is not None:
        comparison = []
        for name in ("node-current", "node-pr1", "node-combined", "go-livepeer"):
            r = aggregates.get((name, 110, 1))
            if r:
                comparison.append(f'**{name}: {fmt(r["p50"])} ms** ({r["successes"]}/{r["attempts"]} successful)')
        lines[4:4] = ["At 110 ms additional RPC latency with one client, median warm dispatch was " + "; ".join(comparison) + ".", ""]
    lines += ["", "## Reproduction and limits", "", "Run `python3 performance/gcp.py --combined-checkout /absolute/path/to/reviewed/checkout --reviewed`. It provisions one VM, builds the pinned source snapshots, deploys the protocol, runs the comparisons, collects results, and deletes the VM. GCP authentication and the reviewed checkout are prerequisites.", "", "The manifest contains binary hashes, Go source hashes for the harness, CPU/platform details, and protocol test parameters. Request records retain actual face values, win probabilities, and ticket counts. Performance uses rare winners; correctness uses frequent winners with ticket face values covering actual devnet gas. The protocol's unlock period is two rounds for bounded withdrawal tests. Local devnet finality exercises application behavior; it does not establish production consensus safety.", ""]
    lines += ["The real-process chain mines at one-second intervals. Different execution-block cadence, catch-up gaps, and event-heavy histories can change observer refresh cost. The population test's constant quiet-head cost does not imply constant cost when every payer changes on-chain.", ""]
    lines += ["Goodput covers the specified finite closed-loop bursts. Longer runs are needed to estimate sustained capacity, especially for the cached paths whose load windows finish quickly.", ""]
    (root / "REPORT.md").write_text("\n".join(lines))


if __name__ == "__main__":
    root = Path(sys.argv[1])
    rows = summarize(root)
    (root / "summary.json").write_text(json.dumps(rows, indent=2) + "\n")
    write_report(root, rows)
    print("| Target | RPC delay | Clients | Repeat | Successes | Dispatch p50 / p95 ms | Success/s | Worker RPC exchanges / calls |")
    print("|---|---:|---:|---:|---:|---:|---:|---:|")
    for row in rows:
        p = row["phases"]["dispatch_ms"]
        fmt = lambda value: f"{value:.3f}" if value is not None else "—"
        rpc = row["rpc"].get("warm", {})
        print(f'| {row["target"]} | {row["delay_ms"]:g} ms | {row["workers"]} | {row["repetition"]} | {row["successes"]}/{row["samples"]} | {fmt(p["p50"])} / {fmt(p["p95"])} | {row["successful_requests_per_second"]:.2f} | {rpc.get("exchanges",0)} / {rpc.get("calls",0)} |')
