#!/usr/bin/env python3
"""Run pinned payment comparisons inside the disposable Linux runtime."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time

import targets

ROOT = Path(os.environ.get("PERF_ROOT", "/work"))


def definition(name):
    return next(t for t in targets.read(ROOT) if t["name"] == name)


def binaries(target):
    binary = ROOT / "bin" / target["name"]
    if target["backend"] == "node":
        return [binary / component for component in ["livepeer", "livepeer-orchestrator", "livepeer-signer", "livepeer-chain"]]
    return [binary, ROOT / "bin/livepeer_cli"] if target["backend"] == "go-livepeer" else []


def command(argv, **kwargs):
    return subprocess.run(argv, check=True, **kwargs)


def manifest():
    frozen = json.loads((ROOT / "manifest.json").read_text())
    sources = {}
    for target in frozen["targets"] + [{"name": "protocol", "backend": "protocol", "repository": "https://github.com/livepeer/protocol.git"}]:
        name = target["name"]
        source = ROOT / "sources" / name
        git = lambda *args: subprocess.check_output(["git", "-c", "safe.directory=" + str(source), "-C", str(source), *args], text=True).strip()
        executables = binaries(target)
        revision = git("rev-parse", "HEAD")
        if target.get("revision", revision) != revision:
            raise SystemExit("Source differs from the frozen target: " + name)
        sources[name] = target | {"revision": revision, "status": git("status", "--porcelain"),
                         "binary_sha256": hashlib.sha256(executables[0].read_bytes()).hexdigest() if executables else None,
                         "binaries_sha256": {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in executables},
                         "binary_go_versions": {str(p.relative_to(ROOT)): subprocess.check_output(["go", "version", str(p)], text=True).strip().split()[-1]
                                                for p in executables}}
    result = frozen | {"sources": sources, "platform": platform.platform(), "cpu_count": os.cpu_count(),
              "go_toolchain": subprocess.check_output(["go","version"],text=True).strip(),
              "ffmpeg_version": subprocess.check_output(["ffmpeg","-version"],text=True).splitlines()[0],
              "ffmpeg_git_commit": "d9751c73e714b01b363483db358b1ea8022c9bea",
              "harness_binary_sha256": hashlib.sha256((ROOT / "bin/scope.test").read_bytes()).hexdigest(),
              "harness_go_sources_sha256": {str(p.relative_to(ROOT / "scope")): hashlib.sha256(p.read_bytes()).hexdigest() for p in (ROOT / "scope").rglob("*.go")},
              "harness_inputs_sha256": {name: hashlib.sha256((ROOT / "scope" / name).read_bytes()).hexdigest()
                                        for name in ["go.mod", "go.sum", "test.env", "testdata/contracts.json", "performance/run.py", "performance/targets.py", "performance/controlled.py", "performance/controlled_test.go.txt"]},
              "clock_ticks_per_second": os.sysconf("SC_CLK_TCK"), "created_at": time.time(),
              "load_model": "closed-loop; no think time; independent manifests",
              "protocol_test_parameters": {"unlock_period_rounds": 2, "block_time_seconds": 1, "slots_per_epoch": 1}}
    cpuinfo = Path("/proc/cpuinfo")
    if cpuinfo.is_file():
        result["cpu_model"] = next((line.split(":", 1)[1].strip() for line in cpuinfo.read_text().splitlines() if line.startswith("model name")), "unknown")
    result["protocol_deployment"] = {name: json.loads((ROOT / "sources/protocol/deployments/gethDev" / (name + ".json")).read_text())["address"]
                                     for name in ["Controller", "TicketBroker", "RoundsManager", "BondingManager"]}
    (ROOT / "manifest.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def environment(target, result):
    backend = definition(target)["backend"]
    node = "node-current" if backend == "go-livepeer" else target
    controller = json.loads((ROOT / "sources/protocol/deployments/gethDev/Controller.json").read_text())["address"]
    maximum = (1 << 256) - 1
    env = os.environ | {
        "E2E_ENV_FILE": str(ROOT / "scope/test.env"), "E2E_MODE": "local",
        "E2E_ARTIFACT_DIR": str(result), "E2E_PERF_TARGET": target, "E2E_PERF_BACKEND": backend,
        "E2E_RPC_URL": "http://127.0.0.1:8545", "E2E_EXPECTED_CHAIN_ID": "54321", "E2E_ETH_CONTROLLER": controller,
        "E2E_NODE_DIR": str(ROOT / "sources" / node), "E2E_NODE_BINARY": str(ROOT / "bin" / node / "livepeer"),
        "GO_LIVEPEER_DIR": str(ROOT / "sources/go-livepeer"), "LIVEPEER_BINARY": str(ROOT / "bin/go-livepeer"),
        "E2E_GO_LIVEPEER_CLI_BINARY": str(ROOT / "bin/livepeer_cli"),
        "E2E_ORCHESTRATORS": backend, "E2E_SIGNERS": backend,
        "E2E_TICKET_FACE_VALUE": "1000000000000000", "E2E_TICKET_WIN_PROB": str((maximum + 99999999) // 100000000),
        "E2E_TICKET_EV": "10000000", "E2E_HTTP_TIMEOUT": "30s", "E2E_READY_TIMEOUT": "90s",
        "E2E_ACTIVATION_TIMEOUT": "5m", "E2E_CLI_TIMEOUT": "3m", "E2E_JOB_TIMEOUT": "60s",
        "E2E_CHAIN_REALTIME": "true", "E2E_BLOCK_TIMEOUT": "5m",
        "E2E_PERF_REVISION": subprocess.check_output(["git","-c","safe.directory="+str(ROOT/"sources"/target),"-C",str(ROOT/"sources"/target),"rev-parse","HEAD"],text=True).strip(),
    }
    names = [t["name"] for t in targets.read(ROOT)]
    env["GIT_CONFIG_COUNT"] = str(len(names))
    for i,name in enumerate(names):
        env[f"GIT_CONFIG_KEY_{i}"] = "safe.directory"
        env[f"GIT_CONFIG_VALUE_{i}"] = str(ROOT / "sources" / name)
    return env


def run_test(target, label, pattern, settings=None, resume=False):
    result = ROOT / "results" / target / label
    result.mkdir(parents=True, exist_ok=True)
    env = environment(target, result) | (settings or {})
    identity = {"revision": env["E2E_PERF_REVISION"], "pattern": pattern, "settings": settings or {},
                "binaries_sha256": {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in binaries(definition(target))},
                "harness_sha256": hashlib.sha256((ROOT / "bin/scope.test").read_bytes()).hexdigest()}
    status_path = result / "exit.json"
    if resume and status_path.is_file():
        previous = json.loads(status_path.read_text())
        if previous.get("identity") != identity:
            raise SystemExit("Refusing to reuse results with different inputs: " + str(result) + "; use a fresh results directory")
        return previous["exit_code"]
    start = time.time()
    with (result / "test.log").open("w") as out:
        completed = subprocess.run([str(ROOT / "bin/scope.test"), "-test.v", "-test.count=1", "-test.timeout=30m", "-test.run=" + pattern],
                                   cwd=ROOT / "scope", env=env, stdout=out, stderr=subprocess.STDOUT, timeout=1900)
    status = {"target": target, "label": label, "pattern": pattern, "exit_code": completed.returncode,
              "started_at": start, "finished_at": time.time(), "elapsed_seconds": time.time() - start,
              "settings": settings or {}, "source_revision": env["E2E_PERF_REVISION"], "identity": identity}
    (result / "exit.json").write_text(json.dumps(status, indent=2) + "\n")
    print(json.dumps(status), flush=True)
    return completed.returncode


def matrix(args):
    selected = args.targets.split(",")
    plan = {"targets": selected, "repetitions": 1 if args.quick else 3, "delays_ms": [0, 110], "workers": [1, 32]}
    plan["expected_cells"] = len(selected) * plan["repetitions"] * len(plan["delays_ms"]) * len(plan["workers"])
    plan_path = ROOT / "results" / ("pilot-matrix.json" if args.quick else "matrix.json")
    if plan_path.exists() and json.loads(plan_path.read_text()) != plan:
        raise SystemExit("Matrix selection changed; use a fresh results directory")
    plan_path.write_text(json.dumps(plan, indent=2) + "\n")
    shared = {"E2E_PERF_RECIPIENT_DIR": str(ROOT / "private/performance-recipient")}
    if run_test(selected[0], "prepare-recipient", "^TestPerformancePrepareRecipient$", shared):
        raise SystemExit("Shared performance recipient setup failed")
    for repeat in range(plan["repetitions"]):
        ordered = selected[repeat % len(selected):] + selected[:repeat % len(selected)]
        for delay in [0, 110]:
            for workers in [1, 32]:
                for target in ordered:
                    label = ("pilot-" if args.quick else "") + f"delay-{delay}-workers-{workers}-repeat-{repeat}"
                    run_test(target, label, "^TestPaymentPerformance$", shared | {
                        "E2E_PERF_WORKERS": str(workers), "E2E_PERF_SAMPLES": str((8 if workers == 1 else 64) if args.quick else (128 if workers == 1 else 1024)),
                        "E2E_PERF_DELAY": f"{delay}ms", "E2E_PERF_REPETITION": str(repeat),
                        "E2E_PERF_IDLE": "60s" if not args.quick and repeat == 0 and workers == 1 else "0s"}, resume=True)


def correctness(args):
    # Keep validation builds outside the timing matrix. This also permits a
    # fixture-only correction after timing has started without replacing a
    # running test executable.
    command(["go", "test", "-c", "-o", str(ROOT / "bin/scope.test"), "."], cwd=ROOT / "scope")
    (ROOT / "results/validation-harness.json").write_text(json.dumps({
        "binary_sha256": hashlib.sha256((ROOT / "bin/scope.test").read_bytes()).hexdigest(),
        "go_sources_sha256": {str(p.relative_to(ROOT / "scope")): hashlib.sha256(p.read_bytes()).hexdigest() for p in (ROOT / "scope").rglob("*.go")},
    }, indent=2) + "\n")
    # These public protocol cases use real child processes and require no SDK,
    # media, Kafka, or externally selected provider.
    cases = "identity|challenge_and_admission|malformed_signer_requests|state_continuation_and_binding|bad_payment_credentials|ticket_parameter_validation|batch_count_and_exposure|unfunded_signer|recipient_ticket_rejection|concurrent_admission_and_replay|concurrent_independent_jobs|funding"
    # Correctness deliberately uses frequent winners and a face value covering
    # the devnet's actual redemption gas. Timing uses separate low-odds settings.
    settings = {"E2E_TICKET_FACE_VALUE": "400000000000", "E2E_TICKET_WIN_PROB": str(((1 << 256) - 1) // 2), "E2E_TICKET_EV": "200000000000"}
    recovery = settings | {"E2E_TICKET_FACE_VALUE":"1000000000000000", "E2E_TICKET_EV":"500000000000000", "E2E_SIGNER_MAX_TICKET_EV":"3000000000000000", "E2E_SIGNER_MAX_BATCH_EV":"20000000000000000"}
    for target in args.targets.split(","):
        backend = definition(target)["backend"]
        if run_test(target, "prerequisites", "^TestPerformanceChainPrerequisites$"):
            raise SystemExit("Disposable chain prerequisites failed for " + target)
        if target == "go-livepeer":
            continue
        if not args.recovery_only:
            run_test(target, "protocol-correctness", f"^TestOnchainPayments$/{backend}$/{backend}$/same-wallet$/({cases})", settings)
        run_test(target, "recovery-correctness", f"^TestOnchainRecovery$/{backend}$/{backend}$/same-wallet$/(redemption_and_duplicate|pending_restart|RPC_interruption|delayed_finality|reorg_recovery|expired_parameters)$", recovery)


def validate(args):
    for target in args.targets.split(","):
        if target == "go-livepeer":
            continue
        result = ROOT / "results" / target / "repository-race"
        result.mkdir(parents=True, exist_ok=True)
        source = ROOT / "sources" / target
        command(["git", "config", "--global", "--add", "safe.directory", str(source)])
        with (result / "test.log").open("w") as out:
            status = subprocess.run(["go", "test", "-race", "./...", "-count=1", "-timeout=5m"], cwd=source,
                                    stdout=out, stderr=subprocess.STDOUT, timeout=1200)
        (result / "exit.json").write_text(json.dumps({"exit_code": status.returncode}))
        print(json.dumps({"target": target, "repository_race_exit_code": status.returncode}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["manifest", "matrix", "correctness", "validate", "smoke"])
    parser.add_argument("--targets", help="Comma-separated subset of configured targets (default: driver selection)")
    parser.add_argument("--quick", action="store_true", help="Small pilot cells, excluded from the formal report")
    parser.add_argument("--recovery-only", action="store_true", help="For correctness, rerun only recovery cases")
    args = parser.parse_args()
    frozen = json.loads((ROOT / "manifest.json").read_text())
    selection = args.targets
    if selection is None and "selected_targets" in frozen:
        selection = ",".join(frozen["selected_targets"])
    try:
        args.targets = ",".join(targets.select(frozen["targets"], selection))
    except ValueError as error:
        parser.error(str(error))
    if args.action == "manifest":
        manifest()
    elif args.action == "matrix":
        matrix(args)
    elif args.action == "correctness":
        correctness(args)
    elif args.action == "validate":
        validate(args)
    else:
        failed = False
        for target in args.targets.split(","):
            failed |= bool(run_test(target, "smoke", "^TestPaymentPerformance$", {"E2E_PERF_SAMPLES": "2"}))
        sys.exit(1 if failed else 0)
