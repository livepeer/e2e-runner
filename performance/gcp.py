#!/usr/bin/env python3
"""Provision, run, collect, and delete a reproducible payment comparison VM."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import shlex
import subprocess
import tarfile
import time


def run(argv, **kwargs):
    return subprocess.run(argv, check=True, **kwargs)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--combined-checkout", type=Path, required=True)
    parser.add_argument("--reviewed", action="store_true", required=True, help="The combined checkout has completed its final review")
    parser.add_argument("--project", default="livepeer-simple-infra")
    parser.add_argument("--zone", default="us-west1-b")
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    vm = "payment-perf-" + stamp
    output = root / "artifacts" / vm
    output.mkdir(parents=True)
    shared = ["--project=" + args.project, "--zone=" + args.zone, "--quiet"]
    ssh = lambda text: run(["gcloud", "compute", "ssh", vm, *shared, "--command=" + text])
    copy = lambda source, target: run(["gcloud", "compute", "scp", str(source), target, *shared])
    status = subprocess.check_output(["git", "-C", str(args.combined_checkout), "status", "--porcelain"], text=True)
    if status:
        raise SystemExit("Combined checkout must be clean")
    bundle = output / "node.bundle"
    run(["git", "-C", str(args.combined_checkout), "bundle", "create", str(bundle), "HEAD", "refs/remotes/blueclaw-review/pr-1-original"])
    tracked = subprocess.check_output(["git", "-C", str(root), "ls-files", "-z"]).decode().split("\0")
    files = set(filter(None, tracked)) | {"performance_test.go"}
    files.update(str(p.relative_to(root)) for p in (root / "performance").glob("*") if p.is_file())
    archive = output / "scope.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        for name in sorted(files):
            if (root / name).is_file():
                tar.add(root / name, arcname="scope/" + name)
    created = False
    try:
        run(["gcloud", "compute", "instances", "create", vm, *shared, "--machine-type=n2-standard-8",
             "--image-family=ubuntu-2404-lts-amd64", "--image-project=ubuntu-os-cloud", "--boot-disk-size=100GB",
             "--no-service-account", "--no-scopes", "--max-run-duration=8h", "--instance-termination-action=DELETE",
             "--labels=purpose=payment-performance"])
        created = True
        for attempt in range(30):
            try:
                ssh("true")
                break
            except subprocess.CalledProcessError:
                if attempt == 29:
                    raise
                time.sleep(5)
        ssh('sudo apt-get update -qq && sudo apt-get install -y -qq docker.io git python3 && sudo usermod -aG docker "$USER" && mkdir -p /tmp/payment-perf')
        copy(bundle, vm + ":/tmp/payment-perf/node.bundle")
        copy(archive, vm + ":/tmp/payment-perf/scope.tar.gz")
        ssh("cd /tmp/payment-perf && tar -xzf scope.tar.gz && bash scope/performance/bootstrap.sh >bootstrap.log 2>&1")
        ssh("python3 -c " + shlex.quote("import json,pathlib;p=pathlib.Path('/tmp/payment-perf/manifest.json');v=json.loads(p.read_text());v['combined_review_complete']=True;p.write_text(json.dumps(v,indent=2))"))
        container = "docker run --rm --network host -v /tmp/payment-perf:/work -v payment-perf-modules:/go/pkg/mod -v payment-perf-build:/root/.cache/go-build payment-perf-runtime "
        ssh(container + "python3 /work/scope/performance/run.py correctness")
        ssh(container + "python3 /work/scope/performance/controlled.py")
        ssh(container + "python3 /work/scope/performance/run.py validate")
        ssh(container + "python3 /work/scope/performance/diagnose_go_manifest.py")
        ssh(container + "python3 /work/scope/performance/run.py matrix")
        ssh("python3 /tmp/payment-perf/scope/performance/report.py /tmp/payment-perf/results > /tmp/payment-perf/results/table.md")
    finally:
        if created:
            try:
                ssh("cd /tmp/payment-perf && sudo tar -czf results.tar.gz --ignore-failed-read results manifest.json bootstrap.log")
                copy(vm + ":/tmp/payment-perf/results.tar.gz", str(output / "results.tar.gz"))
            finally:
                run(["gcloud", "compute", "instances", "delete", vm, *shared])
        bundle.unlink(missing_ok=True)
        archive.unlink(missing_ok=True)
        print("Payment comparison artifacts:", output)


if __name__ == "__main__":
    main()
