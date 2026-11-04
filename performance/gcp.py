#!/usr/bin/env python3
"""Provision, run, collect, and delete a reproducible payment comparison VM."""
import argparse
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import shlex
import subprocess
import tarfile
import time

import targets


def run(argv, **kwargs):
    return subprocess.run(argv, check=True, **kwargs)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--targets", help="Comma-separated subset of the configured targets (default: all)")
    parser.add_argument("--project", default="livepeer-simple-infra")
    parser.add_argument("--zone", default="us-west1-b")
    args = parser.parse_args()
    try:
        settings = targets.load_configuration()
        configuration = settings["targets"]
        selected = targets.select(configuration, args.targets)
    except ValueError as error:
        parser.error(str(error))
    root = Path(__file__).resolve().parent.parent
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    vm = "payment-perf-" + stamp
    output = root / "artifacts" / vm
    output.mkdir(parents=True)
    shared = ["--project=" + args.project, "--zone=" + args.zone, "--quiet"]
    ssh = lambda text: run(["gcloud", "compute", "ssh", vm, *shared, "--command=" + text])
    copy = lambda source, target: run(["gcloud", "compute", "scp", str(source), target, *shared])
    # node-current is also needed by the go-livepeer test fixture. Only selected
    # configurable sources are resolved, uploaded, and built.
    configuration = [t for t in configuration if t["name"] in selected or t["name"] in {"node-current", "go-livepeer"}]
    bundles = []
    for index, target in enumerate(configuration):
        if "ref" in target:
            path = output / (target["name"] + ".bundle")
            configuration[index] = targets.bundle(target, path)
            bundles.append(path)
    frozen = {"targets": configuration, "selected_targets": selected}
    fixture = None
    if settings.get("controlled_fixture"):
        fixture = Path(settings["controlled_fixture"]).read_bytes()
        frozen["controlled_fixture"] = {"path": "controlled-fixture.go", "sha256": hashlib.sha256(fixture).hexdigest()}
    manifest = output / "manifest.json"
    manifest.write_text(json.dumps(frozen, indent=2) + "\n")
    tracked = subprocess.check_output(["git", "-C", str(root), "ls-files", "-z"]).decode().split("\0")
    files = set(filter(None, tracked)) | {"performance_test.go"}
    files.update(str(p.relative_to(root)) for p in (root / "performance").glob("*") if p.is_file())
    archive = output / "scope.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        for name in sorted(files):
            if (root / name).is_file():
                tar.add(root / name, arcname="scope/" + name)
        if fixture is not None:
            entry = tarfile.TarInfo("controlled-fixture.go")
            entry.size = len(fixture)
            entry.mode = 0o444
            tar.addfile(entry, io.BytesIO(fixture))
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
        copy(manifest, vm + ":/tmp/payment-perf/manifest.json")
        for bundle in bundles:
            copy(bundle, vm + ":/tmp/payment-perf/" + bundle.name)
        copy(archive, vm + ":/tmp/payment-perf/scope.tar.gz")
        ssh("cd /tmp/payment-perf && tar -xzf scope.tar.gz && bash scope/performance/bootstrap.sh >bootstrap.log 2>&1")
        container = "docker run --rm --network host -v /tmp/payment-perf:/work -v payment-perf-modules:/go/pkg/mod -v payment-perf-build:/root/.cache/go-build payment-perf-runtime "
        selection = " --targets=" + shlex.quote(",".join(selected))
        ssh(container + "python3 /work/scope/performance/run.py correctness" + selection)
        ssh(container + "python3 /work/scope/performance/controlled.py" + selection)
        ssh(container + "python3 /work/scope/performance/run.py validate" + selection)
        if "go-livepeer" in selected:
            ssh(container + "python3 /work/scope/performance/diagnose_go_manifest.py")
        ssh(container + "python3 /work/scope/performance/run.py matrix" + selection)
        ssh("python3 /tmp/payment-perf/scope/performance/report.py /tmp/payment-perf/results > /tmp/payment-perf/results/table.md")
    finally:
        if created:
            try:
                ssh("cd /tmp/payment-perf && sudo tar -czf results.tar.gz --ignore-failed-read results manifest.json bootstrap.log")
                copy(vm + ":/tmp/payment-perf/results.tar.gz", str(output / "results.tar.gz"))
            finally:
                run(["gcloud", "compute", "instances", "delete", vm, *shared])
        for bundle in bundles:
            bundle.unlink(missing_ok=True)
        archive.unlink(missing_ok=True)
        print("Payment comparison artifacts:", output)


if __name__ == "__main__":
    main()
