#!/usr/bin/env python3
"""Node target configuration; resolved inputs extend the existing run manifest."""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

NODE_REPOSITORY = "https://github.com/livepeer/node.git"
FIXED = [
    {"name": "node-current", "backend": "node", "repository": NODE_REPOSITORY, "controlled": "direct"},
    {"name": "go-livepeer", "backend": "go-livepeer", "repository": "https://github.com/livepeer/go-livepeer.git"},
]
CONTROLLED = {"auto", "direct", "chain-view", "payment-observer", "none"}


def configured(settings):
    targets = [dict(FIXED[0])]
    names = settings.get("E2E_PERF_NODE_TARGETS", "").strip()
    for name in names.split(",") if names else []:
        name = name.strip()
        if not re.fullmatch(r"[a-z][a-z0-9]*(?:-[a-z0-9]+)*", name) or name in {"node-current", "go-livepeer", "protocol"}:
            raise ValueError("Invalid or reserved Node target name: " + repr(name))
        if any(t["name"] == name for t in targets):
            raise ValueError("Duplicate Node target: " + name)
        prefix = "E2E_PERF_NODE_" + name.upper().replace("-", "_") + "_"
        checkout = settings.get(prefix + "CHECKOUT", "")
        ref = settings.get(prefix + "REF", "HEAD" if checkout else "")
        if not ref or ref.startswith("-"):
            raise ValueError(prefix + "REF must identify a commit, branch, or pull-request ref")
        adapter = settings.get(prefix + "CONTROLLED", "auto")
        if adapter not in CONTROLLED:
            raise ValueError(prefix + "CONTROLLED must be one of " + ", ".join(sorted(CONTROLLED)))
        target = {"name": name, "backend": "node", "repository": settings.get(prefix + "REPO") or NODE_REPOSITORY,
                  "ref": ref, "controlled": adapter}
        if checkout:
            target["checkout"] = str(Path(checkout).expanduser().resolve())
        targets.append(target)
    return targets + [dict(FIXED[1])]


def load_configuration():
    root = Path(__file__).resolve().parent.parent
    env = dict(os.environ)
    env["E2E_ENV_FILE"] = str(Path(env.get("E2E_ENV_FILE") or root / "test.env").resolve())
    settings = json.loads(subprocess.check_output(
        [env.get("E2E_GO_BINARY", "go"), "run", "./cmd/e2e", "performance-config"], cwd=root, env=env, text=True))
    configuration = {"targets": configured(settings)}
    if settings.get("E2E_PERF_CONTROLLED_FIXTURE"):
        configuration["controlled_fixture"] = str(Path(settings["E2E_PERF_CONTROLLED_FIXTURE"]).expanduser().resolve())
    return configuration


def select(targets, value=None):
    names = [t["name"] for t in targets]
    selected = [name.strip() for name in value.split(",")] if value is not None else names
    if not selected or len(set(selected)) != len(selected) or not set(selected) <= set(names):
        raise ValueError("targets must be a nonempty comma-separated subset of " + ",".join(names))
    return selected


def git(source, *args):
    return subprocess.check_output(["git", "-c", "safe.directory=" + str(source), "-C", str(source), *args], text=True).strip()


def bundle(target, output):
    """Freeze each configured ref before provisioning, without modifying checkouts."""
    checkout = target.get("checkout")
    repository, ref = target["repository"], target["ref"]
    if checkout:
        if git(checkout, "status", "--porcelain"):
            raise ValueError("Node checkout must be clean: " + checkout)
        ref = git(checkout, "rev-parse", "--verify", ref + "^{commit}")
        repository = checkout
        origin = subprocess.run(["git", "-C", checkout, "remote", "get-url", "origin"], text=True, capture_output=True)
        target["repository"] = origin.stdout.strip() if origin.returncode == 0 else checkout
    with tempfile.TemporaryDirectory(prefix="payment-perf-source-") as folder:
        subprocess.run(["git", "init", "--bare", "--quiet", folder], check=True)
        subprocess.run(["git", "-C", folder, "fetch", "--no-tags", "--", repository, ref], check=True)
        revision = git(folder, "rev-parse", "FETCH_HEAD^{commit}")
        subprocess.run(["git", "-C", folder, "update-ref", "refs/heads/performance", revision], check=True)
        subprocess.run(["git", "-C", folder, "bundle", "create", str(output), "--all"], check=True)
    return {key: value for key, value in target.items() if key != "checkout"} | {"revision": revision}


def read(root):
    return json.loads((root / "manifest.json").read_text())["targets"]


def prepare(root):
    for target in read(root):
        if "revision" not in target:
            continue  # Fixed baselines are pinned by bootstrap.sh.
        source = root / "sources" / target["name"]
        if source.exists():
            if git(source, "rev-parse", "HEAD") != target["revision"] or git(source, "status", "--porcelain"):
                raise ValueError("Existing source differs from the frozen target: " + str(source))
            continue
        subprocess.run(["git", "clone", "--no-checkout", str(root / (target["name"] + ".bundle")), str(source)], check=True)
        subprocess.run(["git", "-C", str(source), "checkout", "--detach", target["revision"]], check=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["prepare", "names"])
    parser.add_argument("--backend", choices=["node", "go-livepeer"])
    args = parser.parse_args()
    root = Path(os.environ.get("PERF_ROOT", "/work"))
    if args.action == "prepare":
        prepare(root)
    else:
        for target in read(root):
            if args.backend is None or target["backend"] == args.backend:
                print(target["name"])
