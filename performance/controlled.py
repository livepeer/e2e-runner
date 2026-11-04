#!/usr/bin/env python3
"""Install shared controlled overlays in disposable clones of configured Node builds."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess

import targets

ADAPTERS = {
    "direct": "",
    "chain-view": """view,err:=NewChainView(direct,ChainViewOptions{Interval:time.Second,MaxAge:4*time.Second,Recipients:[]common.Address{c.recipient},Now:func()time.Time{return now}});require.NoError(t,err)
 c.snapshot,c.funds,c.active,c.refresh=view.Snapshot,view.PayerFunds,view.IsActiveAt,view.Refresh""",
    "payment-observer": """observer:=NewPaymentObserver(direct,false,c.recipient);observer.maxAge=4*time.Second;observer.now=func()time.Time{return now};observer.lifecycle=context.Background();t.Cleanup(observer.payerReads.Wait)
 observed:=direct;observed.Observer=observer
 c.snapshot,c.funds,c.active,c.refresh=observed.Snapshot,observed.SenderInfo,observed.IsActiveAt,observer.refresh""",
}


def adapter_for(source, requested):
    code = "\n".join(p.read_text() for p in (source / "eth").glob("*.go") if not p.name.endswith("_test.go"))
    if requested != "auto":
        return requested, code
    if "func NewPaymentObserver(" in code:
        return "payment-observer", code
    if "func NewChainView(" in code:
        return "chain-view", code
    if re.search(r"func \(\w+ PaymentChain\) (SenderInfo|PayerFunds)\(", code):
        return "direct", code
    return "none", code


def overlay(template, target, adapter, code):
    text = template.replace("__ADAPTER__", ADAPTERS[adapter]).replace("__TARGET__", target)
    if not re.search(r"func \(\w+ PaymentChain\) SenderInfo\(", code):
        text = text.replace("direct.SenderInfo", "direct.PayerFunds").replace("observed.SenderInfo", "observed.PayerFunds")
    text = text.replace("__CACHED__", str(adapter != "direct").lower())
    text = text.replace("__BOUNDED_READS__", str(adapter == "payment-observer").lower())
    text = text.replace("__FROZEN_CLOCK__", "nowAdvance := func(){}; _ = nowAdvance" if adapter == "direct" else "c.elapse(5*time.Second)")
    text = text.replace(" advance func()", " elapse func(time.Duration)\n advance func()")
    return text.replace(" c.advance=func()", " c.elapse=func(d time.Duration){now=now.Add(d)}\n c.advance=func()")


def run(root, selected):
    template = (root / "scope/performance/controlled_test.go.txt").read_text()
    configuration = json.loads((root / "manifest.json").read_text())
    supplied = configuration.get("controlled_fixture")
    fixture = root / supplied["path"] if supplied else None
    if fixture is not None and hashlib.sha256(fixture.read_bytes()).hexdigest() != supplied["sha256"]:
        raise ValueError("Controlled fixture differs from the frozen input")
    statuses = []
    for target in targets.read(root):
        name = target["name"]
        if name not in selected or target["backend"] != "node":
            continue
        source = root / "sources" / name
        result = root / "results" / name / "controlled"
        result.mkdir(parents=True, exist_ok=True)
        adapter, code = adapter_for(source, target.get("controlled", "auto"))
        if fixture is None or adapter == "none":
            status = {"target": name, "exit_code": None, "skipped": True,
                      "reason": ("No external controlled fixture configured" if fixture is None else "Controlled overlay disabled or no supported private API")
                                + "; black-box correctness and timing still run"}
        else:
            clone = root / "controlled" / name
            clone.parent.mkdir(exist_ok=True)
            if clone.exists():
                shutil.rmtree(clone)
            # Local upload-pack is separate: trust only these exact paths in
            # the disposable container, never a wildcard or host Git settings.
            for trusted in (source, source / ".git"):
                subprocess.run(["git", "config", "--global", "--add", "safe.directory", str(trusted)], check=True)
            subprocess.run(["git", "-c", "safe.directory=" + str(source), "-c", "safe.directory=" + str(source / ".git"), "clone", "--shared", str(source), str(clone)], check=True)
            folder = clone / "internal/performancefixture"
            folder.mkdir()
            shutil.copy2(fixture, folder / "chainrpc.go")
            text = overlay(template, name, adapter, code)
            (clone / "eth/performance_comparison_test.go").write_text(text)
            (result / "overlay.json").write_text(json.dumps({"adapter": adapter, "fixture_sha256": hashlib.sha256(fixture.read_bytes()).hexdigest(),
                "test_sha256": hashlib.sha256(text.encode()).hexdigest(), "source_revision": targets.git(source, "rev-parse", "HEAD")}, indent=2))
            with (result / "test.log").open("w") as out:
                completed = subprocess.run(["go", "test", "-race", "./eth", "-run", "^TestComparison", "-count=1", "-timeout=5m", "-v"], cwd=clone, stdout=out, stderr=subprocess.STDOUT)
            status = {"target": name, "exit_code": completed.returncode, "adapter": adapter}
        (result / "exit.json").write_text(json.dumps(status, indent=2) + "\n")
        statuses.append(status)
    print(json.dumps(statuses), flush=True)


if __name__ == "__main__":
    root = Path(os.environ.get("PERF_ROOT", "/work"))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--targets", help="Comma-separated subset of configured targets (default: driver selection)")
    args = parser.parse_args()
    frozen = json.loads((root / "manifest.json").read_text())
    selection = args.targets
    if selection is None and "selected_targets" in frozen:
        selection = ",".join(frozen["selected_targets"])
    try:
        selected = targets.select(frozen["targets"], selection)
    except ValueError as error:
        parser.error(str(error))
    run(root, selected)
