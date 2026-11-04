#!/usr/bin/env python3
"""Install one shared fixture/test overlay in disposable clones of Node targets."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

root = Path(os.environ.get("PERF_ROOT", "/work"))
template = (root / "scope/performance/controlled_test.go.txt").read_text()
fixture = root / "sources/node-combined/internal/test/chainrpc.go"
adapters = {
    "node-current": "",
    "node-pr1": """view,err:=NewChainView(direct,ChainViewOptions{Interval:time.Second,MaxAge:4*time.Second,Recipients:[]common.Address{c.recipient},Now:func()time.Time{return now}});require.NoError(t,err)
 c.snapshot,c.funds,c.active,c.refresh=view.Snapshot,view.PayerFunds,view.IsActiveAt,view.Refresh""",
    "node-combined": """observer:=NewPaymentObserver(direct,false,c.recipient);observer.maxAge=4*time.Second;observer.now=func()time.Time{return now};observer.lifecycle=context.Background();t.Cleanup(observer.payerReads.Wait)
 observed:=direct;observed.Observer=observer
 c.snapshot,c.funds,c.active,c.refresh=observed.Snapshot,observed.SenderInfo,observed.IsActiveAt,observer.refresh""",
}
statuses = []
for target in sys.argv[1:] or list(adapters):
    if target not in adapters:
        raise SystemExit("Unknown Node target: " + target)
    source = root / "sources" / target
    clone = root / "controlled" / target
    clone.parent.mkdir(exist_ok=True)
    if clone.exists():
        shutil.rmtree(clone)
    # Git's local upload-pack is a separate process and does not inherit -c
    # trust settings. Trust only these two exact paths in this disposable
    # container, never a wildcard or a host user's global configuration.
    for trusted in (source, source / ".git"):
        subprocess.run(["git", "config", "--global", "--add", "safe.directory", str(trusted)], check=True)
    subprocess.run(["git", "-c", "safe.directory=" + str(source), "-c", "safe.directory=" + str(source / ".git"), "clone", "--shared", str(source), str(clone)], check=True)
    folder = clone / "internal/performancefixture"
    folder.mkdir()
    shutil.copy2(fixture, folder / "chainrpc.go")
    text = template.replace("__ADAPTER__", adapters[target]).replace("__TARGET__", target)
    if target == "node-combined":
        # The combined stack removed the duplicate public SenderInfo adapter.
        text = text.replace("direct.SenderInfo", "direct.PayerFunds").replace("observed.SenderInfo", "observed.PayerFunds")
    text = text.replace("__CACHED__", str(target != "node-current").lower())
    text = text.replace("__FROZEN_CLOCK__", "nowAdvance := func(){}; _ = nowAdvance" if target == "node-current" else "c.elapse(5*time.Second)")
    # The service clock advances independently of the mocked provider's head.
    text = text.replace(" advance func()", " elapse func(time.Duration)\n advance func()")
    text = text.replace(" c.advance=func()", " c.elapse=func(d time.Duration){now=now.Add(d)}\n c.advance=func()")
    (clone / "eth/performance_comparison_test.go").write_text(text)
    result = root / "results" / target / "controlled"
    result.mkdir(parents=True, exist_ok=True)
    (result / "overlay.json").write_text(json.dumps({"fixture_sha256": hashlib.sha256(fixture.read_bytes()).hexdigest(),
        "test_sha256": hashlib.sha256(text.encode()).hexdigest(), "source_revision": subprocess.check_output(["git", "-c", "safe.directory=" + str(source), "-C", str(source), "rev-parse", "HEAD"], text=True).strip()}, indent=2))
    with (result / "test.log").open("w") as out:
        status = subprocess.run(["go", "test", "-race", "./eth", "-run", "^TestComparison", "-count=1", "-timeout=5m", "-v"], cwd=clone, stdout=out, stderr=subprocess.STDOUT)
    (result / "exit.json").write_text(json.dumps({"exit_code": status.returncode}))
    statuses.append({"target": target, "exit_code": status.returncode})
print(json.dumps(statuses), flush=True)
