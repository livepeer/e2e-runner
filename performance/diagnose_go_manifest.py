#!/usr/bin/env python3
"""Probe the actual Go manifest RNG in an isolated, test-only source clone."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

root = Path(os.environ.get("PERF_ROOT", "/work"))
source = root / "sources/go-livepeer"
clone = root / "controlled/go-manifest-race"
result = root / "results/go-livepeer/manifest-id-race"
for path in (source, source / ".git"):
    subprocess.run(["git", "config", "--global", "--add", "safe.directory", str(path)], check=True)
revision = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip()
clone.parent.mkdir(exist_ok=True)
if clone.exists():
    shutil.rmtree(clone)
# Export only the pinned tree: a partial/shallow source can contain advertised
# history whose promisor objects are intentionally absent from this VM.
clone.mkdir()
archive = subprocess.Popen(["git", "-C", str(source), "archive", revision], stdout=subprocess.PIPE)
subprocess.run(["tar", "-xf", "-", "-C", str(clone)], stdin=archive.stdout, check=True)
archive.stdout.close()
if archive.wait():
    raise SystemExit("Pinned source export failed")
test = clone / "common/performance_manifest_test.go"
test.write_text('''package common

import (
    "encoding/json"
    "sync"
    "testing"
)

func TestPerformanceManifestIDConcurrency(t *testing.T) {
    const workers, perWorker = 32, 1024
    start := make(chan struct{})
    ids := make(chan string, workers*perWorker)
    var wg sync.WaitGroup
    for w := 0; w < workers; w++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            <-start
            for i := 0; i < perWorker; i++ { ids <- RandomIDGenerator(4) }
        }()
    }
    close(start)
    wg.Wait()
    close(ids)
    seen, duplicates := make(map[string]bool), 0
    for id := range ids { if seen[id] { duplicates++ }; seen[id] = true }
    data, err := json.Marshal(map[string]int{"attempts":workers*perWorker,"unique":len(seen),"duplicates":duplicates})
    if err != nil { t.Fatal(err) }
    t.Log("GO_MANIFEST_JSON " + string(data))
}
''')
subprocess.run(["gofmt", "-w", str(test)], check=True)
result.mkdir(parents=True, exist_ok=True)
(result / "overlay.json").write_text(json.dumps({"source_revision": revision, "test_sha256": hashlib.sha256(test.read_bytes()).hexdigest()}, indent=2) + "\n")
started = time.time()
with (result / "test.log").open("w") as out:
    run = subprocess.run(["go", "test", "-p=2", "-race", "./common", "-run", "^TestPerformanceManifestIDConcurrency$", "-count=1", "-timeout=90s", "-v"],
                         cwd=clone, env=os.environ | {"GOMAXPROCS": "2"}, stdout=out, stderr=subprocess.STDOUT, timeout=600)
status = {"exit_code": run.returncode, "source_revision": revision, "elapsed_seconds": time.time() - started}
(result / "exit.json").write_text(json.dumps(status, indent=2) + "\n")
print(json.dumps(status), flush=True)
