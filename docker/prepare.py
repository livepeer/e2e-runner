"""Snapshot inputs without changing upstream checkouts or copying ignored secrets."""
import json
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import sys

root = Path(sys.argv[1]).resolve()
root.mkdir(parents=True, exist_ok=True)
(root / "tmp").mkdir(mode=0o700)
(root / "artifacts").mkdir()
env = {k: v for k, v in os.environ.items() if k.startswith("E2E_")}
provenance = {}
excluded_dirs = {"artifacts", ".git", ".gocache", ".codex-gocache", ".codex-gomodcache",
                 ".gomodcache", ".venv", "node_modules", "__pycache__"}

def snapshot(source, name):
    source = Path(source).resolve()
    target = root / name
    if (source / ".git").exists():
        files = subprocess.check_output(
            ["git", "-C", str(source), "ls-files", "-z", "--cached", "--others", "--exclude-standard"]
        ).decode().split("\0")
        info = {
            "source": str(source),
            "revision": subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"]).decode().strip(),
            "status": subprocess.check_output(["git", "-C", str(source), "status", "--short"]).decode(),
        }
    else:
        # A VM can consume an exported snapshot with the original provenance.
        info = json.loads((source / ".e2e-source.json").read_text())
        files = []
        for directory, dirs, names in os.walk(source):
            dirs[:] = [name for name in dirs if name not in excluded_dirs]
            files.extend(str((Path(directory) / name).relative_to(source)) for name in names)
    fingerprint = hashlib.sha256()
    for filename in sorted(filter(None, files)):
        if filename == ".e2e-source.json":
            continue
        if any(part in excluded_dirs or part.startswith("._") for part in Path(filename).parts):
            continue
        src, dst = source / filename, target / filename
        if src.is_file():
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
            fingerprint.update(filename.encode() + b"\0")
            fingerprint.update(hashlib.sha256(src.read_bytes()).digest())
    info["snapshot_sha256"] = fingerprint.hexdigest()
    provenance[name] = info
    (target / ".e2e-source.json").write_text(json.dumps(info, indent=2))
    return str(target)

env["E2E_RUN_ROOT"] = str(root)
snapshot(Path.cwd(), "scope")
for key, name in [("GO_LIVEPEER_DIR", "go-livepeer"), ("E2E_NODE_DIR", "node"),
                  ("E2E_CLEARINGHOUSE_DIR", "clearinghouse"), ("PYTHON_RUNNER_DIR", "python-runner")]:
    if not os.environ.get(key):
        raise SystemExit(f"{key} must point to a source checkout")
    env[key] = snapshot(os.environ[key], name)
if os.environ.get("E2E_GOLANG_RUNNER_DIR"):
    env["E2E_GOLANG_RUNNER_DIR"] = snapshot(os.environ["E2E_GOLANG_RUNNER_DIR"], "golang-runner")
# Accommodate go-livepeer's local lpms replacement. Other relative replacements
# fail at build rather than silently resolving to a different source revision.
lpms = os.environ.get("E2E_LPMS_DIR")
if not lpms:
    sibling = Path(os.environ["GO_LIVEPEER_DIR"]).resolve().parent / "lpms"
    if sibling.is_dir():
        lpms = str(sibling)
if lpms:
    snapshot(lpms, "lpms")
external_settings = {"E2E_RPC_URL", "E2E_ETH_CONTROLLER", "E2E_EXPECTED_CHAIN_ID", "E2E_PRICE_FEED_ADDRESS",
                     "E2E_FUNDER_ADDRESS", "E2E_LPT_FUNDER_ADDRESS", "E2E_FUNDER_KEY_FILE", "E2E_BOOTSTRAP_SECRET_FILE"}
for key in external_settings:
    env.pop(key, None)
env.update(E2E_MODE="compose", E2E_GO_BINARY="go", E2E_ENV_FILE=str(root / "scope/test.env"),
           E2E_ARTIFACT_DIR=str(root / "artifacts"), TMPDIR=str(root / "tmp"),
           LIVEPEER_BINARY=str(root / "bin/livepeer"),
           E2E_GO_LIVEPEER_CLI_BINARY=str(root / "bin/livepeer_cli"),
           E2E_NODE_BINARY=str(root / "bin/node/livepeer"),
           E2E_CLEARINGHOUSE_BINARY=str(root / "bin/clearinghouse"),
           E2E_COMPOSE_FILE=str(root / "scope/docker/compose.yaml"),
           COMPOSE_PROJECT_NAME=os.environ["COMPOSE_PROJECT_NAME"])
# Preserve portable suite settings across VM export without transporting host
# paths, externally selected chains, or funding credentials.
defaults = root / "scope/test.env"
defaults.write_text("\n".join(line for line in defaults.read_text().splitlines()
                             if line.strip().removeprefix("export ").split("=", 1)[0].strip() not in external_settings) + "\n")
portable = {line.split("=", 1)[0] for line in defaults.read_text().splitlines()
            if line.startswith("E2E_") and "=" in line}
with defaults.open("a") as out:
    for key in sorted(portable & env.keys()):
        value = env[key]
        if any(c in value for c in "\n\r'"):
            raise SystemExit(f"unsupported characters in {key}")
        out.write(f"\n{key}='{value}'")
portable.update({"E2E_GETH_IMAGE", "E2E_CLOUDFLARED_IMAGE", "E2E_RUNTIME_IMAGE"})
with (root / "scope/.e2e-settings.env").open("w") as out:
    for key in sorted(portable & env.keys()):
        value = env[key]
        if any(c in value for c in "\n\r'"):
            raise SystemExit(f"unsupported characters in {key}")
        out.write(f"export {key}='{value}'\n")
# Compose env_file supports single-quoted literal values. Reject characters
# that cannot be represented, rather than interpreting arbitrary dotenv syntax.
with (root / "container.env").open("w") as out:
    for key, value in sorted(env.items()):
        if any(c in value for c in "\n\r'"):
            raise SystemExit(f"unsupported characters in {key}")
        out.write(f"{key}='{value}'\n")
(root / "container.env").chmod(0o600)
(root / "artifacts/source-provenance.json").write_text(json.dumps(provenance, indent=2))
