import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


class SnapshotTest(unittest.TestCase):
    def test_vm_export_preserves_sources_and_filters_external_chain(self):
        script = Path(__file__).with_name("prepare.py").resolve()
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            scope = base / "scope"
            env = os.environ.copy()
            for name in ("scope", "go-livepeer", "node", "clearinghouse", "python-runner"):
                source = base / name
                source.mkdir()
                (source / ".e2e-source.json").write_text(json.dumps({"source": "/original/" + name, "revision": "abc123", "status": " M main.go"}))
                (source / "main.go").write_text("modified working source\n")
            (scope / "test.env").write_text("E2E_HTTP_TIMEOUT=5s\nE2E_RPC_URL=https://external.invalid\nexport E2E_BOOTSTRAP_SECRET_FILE=/production/secret\n")
            migrations = base / "clearinghouse/migrations"
            migrations.mkdir()
            (migrations / "001_initial.sql").write_text("CREATE TABLE example (id INTEGER);\n")
            (migrations / "._001_initial.sql").write_bytes(b"AppleDouble metadata")
            ignored = scope / "artifacts/old-run/.venv"
            ignored.mkdir(parents=True)
            (ignored / "python").symlink_to("/root/private-python")
            env.update(GO_LIVEPEER_DIR=str(base / "go-livepeer"), E2E_NODE_DIR=str(base / "node"),
                       E2E_CLEARINGHOUSE_DIR=str(base / "clearinghouse"), PYTHON_RUNNER_DIR=str(base / "python-runner"),
                       COMPOSE_PROJECT_NAME="test-snapshot", E2E_RPC_URL="https://external.invalid",
                       E2E_ETH_CONTROLLER="external-controller", E2E_EXPECTED_CHAIN_ID="1",
                       E2E_FUNDER_KEY_FILE="/production/key", E2E_BOOTSTRAP_SECRET_FILE="/production/secret",
                       E2E_HTTP_TIMEOUT="17s")
            for key in ("E2E_GOLANG_RUNNER_DIR", "E2E_LPMS_DIR"):
                env.pop(key, None)
            target = base / "export"
            subprocess.run(["python3", str(script), str(target)], cwd=scope, env=env, check=True)
            config = (target / "container.env").read_text()
            for key in ("E2E_RPC_URL", "E2E_ETH_CONTROLLER", "E2E_EXPECTED_CHAIN_ID", "E2E_FUNDER_KEY_FILE", "E2E_BOOTSTRAP_SECRET_FILE"):
                self.assertNotIn(key + "=", config)
            self.assertEqual((target / "node/main.go").read_text(), "modified working source\n")
            self.assertTrue((target / "clearinghouse/migrations/001_initial.sql").is_file())
            self.assertFalse((target / "clearinghouse/migrations/._001_initial.sql").exists())
            self.assertEqual((target / "container.env").stat().st_mode & 0o777, 0o600)
            self.assertIn("E2E_HTTP_TIMEOUT='17s'", (target / "scope/test.env").read_text())
            self.assertNotIn("E2E_RPC_URL=", (target / "scope/test.env").read_text())
            self.assertNotIn("E2E_BOOTSTRAP_SECRET_FILE=", (target / "scope/test.env").read_text())
            source = json.loads((target / "node/.e2e-source.json").read_text())
            self.assertEqual(source["revision"], "abc123")
            self.assertEqual(source["status"], " M main.go")
            self.assertEqual(len(source["snapshot_sha256"]), 64)


if __name__ == "__main__":
    unittest.main()
