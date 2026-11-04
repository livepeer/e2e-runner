"""Local integration checks; no cloud resources or product builds required."""
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import tarfile
import unittest
from unittest.mock import patch

import controlled
import gcp
import report
import run
import targets

REPO = Path(__file__).resolve().parent.parent


class TargetTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="performance-target-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        environment = patch.dict(os.environ, {"GIT_CONFIG_GLOBAL": str(self.root / "gitconfig"), "GIT_CONFIG_NOSYSTEM": "1"})
        environment.start()
        self.addCleanup(environment.stop)

    def git(self, source, *args):
        return subprocess.check_output(["git", "-C", str(source), *args], text=True, stderr=subprocess.PIPE).strip()

    def repository(self, name="upstream"):
        source = self.root / name
        source.mkdir(parents=True)
        self.git(source, "init", "--quiet")
        self.git(source, "config", "user.name", "Performance fixture")
        self.git(source, "config", "user.email", "fixture@example.invalid")
        (source / "file").write_text("first\n")
        self.git(source, "add", "file")
        self.git(source, "commit", "--quiet", "-m", "first")
        return source

    def configuration(self, **settings):
        return targets.configured({"E2E_PERF_NODE_TARGETS": "trial", "E2E_PERF_NODE_TRIAL_REF": "HEAD"} | settings)

    def test_fixed_baselines_and_arbitrary_names(self):
        self.assertEqual([t["name"] for t in targets.configured({})], ["node-current", "go-livepeer"])
        values = self.configuration(E2E_PERF_NODE_TARGETS="trial,next-tip", E2E_PERF_NODE_NEXT_TIP_REF="refs/pull/43/head")
        self.assertEqual([t["name"] for t in values], ["node-current", "trial", "next-tip", "go-livepeer"])
        self.assertEqual(values[2]["ref"], "refs/pull/43/head")
        self.assertEqual(targets.select(values, "trial,node-current"), ["trial", "node-current"])
        for names in ("node-current", "go-livepeer", "protocol", "trial,trial", "../trial", "trial,,other", "Trial", "trial_name"):
            with self.subTest(names=names), self.assertRaises(ValueError):
                self.configuration(E2E_PERF_NODE_TARGETS=names)
        for selection in ("", "trial,trial", "missing"):
            with self.subTest(selection=selection), self.assertRaises(ValueError):
                targets.select(values, selection)
        with self.assertRaises(ValueError):
            targets.configured({"E2E_PERF_NODE_TARGETS": "missing-ref"})

    def test_real_dotenv_loader_and_environment_precedence(self):
        config = self.root / "test.local.env"
        fixture = self.root / "provider fixture.go"
        fixture.write_text("package fixture\n")
        config.write_text('BUILD_REF=refs/pull/43/head\nexport E2E_PERF_NODE_TARGETS="my-build" # comment\n'
                          'E2E_PERF_NODE_MY_BUILD_REF="${BUILD_REF}"\n'
                          "E2E_PERF_NODE_MY_BUILD_REPO='https://github.com/example/fork.git'\n"
                          f"E2E_PERF_CONTROLLED_FIXTURE='{fixture}'\n")
        # No process target settings should leak into this fixture.
        clean = {k: v for k, v in os.environ.items() if not k.startswith("E2E_PERF_NODE_")}
        clean.pop("E2E_PERF_CONTROLLED_FIXTURE", None)
        with patch.dict(os.environ, clean | {"E2E_ENV_FILE": str(config)}, clear=True):
            configuration = targets.load_configuration()
            self.assertEqual(configuration["targets"][1]["ref"], "refs/pull/43/head")
            self.assertEqual(configuration["controlled_fixture"], str(fixture.resolve()))
            with patch.dict(os.environ, {"E2E_PERF_NODE_MY_BUILD_REF": "release"}):
                self.assertEqual(targets.load_configuration()["targets"][1]["ref"], "release")

    def test_independent_git_bundles_freeze_refs_and_leave_checkout_unchanged(self):
        source = self.repository()
        first = self.git(source, "rev-parse", "HEAD")
        self.git(source, "update-ref", "refs/pull/7/head", first)
        (source / "file").write_text("second\n")
        self.git(source, "commit", "--quiet", "-am", "second")
        second = self.git(source, "rev-parse", "HEAD")
        before = self.git(source, "show-ref")
        config = self.configuration(E2E_PERF_NODE_TRIAL_REPO=str(source), E2E_PERF_NODE_TRIAL_REF="refs/pull/7/head")
        config[1] = targets.bundle(config[1], self.root / "trial.bundle")
        self.assertEqual(config[1]["revision"], first)
        (self.root / "manifest.json").write_text(json.dumps({"targets": config}))
        (self.root / "sources").mkdir()
        targets.prepare(self.root)
        self.assertEqual(self.git(self.root / "sources/trial", "rev-parse", "HEAD"), first)
        self.assertEqual((self.root / "sources/trial/file").read_text(), "first\n")
        targets.prepare(self.root)  # Safe, identical resume.
        local = self.configuration(E2E_PERF_NODE_TRIAL_CHECKOUT=str(source))[1]
        frozen = targets.bundle(local, self.root / "local.bundle")
        self.assertEqual(frozen["revision"], second)
        self.assertNotIn("checkout", frozen)
        self.assertEqual(self.git(source, "show-ref"), before)
        self.assertEqual(self.git(source, "rev-parse", "HEAD"), second)
        (source / "file").write_text("dirty\n")
        with self.assertRaisesRegex(ValueError, "must be clean"):
            targets.bundle(local, self.root / "dirty.bundle")
        (self.root / "sources/trial/file").write_text("dirty\n")
        with self.assertRaisesRegex(ValueError, "differs"):
            targets.prepare(self.root)

    def runtime(self):
        configuration = self.configuration()
        (self.root / "manifest.json").write_text(json.dumps({"targets": configuration}))
        self.repository("sources/trial")
        deployment = self.root / "sources/protocol/deployments/gethDev"
        deployment.mkdir(parents=True)
        (deployment / "Controller.json").write_text('{"address":"0x123"}')
        (self.root / "scope").mkdir()
        binaries = self.root / "bin/trial"
        binaries.mkdir(parents=True)
        for component in ["livepeer", "livepeer-orchestrator", "livepeer-signer", "livepeer-chain"]:
            (binaries / component).write_bytes(b"product")
        executable = self.root / "bin/scope.test"
        executable.write_text('#!/bin/sh\nprintf "run\\n" >> "$E2E_ARTIFACT_DIR/invocations"\n')
        executable.chmod(0o755)
        return configuration

    def test_routing_and_resume_check_actual_build_and_harness(self):
        self.runtime()
        with patch.object(run, "ROOT", self.root), contextlib.redirect_stdout(io.StringIO()):
            env = run.environment("trial", self.root / "results")
            self.assertEqual(env["E2E_PERF_BACKEND"], "node")
            self.assertEqual(env["E2E_NODE_BINARY"], str(self.root / "bin/trial/livepeer"))
            for name in ("trial", "node-current", "go-livepeer"):
                self.assertIn(str(self.root / "sources" / name), env.values())
            for _ in range(2):
                self.assertEqual(run.run_test("trial", "cell", "pattern", {"E2E_PERF_SAMPLES": "2"}, resume=True), 0)
            self.assertEqual((self.root / "results/trial/cell/invocations").read_text(), "run\n")
            for changed in (self.root / "bin/trial/livepeer", self.root / "bin/trial/livepeer-signer", self.root / "bin/scope.test"):
                original = changed.read_bytes()
                changed.write_bytes(original + b"changed")
                with self.assertRaisesRegex(SystemExit, "different inputs"):
                    run.run_test("trial", "cell", "pattern", {"E2E_PERF_SAMPLES": "2"}, resume=True)
                changed.write_bytes(original)
            with self.assertRaisesRegex(SystemExit, "different inputs"):
                run.run_test("trial", "cell", "pattern", {"E2E_PERF_SAMPLES": "3"}, resume=True)
            source = self.root / "sources/trial"
            (source / "file").write_text("new revision\n")
            self.git(source, "commit", "--quiet", "-am", "new revision")
            with self.assertRaisesRegex(SystemExit, "different inputs"):
                run.run_test("trial", "cell", "pattern", {"E2E_PERF_SAMPLES": "2"}, resume=True)

    def test_selected_matrix_size_and_rotating_order(self):
        (self.root / "results").mkdir()
        args = type("Args", (), {"targets": "node-current,trial", "quick": False})()
        with patch.object(run, "ROOT", self.root), patch.object(run, "run_test", return_value=0) as execute:
            run.matrix(args)
        calls = execute.call_args_list
        self.assertEqual(len(calls), 25)  # One recipient setup and 24 timing cells.
        self.assertEqual([c.args[0] for c in calls[1:3]], ["node-current", "trial"])
        self.assertEqual([c.args[0] for c in calls[9:11]], ["trial", "node-current"])
        self.assertEqual(json.loads((self.root / "results/matrix.json").read_text())["expected_cells"], 24)

    def test_build_script_and_manifest_accept_non_node_prefixed_names(self):
        configuration = self.configuration()
        for name in [t["name"] for t in configuration] + ["protocol"]:
            self.repository("sources/" + name)
        (self.root / "manifest.json").write_text(json.dumps({"targets": configuration, "selected_targets": ["trial"]}))
        deployment = self.root / "sources/protocol/deployments/gethDev"
        deployment.mkdir(parents=True)
        for name in ("Controller", "TicketBroker", "RoundsManager", "BondingManager"):
            (deployment / (name + ".json")).write_text('{"address":"0x123"}')
        (self.root / "scope").symlink_to(REPO, target_is_directory=True)
        tools = self.root / "tools"
        tools.mkdir()
        compiler = tools / "go"
        compiler.write_text('''#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
args = sys.argv[1:]
with open(os.environ["GO_COMMAND_LOG"], "a") as log:
    log.write(json.dumps({"cwd": os.getcwd(), "args": args}) + "\\n")
if args[0] == "version":
    print("go version go1.27.test linux/amd64" if len(args) == 1 else args[1] + ": go1.27.test")
else:
    output = args[args.index("-o") + 1]
    paths = [Path(output) / Path(p).name for p in args if p.startswith("./cmd/")] if output.endswith("/") else [Path(output)]
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"stub build")
''')
        compiler.chmod(0o755)
        ffmpeg = tools / "ffmpeg"
        ffmpeg.write_text('#!/bin/sh\necho "ffmpeg test fixture"\n')
        ffmpeg.chmod(0o755)
        log = self.root / "commands.jsonl"
        env = os.environ | {"PATH": str(tools) + os.pathsep + os.environ["PATH"], "PERF_ROOT": str(self.root), "GO_COMMAND_LOG": str(log)}
        subprocess.run(["bash", str(REPO / "performance/build.sh")], env=env, check=True, capture_output=True)
        manifest = json.loads((self.root / "manifest.json").read_text())
        self.assertEqual(manifest["selected_targets"], ["trial"])
        binaries = manifest["sources"]["trial"]["binaries_sha256"]
        self.assertEqual(len(binaries), 4)
        self.assertIn("bin/trial/livepeer-signer", binaries)
        self.assertEqual(manifest["sources"]["trial"]["backend"], "node")
        self.assertEqual(manifest["sources"]["trial"]["revision"], self.git(self.root / "sources/trial", "rev-parse", "HEAD"))
        builds = [json.loads(line) for line in log.read_text().splitlines() if json.loads(line)["args"][0] == "build"]
        self.assertEqual([Path(b["cwd"]).name for b in builds], ["node-current", "trial", "go-livepeer", "go-livepeer"])

    def test_driver_propagates_selection_and_deletes_vm_after_failure(self):
        upstream = self.repository()
        fixture = self.root / "external-fixture.go"
        fixture.write_text("package fixture\n")
        for failure in (False, True):
            with self.subTest(matrix_failure=failure):
                driver = self.repository("driver-" + str(failure))
                (driver / "performance").mkdir()
                configuration = self.configuration(E2E_PERF_NODE_TARGETS="trial,unused", E2E_PERF_NODE_TRIAL_REPO=str(upstream),
                                                   E2E_PERF_NODE_UNUSED_REF="missing-branch")
                commands = []
                settings = {"targets": configuration}
                if not failure:
                    settings["controlled_fixture"] = str(fixture)

                def execute(argv, **kwargs):
                    commands.append(argv)
                    if argv[:3] == ["gcloud", "compute", "scp"] and argv[3].endswith("scope.tar.gz"):
                        with tarfile.open(argv[3]) as archive:
                            if not failure:
                                self.assertEqual(archive.extractfile("controlled-fixture.go").read(), fixture.read_bytes())
                            else:
                                self.assertNotIn("controlled-fixture.go", archive.getnames())
                    if failure and any("run.py matrix" in arg for arg in argv):
                        raise subprocess.CalledProcessError(1, argv)
                    return subprocess.CompletedProcess(argv, 0)

                with patch.object(gcp, "__file__", str(driver / "performance/gcp.py")), \
                     patch.object(targets, "load_configuration", return_value=settings), \
                     patch.object(gcp, "run", side_effect=execute), \
                     patch("sys.argv", ["gcp.py", "--targets=node-current,trial"]), \
                     contextlib.redirect_stdout(io.StringIO()):
                    if failure:
                        with self.assertRaises(subprocess.CalledProcessError):
                            gcp.main()
                    else:
                        gcp.main()
                remote = [arg for command in commands for arg in command if arg.startswith("--command=")]
                for phase in ("run.py correctness", "controlled.py", "run.py validate", "run.py matrix"):
                    matching = [command for command in remote if phase in command]
                    self.assertEqual(len(matching), 1)
                    self.assertIn("--targets=node-current,trial", matching[0])
                self.assertFalse(any("diagnose_go_manifest.py" in command for command in remote))
                self.assertEqual(sum(command[:4] == ["gcloud", "compute", "instances", "delete"] for command in commands), 1)
                output = next((driver / "artifacts").iterdir())
                manifest = json.loads((output / "manifest.json").read_text())
                self.assertEqual([t["name"] for t in manifest["targets"]], ["node-current", "trial", "go-livepeer"])
                self.assertEqual(manifest["targets"][1]["revision"], self.git(upstream, "rev-parse", "HEAD"))
                if not failure:
                    self.assertEqual(manifest["controlled_fixture"], {"path": "controlled-fixture.go", "sha256": hashlib.sha256(fixture.read_bytes()).hexdigest()})
                else:
                    self.assertNotIn("controlled_fixture", manifest)
                self.assertFalse(list(output.glob("*.bundle")))

    def test_controlled_adapters_follow_source_api_and_explicit_skip(self):
        source = self.root / "source/eth"
        source.mkdir(parents=True)
        template = (REPO / "performance/controlled_test.go.txt").read_text()
        for adapter, code in (("direct", "func (c PaymentChain) SenderInfo("), ("chain-view", "func NewChainView("),
                              ("payment-observer", "func NewPaymentObserver(\nfunc (c PaymentChain) PayerFunds("), ("none", "package eth")):
            (source / "payment.go").write_text(code)
            actual, content = controlled.adapter_for(source.parent, "auto")
            self.assertEqual(actual, adapter)
            if adapter != "none":
                overlay = controlled.overlay(template, "any-name", adapter, content)
                self.assertNotIn("__", overlay)
                self.assertIn('"target":"any-name"', overlay)
                if adapter == "payment-observer":
                    self.assertNotIn("direct.SenderInfo", overlay)
        self.assertEqual(controlled.adapter_for(source.parent, "none")[0], "none")
        self.runtime()
        shutil.rmtree(self.root / "scope")
        (self.root / "scope").symlink_to(REPO, target_is_directory=True)
        with contextlib.redirect_stdout(io.StringIO()):
            controlled.run(self.root, ["trial"])
        self.assertTrue(json.loads((self.root / "results/trial/controlled/exit.json").read_text())["skipped"])

    def test_external_controlled_fixture_copy_hash_and_missing_fixture_skip(self):
        self.runtime()
        shutil.rmtree(self.root / "scope")
        (self.root / "scope").symlink_to(REPO, target_is_directory=True)
        source = self.root / "sources/trial"
        (source / "eth").mkdir()
        (source / "internal").mkdir()
        # A minimal source API for routing the overlay, not a product mock.
        (source / "eth/payment.go").write_text("package eth\ntype PaymentChain struct{}\nfunc (c PaymentChain) SenderInfo() {}\n")
        (source / "internal/.gitkeep").touch()
        self.git(source, "add", ".")
        self.git(source, "commit", "--quiet", "-m", "controlled API")
        with patch.object(controlled.subprocess, "run") as execute, contextlib.redirect_stdout(io.StringIO()):
            controlled.run(self.root, ["trial"])
        execute.assert_not_called()
        status = self.root / "results/trial/controlled/exit.json"
        self.assertTrue(json.loads(status.read_text())["skipped"])
        self.assertIn("No external controlled fixture configured", json.loads(status.read_text())["reason"])
        fixture = self.root / "controlled-fixture.go"
        fixture.write_text("package fixture\n")
        manifest = self.root / "manifest.json"
        frozen = json.loads(manifest.read_text())
        frozen["controlled_fixture"] = {"path": fixture.name, "sha256": hashlib.sha256(fixture.read_bytes()).hexdigest()}
        manifest.write_text(json.dumps(frozen))
        original_run = subprocess.run

        def execute(argv, **kwargs):
            return subprocess.CompletedProcess(argv, 0) if argv[0] == "go" else original_run(argv, **kwargs)

        with patch.object(controlled.subprocess, "run", side_effect=execute), contextlib.redirect_stdout(io.StringIO()):
            controlled.run(self.root, ["trial"])
        self.assertEqual((self.root / "controlled/trial/internal/performancefixture/chainrpc.go").read_bytes(), fixture.read_bytes())
        overlay = json.loads((self.root / "results/trial/controlled/overlay.json").read_text())
        self.assertEqual(overlay["fixture_sha256"], frozen["controlled_fixture"]["sha256"])
        self.assertEqual(json.loads(status.read_text())["exit_code"], 0)
        fixture.write_text("package changed\n")
        with self.assertRaisesRegex(ValueError, "differs from the frozen input"):
            controlled.run(self.root, ["trial"])

    def test_report_uses_configured_names_repositories_and_expected_cells(self):
        result = self.root / "results"
        result.mkdir()
        configuration = self.configuration(E2E_PERF_NODE_TARGETS="trial,other", E2E_PERF_NODE_OTHER_REF="main")
        sources = {t["name"]: t | {"revision": "a" * 40} for t in configuration}
        sources["trial"]["repository"] = "https://github.com/example/fork.git"
        (self.root / "manifest.json").write_text(json.dumps({"sources": sources, "selected_targets": list(sources)}))
        (result / "matrix.json").write_text(json.dumps({"expected_cells": 48, "repetitions": 3}))
        for name, dispatch in (("node-current", 20), ("trial", 10), ("other", 5), ("go-livepeer", 40)):
            cell = result / name / "delay-110-workers-1-repeat-0"
            cell.mkdir(parents=True)
            (cell / "cell.json").write_text(json.dumps({"target": name, "revision": "a" * 40, "delay_ms": 110, "workers": 1,
                "repetition": 0, "samples": 1, "successes": 1, "elapsed_seconds": 1, "idle_seconds": 0,
                "resources": {"processes": [{"cpu_ticks": 1, "sampled_peak_rss_bytes": 100}] * 2},
                "startup": {"orchestrator_http_ready_seconds": 1, "signer_http_ready_seconds": 1}}))
            sample = {phase: dispatch for phase in ["challenge_ms", "signer_ms", "receive_ms", "dispatch_ms", "first_byte_ms", "total_ms"]}
            (cell / "requests.jsonl").write_text(json.dumps(sample | {"window": "warm", "tickets": 1, "face_value_wei": "100", "win_probability": "1"}) + "\n")
        skipped = result / "trial/controlled"
        skipped.mkdir()
        (skipped / "exit.json").write_text('{"exit_code":null,"skipped":true,"reason":"unsupported API"}')
        report.write_report(result, report.summarize(result))
        text = (result / "REPORT.md").read_text()
        self.assertIn("4/48 formal cells", text)
        self.assertIn("https://github.com/example/fork/commit/", text)
        self.assertIn("| trial | node-current | 110 ms | 1 | 0.5000× | 50.00% |", text)
        self.assertIn("| trial | go-livepeer | 110 ms | 1 | 0.2500× | 75.00% |", text)
        self.assertIn("| other | trial | 110 ms | 1 | 0.5000× | 50.00% |", text)
        self.assertIn("| trial | controlled | skipped | unsupported API |", text)
        self.assertNotIn("node-combined", text)
        self.assertNotIn("Provisional combined", text)


if __name__ == "__main__":
    unittest.main()
