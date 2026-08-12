from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

import yaml


SKILL = Path(__file__).resolve().parents[1]
RUNNER = SKILL / "scripts" / "workflow.py"
FAKE_CODEX = SKILL / "tests" / "fake_codex.py"


def git(root: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(root), *args], check=True, capture_output=True)


class WorkflowRuntimeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name) / "repo"
        self.root.mkdir()
        git(self.root, "init", "-q")
        (self.root / "seed.txt").write_text("seed\n", encoding="utf-8")
        git(self.root, "add", "seed.txt")
        git(self.root, "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-qm", "seed")
        git(self.root, "config", "user.name", "Workflow Test User")
        git(self.root, "config", "user.email", "workflow-test@example.com")
        (self.root / ".agents" / "codex-dynamic-workflows").mkdir(parents=True)
        self.state_dir = Path(self.temp.name) / "state"
        self.env = os.environ.copy()
        self.env["CODEX_WORKFLOW_STATE_DIR"] = str(self.state_dir)
        self.env["CODEX_WORKFLOW_CODEX_BIN"] = str(FAKE_CODEX)
        self.env["GIT_CONFIG_GLOBAL"] = str(Path(self.temp.name) / "empty-gitconfig")
        self.env["GIT_CONFIG_NOSYSTEM"] = "1"
        os.chmod(FAKE_CODEX, 0o755)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def write_workflow(self, name: str, spec: dict, files: dict[str, str] | None = None) -> None:
        directory = self.root / ".agents" / "codex-dynamic-workflows" / name
        directory.mkdir(parents=True)
        (directory / "workflow.yaml").write_text(yaml.safe_dump(spec, sort_keys=False), encoding="utf-8")
        for relative, content in (files or {}).items():
            path = directory / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")

    def invoke(self, *args: str, expected: int = 0) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            [sys.executable, str(RUNNER), *args], cwd=self.root, env=self.env,
            capture_output=True, text=True,
        )
        self.assertEqual(result.returncode, expected, result.stderr + result.stdout)
        return result

    def state(self, run_id: str) -> dict:
        matches = list(self.state_dir.glob(f"*/{run_id}/state.json"))
        self.assertEqual(len(matches), 1)
        return json.loads(matches[0].read_text(encoding="utf-8"))

    def wait_for_status(self, run_id: str, expected: str, timeout: float = 8.0) -> dict:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            state = self.state(run_id)
            if state["status"] == expected:
                return state
            time.sleep(0.05)
        self.fail(f"run {run_id} did not reach {expected}: {self.state(run_id)}")

    def base(self, name: str, nodes: list[dict], failure: str = "continue_independent") -> dict:
        return {
            "api_version": "codex.workflow/v1", "name": name, "description": name,
            "execution": {"failure_policy": failure, "max_concurrency": 4}, "nodes": nodes,
        }

    def test_static_parallel_dag(self) -> None:
        spec = self.base("static-flow", [
            {"id": "start", "type": "command", "command": ["python3", "-c", "print('start')"]},
            {"id": "left", "type": "command", "depends_on": ["start"], "command": ["python3", "-c", "print('left')"]},
            {"id": "right", "type": "command", "depends_on": ["start"], "command": ["python3", "-c", "print('right')"]},
            {"id": "finish", "type": "command", "depends_on": ["left", "right"], "command": ["python3", "-c", "print('finish')"]},
        ])
        self.write_workflow("static-flow", spec)
        self.invoke("validate", "static-flow")
        self.invoke("run", "static-flow", "--run-id", "static-run")
        state = self.state("static-run")
        self.assertEqual(state["status"], "completed")
        self.assertTrue(all(node["status"] == "success" for node in state["nodes"].values()))

    def test_detached_start_latest_status_and_declared_result(self) -> None:
        spec = self.base("background-flow", [{
            "id": "produce",
            "type": "command",
            "command": [
                "python3", "-c",
                "import json,time; time.sleep(.4); print(json.dumps({'answer': 42}))",
            ],
            "capture_json": True,
        }])
        spec["result"] = {"outputs": {"answer": "${{ nodes.produce.output.answer }}"}}
        self.write_workflow("background-flow", spec)

        started_at = time.monotonic()
        started = self.invoke("start", "background-flow", "--run-id", "background-run")
        self.assertLess(time.monotonic() - started_at, 2.0)
        self.assertEqual(json.loads(started.stdout)["run_id"], "background-run")
        status = json.loads(self.invoke("status", "--json").stdout)
        self.assertEqual(status["run_id"], "background-run")
        self.wait_for_status("background-run", "completed")
        result = json.loads(self.invoke("result", "--json").stdout)
        self.assertTrue(result["result_ready"])
        self.assertEqual(result["outputs"], {"answer": 42})
        self.assertNotIn("thread_id", json.dumps(result))

    def test_latest_run_selection_and_runs_listing(self) -> None:
        spec = self.base("latest-flow", [{
            "id": "done", "type": "command", "command": ["python3", "-c", "print('done')"]
        }])
        self.write_workflow("latest-flow", spec)
        self.invoke("run", "latest-flow", "--run-id", "older-run")
        time.sleep(0.01)
        self.invoke("run", "latest-flow", "--run-id", "newer-run")
        latest = json.loads(self.invoke("status", "--json").stdout)
        self.assertEqual(latest["run_id"], "newer-run")
        explicit = json.loads(self.invoke("status", "older-run", "--json").stdout)
        self.assertEqual(explicit["run_id"], "older-run")
        runs = json.loads(self.invoke("runs", "--json").stdout)
        self.assertEqual([item["run_id"] for item in runs[:2]], ["newer-run", "older-run"])

    def test_result_reports_running_and_failed_states(self) -> None:
        running_spec = self.base("running-result", [{
            "id": "slow", "type": "command",
            "command": ["python3", "-c", "import time; time.sleep(2)"],
        }])
        self.write_workflow("running-result", running_spec)
        self.invoke("start", "running-result", "--run-id", "running-result-run")
        pending = json.loads(self.invoke("result", "running-result-run", "--json", expected=3).stdout)
        self.assertFalse(pending["result_ready"])
        self.invoke("cancel", "running-result-run")
        self.wait_for_status("running-result-run", "cancelled")
        cancelled = json.loads(self.invoke("result", "running-result-run", "--json", expected=1).stdout)
        self.assertIsNotNone(cancelled["finished_at"])
        self.assertIsNotNone(self.state("running-result-run")["nodes"]["slow"]["finished_at"])

        failed_spec = self.base("failed-result", [{
            "id": "bad", "type": "command", "command": ["python3", "-c", "raise SystemExit(9)"]
        }])
        self.write_workflow("failed-result", failed_spec)
        self.invoke("run", "failed-result", "--run-id", "failed-result-run", expected=1)
        failed = json.loads(self.invoke("result", "failed-result-run", "--json", expected=1).stdout)
        self.assertTrue(failed["result_ready"])
        self.assertEqual(failed["failed_nodes"][0]["id"], "bad")

    def test_status_reports_dead_stale_runner(self) -> None:
        spec = self.base("stale-flow", [{
            "id": "done", "type": "command", "command": ["python3", "-c", "print('done')"]
        }])
        self.write_workflow("stale-flow", spec)
        self.invoke("run", "stale-flow", "--run-id", "stale-run")
        state_path = next(self.state_dir.glob("*/stale-run/state.json"))
        state = json.loads(state_path.read_text(encoding="utf-8"))
        state["status"] = "running"
        state["updated_at"] = "2000-01-01T00:00:00+00:00"
        state_path.write_text(json.dumps(state), encoding="utf-8")
        run_dir = state_path.parent
        (run_dir / "runner.json").write_text(
            json.dumps({"pid": 99999999, "mode": "detached-start", "started_at": state["created_at"]}),
            encoding="utf-8",
        )
        status = json.loads(self.invoke("status", "stale-run", "--json").stdout)
        self.assertFalse(status["runner"]["alive"])
        self.assertTrue(status["runner"]["heartbeat_stale"])

    def test_detached_resume_recovers_interrupted_node(self) -> None:
        marker = Path(self.temp.name) / "resume-marker"
        script = (
            "from pathlib import Path; import time; "
            f"p=Path({str(marker)!r}); first=not p.exists(); p.write_text('x'); "
            "time.sleep(5) if first else None; print('done')"
        )
        spec = self.base("resume-flow", [{
            "id": "interruptible", "type": "command", "command": ["python3", "-c", script]
        }])
        self.write_workflow("resume-flow", spec)
        started = json.loads(
            self.invoke("start", "resume-flow", "--run-id", "resume-run").stdout
        )
        pid = started["runner"]["pid"]
        os.killpg(pid, signal.SIGKILL)
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            try:
                os.kill(pid, 0)
            except OSError:
                break
            time.sleep(0.02)
        resumed = json.loads(self.invoke("resume", "resume-run", "--detach").stdout)
        self.assertTrue(resumed["detached"])
        state = self.wait_for_status("resume-run", "completed")
        self.assertEqual(state["nodes"]["interruptible"]["status"], "success")

    def test_rejects_path_traversal_in_workflow_names_and_run_ids(self) -> None:
        spec = self.base("safe-flow", [{
            "id": "done", "type": "command", "command": ["python3", "-c", "print('done')"]
        }])
        self.write_workflow("safe-flow", spec)

        invalid_workflow = self.invoke("validate", "../safe-flow", expected=2)
        self.assertIn("invalid workflow name", invalid_workflow.stderr)

        escaped_run_dir = self.state_dir / "escaped-run"
        invalid_start = self.invoke(
            "run", "safe-flow", "--run-id", "../escaped-run", expected=2
        )
        self.assertIn("invalid run ID", invalid_start.stderr)
        self.assertFalse(escaped_run_dir.exists())

        invalid_lookup = self.invoke("status", "../escaped-run", expected=2)
        self.assertIn("invalid run ID", invalid_lookup.stderr)

    def test_foreach_structured_expansion(self) -> None:
        target_schema = json.dumps({
            "type": "object", "properties": {"targets": {"type": "array", "items": {"type": "object", "properties": {"id": {"type": "string"}}, "required": ["id"], "additionalProperties": False}}},
            "required": ["targets"], "additionalProperties": False,
        })
        result_schema = json.dumps({"type": "object", "properties": {"ok": {"type": "boolean"}}, "required": ["ok"], "additionalProperties": False})
        spec = self.base("foreach-flow", [
            {"id": "discover", "type": "agent", "prompt": "prompts/discover.md", "output_schema": "schemas/targets.json", "sandbox": "read-only"},
            {"id": "repair", "type": "foreach", "depends_on": ["discover"], "items": "${{ nodes.discover.output.targets }}", "max_items": 5,
             "template": {"type": "agent", "prompt": "prompts/repair.md", "output_schema": "schemas/result.json", "sandbox": "read-only"}},
        ])
        self.write_workflow("foreach-flow", spec, {
            "prompts/discover.md": "DISCOVER_TARGETS", "prompts/repair.md": "REPAIR_TARGET",
            "schemas/targets.json": target_schema, "schemas/result.json": result_schema,
        })
        self.invoke("run", "foreach-flow", "--run-id", "foreach-run")
        state = self.state("foreach-run")
        self.assertEqual(state["status"], "completed")
        self.assertEqual(state["nodes"]["repair"]["children"], ["repair[alpha]", "repair[beta]"])
        self.assertTrue(state["nodes"]["repair"]["sealed"])

    def test_loop_stops_on_structured_condition(self) -> None:
        command = [
            "python3", "-c",
            "import json; print(json.dumps({'passed': int('${{ loop.iteration }}') >= 2}))",
        ]
        spec = self.base("loop-flow", [{
            "id": "repair-loop", "type": "loop", "max_iterations": 4, "on_exhausted": "fail",
            "body": [{"id": "verify", "type": "command", "command": command, "capture_json": True}],
            "until": {"source": "verify", "field": "passed", "equals": True},
        }])
        self.write_workflow("loop-flow", spec)
        self.invoke("run", "loop-flow", "--run-id", "loop-run")
        state = self.state("loop-run")
        self.assertEqual(state["nodes"]["repair-loop"]["iteration"], 2)
        self.assertEqual(state["status"], "completed")

    def test_worktree_lanes_and_explicit_patch_merge(self) -> None:
        write_a = "from pathlib import Path; Path('a.txt').write_text('a\\n')"
        write_b = "from pathlib import Path; Path('b.txt').write_text('b\\n')"
        spec = self.base("merge-flow", [
            {"id": "write-a", "type": "command", "command": ["python3", "-c", write_a], "lane": "lane-a", "writes": True},
            {"id": "write-b", "type": "command", "command": ["python3", "-c", write_b], "lane": "lane-b", "writes": True},
            {"id": "integrate", "type": "merge", "depends_on": ["write-a", "write-b"], "lanes": ["lane-a", "lane-b"],
             "output": {"mode": "patch", "path": "artifacts/result.patch"}},
        ])
        self.write_workflow("merge-flow", spec)
        self.invoke("run", "merge-flow", "--run-id", "merge-run", "--base", "HEAD")
        state = self.state("merge-run")
        result = state["nodes"]["integrate"]["output"]
        patch = Path(result["path"])
        self.assertTrue(patch.is_file())
        content = patch.read_text(encoding="utf-8")
        self.assertIn("a.txt", content)
        self.assertIn("b.txt", content)
        self.assertFalse((self.root / "a.txt").exists())
        lane = state["lanes"]["lane-a"]
        author = subprocess.run(
            ["git", "-C", lane["path"], "show", "-s", "--format=%an <%ae>", lane["head"]],
            check=True, capture_output=True, text=True, env=self.env,
        ).stdout.strip()
        self.assertEqual(author, "Workflow Test User <workflow-test@example.com>")

    def test_writing_workflow_requires_git_user_identity(self) -> None:
        git(self.root, "config", "--unset-all", "user.name")
        git(self.root, "config", "--unset-all", "user.email")
        spec = self.base("identity-flow", [{
            "id": "write", "type": "command",
            "command": ["python3", "-c", "from pathlib import Path; Path('x').write_text('x')"],
            "lane": "identity", "writes": True,
        }])
        self.write_workflow("identity-flow", spec)
        result = self.invoke(
            "run", "identity-flow", "--run-id", "identity-run", "--base", "HEAD", expected=2
        )
        self.assertIn("configured Git user.name and user.email", result.stderr)

    def test_continue_independent_skips_only_dependent_nodes(self) -> None:
        spec = self.base("failure-flow", [
            {"id": "bad", "type": "command", "command": ["python3", "-c", "raise SystemExit(7)"]},
            {"id": "dependent", "type": "command", "depends_on": ["bad"], "command": ["python3", "-c", "print('no')"]},
            {"id": "independent", "type": "command", "command": ["python3", "-c", "print('yes')"]},
        ])
        self.write_workflow("failure-flow", spec)
        self.invoke("run", "failure-flow", "--run-id", "failure-run", expected=1)
        state = self.state("failure-run")
        self.assertEqual(state["nodes"]["bad"]["status"], "failed")
        self.assertEqual(state["nodes"]["dependent"]["status"], "skipped")
        self.assertEqual(state["nodes"]["independent"]["status"], "success")

    def test_all_terminal_barrier_opens_after_failure(self) -> None:
        spec = self.base("barrier-flow", [
            {"id": "bad", "type": "command", "command": ["python3", "-c", "raise SystemExit(3)"]},
            {"id": "good", "type": "command", "command": ["python3", "-c", "print('ok')"]},
            {"id": "settled", "type": "barrier", "depends_on": ["bad", "good"], "policy": {"mode": "all_terminal"}},
            {"id": "report", "type": "command", "depends_on": ["settled"], "command": ["python3", "-c", "print('report')"]},
        ])
        self.write_workflow("barrier-flow", spec)
        self.invoke("run", "barrier-flow", "--run-id", "barrier-run", expected=1)
        state = self.state("barrier-run")
        self.assertEqual(state["nodes"]["settled"]["status"], "success")
        self.assertEqual(state["nodes"]["report"]["status"], "success")

    def test_retry_then_success(self) -> None:
        marker = Path(self.temp.name) / "retry-marker"
        script = (
            "from pathlib import Path; import sys; "
            f"p=Path({str(marker)!r}); exists=p.exists(); p.write_text('x'); sys.exit(0 if exists else 9)"
        )
        spec = self.base("retry-flow", [{
            "id": "flaky", "type": "command", "command": ["python3", "-c", script],
            "retry": {"max_attempts": 2, "delay_seconds": 0},
        }])
        self.write_workflow("retry-flow", spec)
        self.invoke("run", "retry-flow", "--run-id", "retry-run")
        state = self.state("retry-run")
        self.assertEqual(state["nodes"]["flaky"]["attempt"], 2)
        self.assertEqual(state["nodes"]["flaky"]["status"], "success")

    def test_command_timeout_is_structured(self) -> None:
        spec = self.base("timeout-flow", [{
            "id": "slow", "type": "command",
            "command": ["python3", "-c", "import time; time.sleep(5)"],
            "timeout_seconds": 1,
        }])
        self.write_workflow("timeout-flow", spec)
        self.invoke("run", "timeout-flow", "--run-id", "timeout-run", expected=1)
        node = self.state("timeout-run")["nodes"]["slow"]
        self.assertTrue(node["timed_out"])
        self.assertEqual(node["error"], "process timed out")

    def test_workflow_timeout_uses_configured_deadline(self) -> None:
        spec = self.base("workflow-timeout", [{
            "id": "slow", "type": "command",
            "command": ["python3", "-c", "import time; time.sleep(10)"],
        }], failure="fail_fast")
        spec["execution"]["timeout_seconds"] = 1
        self.write_workflow("workflow-timeout", spec)
        started = time.monotonic()
        self.invoke("run", "workflow-timeout", "--run-id", "workflow-timeout-run", expected=1)
        self.assertLess(time.monotonic() - started, 3)
        state = self.state("workflow-timeout-run")
        self.assertEqual(state["status"], "cancelled")
        self.assertEqual(state["error"], "workflow timeout exceeded")
        self.assertIsNotNone(state["finished_at"])

    def test_when_on_barrier_foreach_template_and_loop_body(self) -> None:
        seed = "import json; print(json.dumps({'enabled': False, 'items': [{'id': 'one'}]}))"
        spec = self.base("dynamic-when", [
            {"id": "seed", "type": "command", "command": ["python3", "-c", seed], "capture_json": True},
            {"id": "gated-barrier", "type": "barrier", "depends_on": ["seed"],
             "policy": {"mode": "all_success"},
             "when": {"source": "seed", "field": "enabled", "equals": True}},
            {"id": "foreach", "type": "foreach", "depends_on": ["seed"],
             "items": "${{ nodes.seed.output.items }}", "max_items": 1,
             "template": {"type": "command", "command": ["python3", "-c", "print('no')"],
                          "when": {"source": "seed", "field": "enabled", "equals": True}}},
            {"id": "loop", "type": "loop", "depends_on": ["seed"], "max_iterations": 1,
             "on_exhausted": "fail",
             "body": [
                 {"id": "check", "type": "command",
                  "command": ["python3", "-c", seed], "capture_json": True},
                 {"id": "optional", "type": "command", "command": ["python3", "-c", "print('no')"],
                  "when": {"source": "check", "field": "enabled", "equals": True}},
             ],
             "until": {"source": "check", "field": "enabled", "equals": False}},
        ])
        self.write_workflow("dynamic-when", spec)
        self.invoke("run", "dynamic-when", "--run-id", "dynamic-when-run")
        state = self.state("dynamic-when-run")
        self.assertEqual(state["nodes"]["gated-barrier"]["status"], "skipped")
        self.assertEqual(state["nodes"]["foreach[one]"]["status"], "skipped")
        self.assertEqual(state["nodes"]["loop.optional@1"]["status"], "skipped")

    def test_fail_fast_cancels_running_branch(self) -> None:
        spec = self.base("fast-fail-flow", [
            {"id": "bad", "type": "command", "command": ["python3", "-c", "raise SystemExit(4)"]},
            {"id": "slow", "type": "command", "command": ["python3", "-c", "import time; time.sleep(10)"]},
            {"id": "later", "type": "command", "depends_on": ["slow"], "command": ["python3", "-c", "print('later')"]},
        ], failure="fail_fast")
        self.write_workflow("fast-fail-flow", spec)
        self.invoke("run", "fast-fail-flow", "--run-id", "fast-fail-run", expected=1)
        state = self.state("fast-fail-run")
        self.assertEqual(state["nodes"]["bad"]["status"], "failed")
        self.assertEqual(state["nodes"]["slow"]["status"], "cancelled")
        self.assertEqual(state["nodes"]["later"]["status"], "skipped")

    def test_foreach_loop_lanes_merge_as_dynamic_group(self) -> None:
        discover = "import json; print(json.dumps([{'id':'alpha'},{'id':'beta'}]))"
        write = "from pathlib import Path; Path('${{ item.id }}.txt').write_text('${{ item.id }}-${{ loop.iteration }}\\n')"
        verify = "import json; print(json.dumps({'passed': int('${{ loop.iteration }}') >= 2}))"
        spec = self.base("dynamic-merge-flow", [
            {"id": "discover", "type": "command", "command": ["python3", "-c", discover], "capture_json": True},
            {"id": "repair", "type": "foreach", "depends_on": ["discover"], "items": "${{ nodes.discover.output }}", "max_items": 5,
             "template": {
                 "type": "loop", "max_iterations": 3, "on_exhausted": "fail",
                 "body": [
                     {"id": "fix", "type": "command", "command": ["python3", "-c", write], "lane": "repair-${{ item.id }}", "writes": True},
                     {"id": "verify", "type": "command", "command": ["python3", "-c", verify], "lane": "repair-${{ item.id }}", "capture_json": True},
                 ],
                 "until": {"source": "verify", "field": "passed", "equals": True},
             }},
            {"id": "integrate", "type": "merge", "depends_on": ["repair"], "lane_groups": ["repair"],
             "output": {"mode": "patch", "path": "artifacts/dynamic.patch"}},
        ])
        self.write_workflow("dynamic-merge-flow", spec)
        self.invoke("validate", "dynamic-merge-flow")
        self.invoke("run", "dynamic-merge-flow", "--run-id", "dynamic-merge-run", "--base", "HEAD")
        state = self.state("dynamic-merge-run")
        self.assertEqual(state["status"], "completed")
        self.assertEqual(state["nodes"]["repair[alpha]"]["iteration"], 2)
        self.assertEqual(state["nodes"]["repair[beta]"]["iteration"], 2)
        patch = Path(state["nodes"]["integrate"]["output"]["path"]).read_text(encoding="utf-8")
        self.assertIn("alpha.txt", patch)
        self.assertIn("beta.txt", patch)
        self.assertIn("alpha-2", patch)
        self.assertIn("beta-2", patch)


if __name__ == "__main__":
    unittest.main()
