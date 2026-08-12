#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "jsonschema==4.26.0",
#   "PyYAML==6.0.3",
# ]
# ///
"""Deterministic global DAG orchestration for project-local Codex workflows."""

from __future__ import annotations

import argparse
import asyncio
import copy
import hashlib
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import jsonschema
import yaml


API_VERSION = "codex.workflow/v1"
TERMINAL = {"success", "failed", "skipped", "cancelled"}
SUCCESS_LIKE = {"success"}
RUN_TERMINAL = {"completed", "failed", "cancelled"}
TEMPLATE_RE = re.compile(r"\$\{\{\s*([^{}]+?)\s*\}\}")
NODE_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,62}$")
WORKFLOW_NAME_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,62}[a-z0-9])?$")
RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
DEFAULT_CONCURRENCY = 4
DEFAULT_AGENT_TIMEOUT = 1800
HEARTBEAT_SECONDS = 5


class WorkflowError(RuntimeError):
    pass


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def run_git(root: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        ["git", "-C", str(root), *args], capture_output=True, text=True, check=False
    )
    if check and result.returncode != 0:
        raise WorkflowError(result.stderr.strip() or f"git {' '.join(args)} failed")
    return result


def find_repo(start: Path) -> Path:
    result = run_git(start.resolve(), "rev-parse", "--show-toplevel", check=False)
    if result.returncode != 0:
        raise WorkflowError(f"not a Git repository: {start}")
    return Path(result.stdout.strip()).resolve()


def repository_id(root: Path) -> str:
    return hashlib.sha256(str(root).encode()).hexdigest()[:16]


def state_root(root: Path) -> Path:
    configured = os.environ.get("CODEX_WORKFLOW_STATE_DIR")
    if configured:
        return Path(configured).expanduser().resolve() / repository_id(root)
    codex_home = Path(os.environ.get("CODEX_HOME", Path.home() / ".codex")).expanduser()
    return codex_home / "workflow-runs" / repository_id(root)


def process_alive(pid: int | None) -> bool:
    if not pid or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def runner_metadata(run_dir: Path) -> dict[str, Any]:
    path = run_dir / "runner.json"
    if not path.is_file():
        return {}
    try:
        metadata = read_json(path)
    except (OSError, ValueError, json.JSONDecodeError):
        return {"metadata_error": "invalid runner.json"}
    pid = metadata.get("pid")
    metadata["alive"] = process_alive(pid if isinstance(pid, int) else None)
    return metadata


def require_workflow_name(name: str) -> str:
    if not WORKFLOW_NAME_RE.fullmatch(name):
        raise WorkflowError(f"invalid workflow name: {name}")
    return name


def require_run_id(run_id: str) -> str:
    if not RUN_ID_RE.fullmatch(run_id):
        raise WorkflowError(f"invalid run ID: {run_id}")
    return run_id


def workflow_dir(root: Path, name: str) -> Path:
    require_workflow_name(name)
    return root / ".agents" / "codex-dynamic-workflows" / name


def load_workflow(root: Path, name: str) -> tuple[dict[str, Any], Path]:
    directory = workflow_dir(root, name)
    path = directory / "workflow.yaml"
    if not path.is_file():
        raise WorkflowError(f"workflow not found: {name}")
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise WorkflowError("workflow.yaml must contain a mapping")
    return data, directory


def schema_path() -> Path:
    return Path(__file__).resolve().parent.parent / "schemas" / "workflow.schema.json"


def validate_workflow(spec: dict[str, Any], directory: Path) -> list[str]:
    errors: list[str] = []
    schema = read_json(schema_path())
    validator = jsonschema.Draft202012Validator(schema)
    for error in sorted(validator.iter_errors(spec), key=lambda item: list(item.path)):
        location = ".".join(str(part) for part in error.absolute_path) or "$"
        errors.append(f"{location}: {error.message}")
    if errors:
        return errors

    nodes = spec["nodes"]
    identifiers = [node["id"] for node in nodes]
    if len(identifiers) != len(set(identifiers)):
        errors.append("node IDs must be unique")
    known = set(identifiers)
    for node in nodes:
        for dependency in node.get("depends_on", []):
            if dependency not in known:
                errors.append(f"{node['id']}: unknown dependency {dependency}")
        if node["type"] == "agent":
            validate_agent_files(node, directory, errors, node["id"])
        elif node["type"] == "command" and node.get("writes") and not node.get("lane"):
            errors.append(f"{node['id']}: writing command requires lane")
        elif node["type"] == "foreach":
            validate_template_files(node["template"], directory, errors, node["id"])
        elif node["type"] == "loop":
            body_ids = [item.get("id") for item in node["body"]]
            if any(not value or not NODE_ID_RE.fullmatch(value) for value in body_ids):
                errors.append(f"{node['id']}: every loop body node needs a valid id")
            if len(body_ids) != len(set(body_ids)):
                errors.append(f"{node['id']}: loop body IDs must be unique")
            if node["until"]["source"] not in set(body_ids):
                errors.append(f"{node['id']}: until.source must reference a body node")
            for item in node["body"]:
                validate_template_files(item, directory, errors, f"{node['id']}.{item.get('id')}")
        elif node["type"] == "merge" and node.get("conflict_prompt"):
            ensure_relative_file(directory, node["conflict_prompt"], errors, node["id"])
        if node["type"] == "merge":
            for group in node.get("lane_groups", []):
                if group not in known:
                    errors.append(f"{node['id']}: unknown lane group {group}")
        if node["type"] == "barrier":
            policy = node["policy"]
            if policy["mode"] == "minimum_success":
                count = policy.get("count")
                if count is None or count > len(node["depends_on"]):
                    errors.append(f"{node['id']}: invalid minimum_success count")

    validate_result_contract(spec.get("result"), known, errors)

    graph = {node["id"]: list(node.get("depends_on", [])) for node in nodes}
    errors.extend(detect_cycles(graph))
    validate_lanes(spec, errors)
    return errors


def validate_result_contract(
    result: dict[str, Any] | None, known_nodes: set[str], errors: list[str]
) -> None:
    if not result:
        return

    def visit(value: Any, label: str) -> None:
        if isinstance(value, str):
            for match in TEMPLATE_RE.finditer(value):
                expression = match.group(1).strip()
                parts = expression.split(".")
                if not parts or parts[0] not in {"inputs", "nodes"}:
                    errors.append(f"{label}: unsupported result expression {expression}")
                elif parts[0] == "nodes" and (len(parts) < 2 or parts[1] not in known_nodes):
                    errors.append(f"{label}: unknown result node {parts[1] if len(parts) > 1 else expression}")
        elif isinstance(value, list):
            for index, item in enumerate(value):
                visit(item, f"{label}[{index}]")
        elif isinstance(value, dict):
            for key, item in value.items():
                visit(item, f"{label}.{key}")

    visit(result.get("outputs", {}), "result.outputs")


def validate_agent_files(node: dict[str, Any], directory: Path, errors: list[str], label: str) -> None:
    ensure_relative_file(directory, node["prompt"], errors, label)
    if node.get("output_schema"):
        ensure_relative_file(directory, node["output_schema"], errors, label)
    if node.get("sandbox", "read-only") == "workspace-write" and not node.get("lane"):
        errors.append(f"{label}: workspace-write agent requires lane")


def validate_template_files(node: dict[str, Any], directory: Path, errors: list[str], label: str) -> None:
    if node["type"] == "agent":
        validate_agent_files(node, directory, errors, label)
    elif node["type"] == "command" and node.get("writes") and not node.get("lane"):
        errors.append(f"{label}: writing command requires lane")
    elif node["type"] == "loop":
        body_ids = [item.get("id") for item in node["body"]]
        if any(not value or not NODE_ID_RE.fullmatch(value) for value in body_ids):
            errors.append(f"{label}: every loop body node needs a valid id")
        if len(body_ids) != len(set(body_ids)):
            errors.append(f"{label}: loop body IDs must be unique")
        if node["until"]["source"] not in set(body_ids):
            errors.append(f"{label}: until.source must reference a body node")
        for item in node["body"]:
            validate_template_files(item, directory, errors, f"{label}.{item.get('id')}")


def ensure_relative_file(directory: Path, relative: str, errors: list[str], label: str) -> None:
    candidate = (directory / relative).resolve()
    try:
        candidate.relative_to(directory.resolve())
    except ValueError:
        errors.append(f"{label}: path escapes workflow directory: {relative}")
        return
    if not candidate.is_file():
        errors.append(f"{label}: file not found: {relative}")


def detect_cycles(graph: dict[str, list[str]]) -> list[str]:
    visiting: set[str] = set()
    visited: set[str] = set()
    errors: list[str] = []

    def visit(node: str, path: list[str]) -> None:
        if node in visiting:
            errors.append("cycle detected: " + " -> ".join(path + [node]))
            return
        if node in visited:
            return
        visiting.add(node)
        for dependency in graph.get(node, []):
            visit(dependency, path + [node])
        visiting.remove(node)
        visited.add(node)

    for node in graph:
        visit(node, [])
    return errors


def validate_lanes(spec: dict[str, Any], errors: list[str]) -> None:
    declared: set[str] = set()
    for node in spec["nodes"]:
        if node.get("lane"):
            declared.add(node["lane"])
        if node["type"] == "foreach" and node["template"].get("lane"):
            # A templated lane cannot be resolved statically, but is still valid.
            declared.add(node["template"]["lane"])
        if node["type"] == "foreach" and node["template"]["type"] == "loop":
            declared.update(item["lane"] for item in node["template"]["body"] if item.get("lane"))
        if node["type"] == "loop":
            declared.update(item["lane"] for item in node["body"] if item.get("lane"))
    for node in spec["nodes"]:
        if node["type"] != "merge":
            continue
        for lane in node.get("lanes", []):
            if "${{" not in lane and lane not in declared:
                errors.append(f"{node['id']}: merge references undeclared lane {lane}")


def parse_inputs(values: list[str], definitions: dict[str, Any]) -> dict[str, Any]:
    raw: dict[str, str] = {}
    for value in values:
        if "=" not in value:
            raise WorkflowError(f"input must use key=value: {value}")
        key, item = value.split("=", 1)
        raw[key] = item
    unknown = set(raw) - set(definitions)
    if unknown:
        raise WorkflowError("unknown input(s): " + ", ".join(sorted(unknown)))
    result: dict[str, Any] = {}
    for key, definition in definitions.items():
        if key in raw:
            result[key] = coerce_input(raw[key], definition["type"])
        elif "default" in definition:
            result[key] = definition["default"]
        elif definition.get("required"):
            raise WorkflowError(f"missing required input: {key}")
    return result


def coerce_input(value: str, kind: str) -> Any:
    if kind == "string":
        return value
    if kind == "integer":
        return int(value)
    if kind == "number":
        return float(value)
    if kind == "boolean":
        lowered = value.lower()
        if lowered not in {"true", "false"}:
            raise WorkflowError(f"invalid boolean: {value}")
        return lowered == "true"
    parsed = json.loads(value)
    expected = list if kind == "array" else dict
    if not isinstance(parsed, expected):
        raise WorkflowError(f"expected {kind}: {value}")
    return parsed


def resolve_path(value: Any, path: list[str]) -> Any:
    current = value
    for part in path:
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            raise WorkflowError(f"template path not found: {'.'.join(path)}")
    return current


def template_context(
    inputs: dict[str, Any],
    outputs: dict[str, Any],
    item: dict[str, Any] | None = None,
    iteration: int | None = None,
) -> dict[str, Any]:
    return {
        "inputs": inputs,
        "nodes": {key: {"output": value} for key, value in outputs.items()},
        "item": item or {},
        "loop": {"iteration": iteration},
    }


def resolve_expression(expression: str, context: dict[str, Any]) -> Any:
    parts = expression.strip().split(".")
    if not parts or parts[0] not in {"inputs", "nodes", "item", "loop"}:
        raise WorkflowError(f"unsupported template expression: {expression}")
    return resolve_path(context, parts)


def render(
    value: Any, context: dict[str, Any], defer_roots: set[str] | None = None
) -> Any:
    deferred = defer_roots or set()
    if isinstance(value, str):
        exact = TEMPLATE_RE.fullmatch(value)
        if exact:
            if exact.group(1).strip().split(".", 1)[0] in deferred:
                return value
            return copy.deepcopy(resolve_expression(exact.group(1), context))

        def replace(match: re.Match[str]) -> str:
            if match.group(1).strip().split(".", 1)[0] in deferred:
                return match.group(0)
            resolved = resolve_expression(match.group(1), context)
            if isinstance(resolved, (dict, list)):
                return json.dumps(resolved, ensure_ascii=False)
            return str(resolved)

        return TEMPLATE_RE.sub(replace, value)
    if isinstance(value, list):
        return [render(item, context, deferred) for item in value]
    if isinstance(value, dict):
        return {key: render(item, context, deferred) for key, item in value.items()}
    return value


def initial_runtime_nodes(spec: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        node["id"]: {
            "id": node["id"],
            "definition": copy.deepcopy(node),
            "type": node["type"],
            "depends_on": list(node.get("depends_on", [])),
            "status": "pending",
            "attempt": 0,
            "created_at": utc_now(),
        }
        for node in spec["nodes"]
    }


def summarize(state: dict[str, Any], run_dir: Path | None = None) -> dict[str, Any]:
    counts: dict[str, int] = {}
    for node in state["nodes"].values():
        counts[node["status"]] = counts.get(node["status"], 0) + 1
    dynamic_open = any(
        node["type"] in {"foreach", "loop"}
        and node["status"] not in TERMINAL
        and not node.get("sealed", False)
        for node in state["nodes"].values()
    )
    terminal = sum(counts.get(name, 0) for name in TERMINAL)
    total = len(state["nodes"])
    summary = {
        "run_id": state["run_id"],
        "workflow": state["workflow"],
        "status": state["status"],
        "counts": counts,
        "total": total,
        "terminal": terminal,
        "progress_percent": None if dynamic_open or total == 0 else round(terminal * 100 / total, 1),
        "running": sorted(node["id"] for node in state["nodes"].values() if node["status"] == "running"),
        "waiting_barriers": sorted(
            node["id"] for node in state["nodes"].values() if node["status"] == "waiting_barrier"
        ),
        "loops": {
            node["id"]: {
                "iteration": node.get("iteration", 0),
                "max_iterations": node["definition"].get("max_iterations"),
                "sealed": node.get("sealed", False),
            }
            for node in state["nodes"].values()
            if node["type"] == "loop"
        },
        "updated_at": state["updated_at"],
    }
    if run_dir is not None:
        metadata = runner_metadata(run_dir)
        if metadata:
            heartbeat_stale = False
            if state["status"] not in RUN_TERMINAL:
                try:
                    updated_at = datetime.fromisoformat(state["updated_at"])
                    heartbeat_stale = (datetime.now(UTC) - updated_at).total_seconds() > HEARTBEAT_SECONDS * 3
                except (TypeError, ValueError):
                    heartbeat_stale = True
            metadata["heartbeat_stale"] = heartbeat_stale
            summary["runner"] = metadata
    return summary


@dataclass
class ProcessResult:
    returncode: int
    stdout: str
    stderr: str
    timed_out: bool = False


async def run_process(
    argv: list[str], cwd: Path, timeout: int | None, env: dict[str, str] | None = None
) -> ProcessResult:
    process = await asyncio.create_subprocess_exec(
        *argv,
        cwd=str(cwd),
        env=env,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout)
    except asyncio.CancelledError:
        os.killpg(process.pid, signal.SIGTERM)
        try:
            await asyncio.wait_for(process.communicate(), timeout=5)
        except TimeoutError:
            os.killpg(process.pid, signal.SIGKILL)
            await process.communicate()
        raise
    except TimeoutError:
        os.killpg(process.pid, signal.SIGTERM)
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=5)
        except TimeoutError:
            os.killpg(process.pid, signal.SIGKILL)
            stdout, stderr = await process.communicate()
        return ProcessResult(process.returncode or 124, stdout.decode(), stderr.decode(), True)
    return ProcessResult(process.returncode or 0, stdout.decode(), stderr.decode())


class RunLock:
    def __init__(self, path: Path):
        self.path = path
        self.fd: int | None = None

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            try:
                lock = read_json(self.path)
                pid = int(lock["pid"])
                os.kill(pid, 0)
            except (OSError, ValueError, KeyError, json.JSONDecodeError):
                self.path.unlink(missing_ok=True)
                return self.acquire()
            raise WorkflowError(f"run already owned by pid {pid}")
        os.write(self.fd, json.dumps({"pid": os.getpid(), "acquired_at": utc_now()}).encode())

    def release(self) -> None:
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None
        self.path.unlink(missing_ok=True)

    def __enter__(self) -> "RunLock":
        self.acquire()
        return self

    def __exit__(self, *_: Any) -> None:
        self.release()


class Runner:
    def __init__(
        self,
        root: Path,
        directory: Path,
        spec: dict[str, Any],
        run_dir: Path,
        state: dict[str, Any],
        allow_source_update: bool = False,
    ):
        self.root = root
        self.directory = directory
        self.spec = spec
        self.run_dir = run_dir
        self.state = state
        self.allow_source_update = allow_source_update
        self.running: dict[str, asyncio.Task[dict[str, Any]]] = {}
        self.lane_locks: dict[str, asyncio.Lock] = {}
        self.worker_semaphore = asyncio.Semaphore(
            spec["execution"].get("max_concurrency", DEFAULT_CONCURRENCY)
        )
        self.cancel_requested = False

    @property
    def state_path(self) -> Path:
        return self.run_dir / "state.json"

    @property
    def events_path(self) -> Path:
        return self.run_dir / "events.jsonl"

    def save(self) -> None:
        self.state["updated_at"] = utc_now()
        atomic_json(self.state_path, self.state)

    def event(self, kind: str, node: str | None = None, **fields: Any) -> None:
        record = {"timestamp": utc_now(), "kind": kind, "run_id": self.state["run_id"]}
        if node:
            record["node_id"] = node
        record.update(fields)
        self.events_path.parent.mkdir(parents=True, exist_ok=True)
        with self.events_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")

    def outputs(self) -> dict[str, Any]:
        return {
            node_id: node.get("output")
            for node_id, node in self.state["nodes"].items()
            if "output" in node
        }

    def dependency_states(self, node: dict[str, Any]) -> list[str]:
        return [self.state["nodes"][item]["status"] for item in node["depends_on"]]

    def ordinary_ready(self, node: dict[str, Any]) -> bool:
        statuses = self.dependency_states(node)
        return all(status == "success" for status in statuses)

    def barrier_ready(self, node: dict[str, Any]) -> tuple[bool, bool]:
        statuses = self.dependency_states(node)
        policy = node["definition"]["policy"]
        mode = policy["mode"]
        if mode == "all_success":
            return all(status == "success" for status in statuses), any(status in {"failed", "skipped", "cancelled"} for status in statuses)
        if mode == "all_terminal":
            return all(status in TERMINAL for status in statuses), False
        successes = sum(status == "success" for status in statuses)
        possible = successes + sum(status not in TERMINAL for status in statuses)
        count = policy["count"]
        return successes >= count, possible < count

    def update_pending(self) -> bool:
        changed = False
        fail_fast = self.spec["execution"]["failure_policy"] == "fail_fast"
        permanent_failure = any(node["status"] == "failed" for node in self.state["nodes"].values())
        for node in self.state["nodes"].values():
            if node["status"] not in {"pending", "waiting_barrier"}:
                continue
            if fail_fast and permanent_failure:
                node["status"] = "skipped"
                node["error"] = "workflow fail_fast policy"
                self.event("node.skipped", node["id"], reason=node["error"])
                changed = True
                continue
            if node["type"] == "barrier":
                ready, impossible = self.barrier_ready(node)
                if ready:
                    if self.evaluate_when(node):
                        node["status"] = "ready"
                    else:
                        node["status"] = "skipped"
                        self.event("node.skipped", node["id"], reason="when condition is false")
                    changed = True
                elif impossible:
                    node["status"] = "skipped"
                    node["error"] = "barrier policy cannot be satisfied"
                    self.event("node.skipped", node["id"], reason=node["error"])
                    changed = True
                elif node["depends_on"]:
                    if node["status"] != "waiting_barrier":
                        node["status"] = "waiting_barrier"
                        self.event("barrier.waiting", node["id"])
                        changed = True
                continue
            statuses = self.dependency_states(node)
            if any(status in {"failed", "skipped", "cancelled"} for status in statuses):
                node["status"] = "skipped"
                node["error"] = "dependency did not succeed"
                self.event("node.skipped", node["id"], reason=node["error"])
                changed = True
            elif all(status == "success" for status in statuses):
                if self.evaluate_when(node):
                    node["status"] = "ready"
                    self.event("node.ready", node["id"])
                else:
                    node["status"] = "skipped"
                    self.event("node.skipped", node["id"], reason="when condition is false")
                changed = True
        return changed

    def evaluate_when(self, node: dict[str, Any]) -> bool:
        condition = node["definition"].get("when")
        if not condition:
            return True
        return evaluate_condition(condition, self.outputs())

    async def execute(self) -> dict[str, Any]:
        self.state["status"] = "running"
        self.event("run.started")
        self.save()
        max_concurrency = self.spec["execution"].get("max_concurrency", DEFAULT_CONCURRENCY)
        started_at = time.monotonic()
        timeout = self.spec["execution"].get("timeout_seconds")
        while True:
            if (self.run_dir / "cancel.requested").exists():
                self.cancel_requested = True
            if timeout and time.monotonic() - started_at > timeout:
                self.cancel_requested = True
                self.state["error"] = "workflow timeout exceeded"
            if self.cancel_requested:
                finished_at = utc_now()
                for task in self.running.values():
                    task.cancel()
                for node in self.state["nodes"].values():
                    if node["status"] not in TERMINAL:
                        node["status"] = "cancelled"
                        node["finished_at"] = finished_at
                self.state["status"] = "cancelled"
                self.state["finished_at"] = finished_at
                self.event("run.cancelled")
                self.save()
                return self.state

            self.update_pending()
            ready = [
                node for node in self.state["nodes"].values()
                if node["status"] == "ready" and node["id"] not in self.running
            ]
            slots = max(0, max_concurrency - len(self.running))
            for node in ready[:slots]:
                node["status"] = "running"
                node["attempt"] += 1
                node["started_at"] = utc_now()
                self.event("node.started", node["id"], attempt=node["attempt"], type=node["type"])
                self.running[node["id"]] = asyncio.create_task(self.run_node(node))
            self.save()

            if not self.running:
                if all(node["status"] in TERMINAL for node in self.state["nodes"].values()):
                    break
                if not ready and not self.update_pending():
                    self.state["status"] = "blocked"
                    self.state["error"] = "no runnable nodes remain"
                    self.event("run.blocked")
                    self.save()
                    return self.state
                await asyncio.sleep(0)
                continue

            wait_seconds = HEARTBEAT_SECONDS
            if timeout:
                remaining = timeout - (time.monotonic() - started_at)
                wait_seconds = max(0.01, min(wait_seconds, remaining))
            done, _ = await asyncio.wait(
                self.running.values(), timeout=wait_seconds, return_when=asyncio.FIRST_COMPLETED
            )
            if not done:
                self.event("run.heartbeat", summary=summarize(self.state))
                self.save()
                continue
            for task in done:
                node_id = next(key for key, value in self.running.items() if value is task)
                del self.running[node_id]
                node = self.state["nodes"][node_id]
                try:
                    outcome = task.result()
                except asyncio.CancelledError:
                    outcome = {"status": "cancelled", "error": "cancelled"}
                except Exception as error:  # noqa: BLE001 - persist worker failures
                    outcome = {"status": "failed", "error": str(error)}
                if outcome["status"] == "failed" and self.should_retry(node):
                    delay = node["definition"].get("retry", {}).get("delay_seconds", 0)
                    node["status"] = "retry_wait"
                    node["error"] = outcome.get("error")
                    self.event("node.retrying", node_id, delay_seconds=delay)
                    if delay:
                        await asyncio.sleep(delay)
                    node["status"] = "ready"
                    continue
                node.update(outcome)
                node["finished_at"] = utc_now()
                self.event(
                    f"node.{node['status']}", node_id,
                    exit_code=node.get("exit_code"), error=node.get("error")
                )
                if (
                    node["status"] == "failed"
                    and self.spec["execution"]["failure_policy"] == "fail_fast"
                ):
                    for other in self.running.values():
                        other.cancel()
                self.save()

        failed = any(node["status"] == "failed" for node in self.state["nodes"].values())
        self.state["status"] = "failed" if failed else "completed"
        self.state["finished_at"] = utc_now()
        self.event(f"run.{self.state['status']}", summary=summarize(self.state))
        self.save()
        return self.state

    def should_retry(self, node: dict[str, Any]) -> bool:
        maximum = node["definition"].get("retry", {}).get("max_attempts", 1)
        return node["attempt"] < maximum

    async def run_node(self, node: dict[str, Any]) -> dict[str, Any]:
        kind = node["type"]
        if kind == "barrier":
            return {"status": "success", "output": {"opened": True}}
        if kind == "foreach":
            return await self.run_foreach(node)
        if kind == "loop":
            return await self.run_loop(node)
        if kind == "merge":
            return await self.run_merge(node)
        definition = render(node["definition"], template_context(self.state["inputs"], self.outputs()))
        lane = definition.get("lane")
        return await self.run_worker(node["id"], definition, lane)

    async def run_worker(self, node_id: str, definition: dict[str, Any], lane: str | None) -> dict[str, Any]:
        async with self.worker_semaphore:
            if lane:
                lock = self.lane_locks.setdefault(lane, asyncio.Lock())
                async with lock:
                    return await self._run_worker(node_id, definition, lane)
            return await self._run_worker(node_id, definition, None)

    async def run_embedded_node(self, child: dict[str, Any], parent: str, **event_fields: Any) -> dict[str, Any]:
        definition = child["definition"]
        maximum = definition.get("retry", {}).get("max_attempts", 1)
        delay = definition.get("retry", {}).get("delay_seconds", 0)
        while child["attempt"] < maximum:
            child["status"] = "running"
            child["attempt"] += 1
            child["started_at"] = utc_now()
            self.event("node.started", child["id"], parent=parent, attempt=child["attempt"], **event_fields)
            if definition["type"] == "loop":
                outcome = await self.run_loop(child)
            else:
                outcome = await self.run_worker(child["id"], definition, definition.get("lane"))
            child.update(outcome)
            child["finished_at"] = utc_now()
            self.event(f"node.{child['status']}", child["id"], parent=parent, **event_fields)
            self.save()
            if child["status"] == "success" or child["attempt"] >= maximum:
                return outcome
            child["status"] = "retry_wait"
            self.event("node.retrying", child["id"], parent=parent, delay_seconds=delay, **event_fields)
            self.save()
            if delay:
                await asyncio.sleep(delay)
        return {"status": "failed", "error": "retry attempts exhausted"}

    async def _run_worker(self, node_id: str, definition: dict[str, Any], lane: str | None) -> dict[str, Any]:
        cwd = self.root
        if lane:
            cwd = await asyncio.to_thread(self.ensure_lane, lane)
        if definition["type"] == "command":
            argv = [str(value) for value in definition["command"]]
            result = await run_process(argv, cwd, definition.get("timeout_seconds"))
            output: Any = {"stdout": result.stdout, "exit_code": result.returncode}
            if definition.get("capture_json") and result.returncode == 0:
                try:
                    output = json.loads(result.stdout)
                except json.JSONDecodeError as error:
                    return {"status": "failed", "exit_code": result.returncode, "error": f"invalid JSON stdout: {error}"}
            if result.returncode != 0:
                return {
                    "status": "failed",
                    "exit_code": result.returncode,
                    "timed_out": result.timed_out,
                    "error": "process timed out" if result.timed_out else trim_error(result.stderr),
                }
            if lane and definition.get("writes"):
                await asyncio.to_thread(self.checkpoint_lane, lane, node_id)
            return {"status": "success", "exit_code": result.returncode, "output": output}
        return await self.run_agent(node_id, definition, cwd, lane)

    async def run_agent(
        self, node_id: str, definition: dict[str, Any], cwd: Path, lane: str | None
    ) -> dict[str, Any]:
        maximum = self.spec["execution"].get("max_agent_calls")
        if maximum is not None and self.state.get("agent_calls", 0) >= maximum:
            return {"status": "failed", "error": "max_agent_calls exceeded"}
        self.state["agent_calls"] = self.state.get("agent_calls", 0) + 1
        prompt_path = self.directory / definition["prompt"]
        prompt = prompt_path.read_text(encoding="utf-8")
        context = {
            "workflow": self.spec["name"],
            "node": node_id,
            "inputs": self.state["inputs"],
            "dependencies": {
                dependency: self.state["nodes"][dependency].get("output")
                for dependency in self.state["nodes"].get(node_id, {}).get("depends_on", [])
                if dependency in self.state["nodes"]
            },
            "item": self.state["nodes"].get(node_id, {}).get("item"),
            "loop_iteration": self.state["nodes"].get(node_id, {}).get("iteration"),
        }
        prompt += "\n\n<workflow_context>\n" + json.dumps(context, ensure_ascii=False, indent=2) + "\n</workflow_context>\n"
        result_path = self.run_dir / "results" / f"{safe_name(node_id)}.json"
        result_path.parent.mkdir(parents=True, exist_ok=True)
        argv = [
            os.environ.get("CODEX_WORKFLOW_CODEX_BIN", "codex"),
            "exec", "--json", "--sandbox", definition.get("sandbox", "read-only"),
            "-C", str(cwd), "-o", str(result_path),
        ]
        if definition.get("model"):
            argv.extend(["--model", definition["model"]])
        if definition.get("output_schema"):
            argv.extend(["--output-schema", str((self.directory / definition["output_schema"]).resolve())])
        argv.append(prompt)
        result = await run_process(argv, cwd, definition.get("timeout_seconds", DEFAULT_AGENT_TIMEOUT))
        thread_id = None
        turn_completed = False
        turn_failed = False
        usage = None
        errors: list[str] = []
        for line in result.stdout.splitlines():
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            kind = event.get("type")
            if kind == "thread.started":
                thread_id = event.get("thread_id")
            elif kind == "turn.completed":
                turn_completed = True
                usage = event.get("usage")
            elif kind == "turn.failed":
                turn_failed = True
                errors.append(str(event.get("error", "turn failed")))
            elif kind == "error":
                errors.append(str(event.get("message") or event.get("error") or "Codex error"))
        outcome: dict[str, Any] = {
            "exit_code": result.returncode,
            "timed_out": result.timed_out,
            "thread_id": thread_id,
            "usage": usage,
        }
        if result.returncode != 0 or turn_failed or not turn_completed:
            message = "Codex process timed out" if result.timed_out else (
                "; ".join(errors) or result.stderr or "Codex turn did not complete"
            )
            outcome.update(status="failed", error=trim_error(message))
            return outcome
        if not result_path.is_file():
            outcome.update(status="failed", error="Codex did not write the final result")
            return outcome
        raw = result_path.read_text(encoding="utf-8")
        try:
            output = json.loads(raw) if definition.get("output_schema") else raw.rstrip()
            if definition.get("output_schema"):
                jsonschema.validate(output, read_json(self.directory / definition["output_schema"]))
        except (json.JSONDecodeError, jsonschema.ValidationError) as error:
            outcome.update(status="failed", error=f"invalid structured result: {error}")
            return outcome
        if lane and definition.get("sandbox", "read-only") == "workspace-write":
            await asyncio.to_thread(self.checkpoint_lane, lane, node_id)
        outcome.update(status="success", output=output)
        return outcome

    async def run_foreach(self, node: dict[str, Any]) -> dict[str, Any]:
        definition = node["definition"]
        context = template_context(self.state["inputs"], self.outputs())
        items = render(definition["items"], context)
        if not isinstance(items, list):
            return {"status": "failed", "error": "foreach items must resolve to an array"}
        if len(items) > definition["max_items"]:
            return {"status": "failed", "error": "foreach max_items exceeded"}
        child_ids: list[str] = []
        for item in items:
            if not isinstance(item, dict) or not isinstance(item.get("id"), str):
                return {"status": "failed", "error": "foreach items require stable string id"}
            child_id = f"{node['id']}[{safe_name(item['id'])}]"
            child_ids.append(child_id)
            if child_id in self.state["nodes"]:
                continue
            child_definition = render(
                copy.deepcopy(definition["template"]),
                template_context(self.state["inputs"], self.outputs(), item=item),
                defer_roots={"loop"},
            )
            child_definition["id"] = child_id
            self.state["nodes"][child_id] = {
                "id": child_id, "definition": child_definition, "type": child_definition["type"],
                "depends_on": [], "status": "ready", "attempt": 0, "parent": node["id"],
                "item": item, "created_at": utc_now(),
            }
            self.event("node.discovered", child_id, parent=node["id"])
        node["sealed"] = True
        node["children"] = child_ids
        self.event("dynamic_group.sealed", node["id"], children=child_ids)
        self.save()
        results_by_id: dict[str, Any] = {}

        async def execute_child(child_id: str) -> tuple[str, dict[str, Any]]:
            child = self.state["nodes"][child_id]
            if child["status"] == "success":
                return child_id, {"status": "success", "output": child.get("output")}
            if child["status"] == "skipped":
                return child_id, {"status": "skipped"}
            condition = child["definition"].get("when")
            if condition and not evaluate_condition(condition, self.outputs()):
                child["status"] = "skipped"
                child["finished_at"] = utc_now()
                self.event("node.skipped", child_id, parent=node["id"], reason="when condition is false")
                self.save()
                return child_id, {"status": "skipped"}
            outcome = await self.run_embedded_node(child, node["id"])
            return child_id, outcome

        outcomes = await asyncio.gather(*(execute_child(child_id) for child_id in child_ids))
        for child_id, outcome in outcomes:
            if outcome["status"] not in {"success", "skipped"}:
                return {"status": "failed", "error": f"foreach child failed: {child_id}"}
            results_by_id[child_id] = outcome.get("output")
        results = [results_by_id[child_id] for child_id in child_ids]
        return {"status": "success", "output": results, "sealed": True, "children": child_ids}

    async def run_loop(self, node: dict[str, Any]) -> dict[str, Any]:
        definition = node["definition"]
        iteration = int(node.get("iteration", 0))
        last_outputs: dict[str, Any] = node.get("last_outputs", {})
        evaluated = set(node.get("evaluated_iterations", []))
        resume_iteration = iteration > 0 and iteration not in evaluated
        while iteration < definition["max_iterations"] or resume_iteration:
            if not resume_iteration:
                iteration += 1
            resume_iteration = False
            node["iteration"] = iteration
            self.event("loop.iteration.started", node["id"], iteration=iteration)
            previous: str | None = None
            iteration_outputs: dict[str, Any] = {}
            for template in definition["body"]:
                child_id = f"{node['id']}.{template['id']}@{iteration}"
                child_definition = render(
                    copy.deepcopy(template),
                    template_context(self.state["inputs"], {**self.outputs(), **iteration_outputs}, iteration=iteration),
                )
                child_definition["id"] = child_id
                child = self.state["nodes"].setdefault(
                    child_id,
                    {"id": child_id, "definition": child_definition, "type": child_definition["type"],
                     "depends_on": [previous] if previous else [], "status": "pending", "attempt": 0,
                     "parent": node["id"], "iteration": iteration, "created_at": utc_now()},
                )
                condition = child_definition.get("when")
                condition_outputs = {**self.outputs(), **iteration_outputs}
                if child["status"] != "success" and condition and not evaluate_condition(
                    condition, condition_outputs
                ):
                    child["status"] = "skipped"
                    child["finished_at"] = utc_now()
                    self.event(
                        "node.skipped", child_id, parent=node["id"], iteration=iteration,
                        reason="when condition is false",
                    )
                    self.save()
                    previous = child_id
                    continue
                if child["status"] != "success":
                    await self.run_embedded_node(child, node["id"], iteration=iteration)
                if child["status"] != "success":
                    return {"status": "failed", "error": f"loop child failed: {child_id}", "iteration": iteration}
                iteration_outputs[template["id"]] = child.get("output")
                previous = child_id
            last_outputs = iteration_outputs
            node["last_outputs"] = last_outputs
            if evaluate_condition(definition["until"], iteration_outputs, self.state["nodes"], node["id"], iteration):
                node["sealed"] = True
                self.event("loop.completed", node["id"], iteration=iteration)
                return {"status": "success", "output": last_outputs, "iteration": iteration, "sealed": True}
            evaluated.add(iteration)
            node["evaluated_iterations"] = sorted(evaluated)
            self.save()
        node["sealed"] = True
        if definition["on_exhausted"] == "continue_partial":
            return {"status": "success", "output": last_outputs, "iteration": iteration, "sealed": True, "exhausted": True}
        return {"status": "failed", "error": "loop max_iterations exhausted", "iteration": iteration, "sealed": True}

    def ensure_lane(self, lane: str) -> Path:
        if not re.fullmatch(r"[A-Za-z0-9._-]+", lane):
            raise WorkflowError(f"invalid lane name: {lane}")
        path = self.run_dir / "worktrees" / lane
        branch = f"codex-dynamic-workflow/{self.state['run_id']}/{lane}"
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            run_git(self.root, "worktree", "add", "-b", branch, str(path), self.state["base_commit"])
        self.state.setdefault("lanes", {}).setdefault(lane, {"path": str(path), "branch": branch})
        return path

    def checkpoint_lane(self, lane: str, node_id: str) -> None:
        path = self.ensure_lane(lane)
        run_git(path, "add", "-A")
        staged = run_git(path, "diff", "--cached", "--quiet", check=False)
        if staged.returncode == 1:
            run_git(path, "commit", "-m", f"workflow: {node_id}")
        head = run_git(path, "rev-parse", "HEAD").stdout.strip()
        self.state["lanes"][lane]["head"] = head

    async def run_merge(self, node: dict[str, Any]) -> dict[str, Any]:
        definition = render(node["definition"], template_context(self.state["inputs"], self.outputs()))
        lane = f"integration-{safe_name(node['id'])}"
        path = await asyncio.to_thread(self.ensure_lane, lane)
        heads: list[str] = []
        source_lanes = list(definition.get("lanes", []))
        for group_id in definition.get("lane_groups", []):
            source_lanes.extend(self.descendant_lanes(group_id))
        source_lanes = list(dict.fromkeys(source_lanes))
        if not source_lanes:
            return {"status": "failed", "error": "merge resolved no source lanes"}
        for source_lane in source_lanes:
            lane_state = self.state.get("lanes", {}).get(source_lane)
            if not lane_state or not lane_state.get("head"):
                return {"status": "failed", "error": f"lane has no checkpoint: {source_lane}"}
            head = lane_state["head"]
            commits = run_git(
                self.root, "rev-list", "--reverse", f"{self.state['base_commit']}..{head}"
            ).stdout.splitlines()
            if not commits:
                return {"status": "failed", "error": f"lane has no commits: {source_lane}"}
            for commit in commits:
                result = await run_process(
                    ["git", "cherry-pick", commit], path, definition.get("timeout_seconds")
                )
                if result.returncode == 0:
                    continue
                if not definition.get("conflict_prompt"):
                    await run_process(["git", "cherry-pick", "--abort"], path, 30)
                    return {"status": "failed", "error": f"merge conflict in lane {source_lane}"}
                conflict_definition = {
                    "type": "agent", "prompt": definition["conflict_prompt"], "sandbox": "workspace-write",
                    "timeout_seconds": definition.get("timeout_seconds", DEFAULT_AGENT_TIMEOUT),
                }
                outcome = await self.run_agent(f"{node['id']}.resolve-{source_lane}", conflict_definition, path, None)
                if outcome["status"] != "success":
                    return {"status": "failed", "error": f"conflict resolver failed for {source_lane}"}
                await run_process(["git", "add", "-A"], path, 30)
                continued = await run_process(["git", "cherry-pick", "--continue"], path, 60)
                if continued.returncode != 0:
                    return {"status": "failed", "error": trim_error(continued.stderr)}
            heads.append(head)
        if definition.get("verify_command"):
            verified = await run_process(definition["verify_command"], path, definition.get("timeout_seconds"))
            if verified.returncode != 0:
                return {"status": "failed", "exit_code": verified.returncode, "error": trim_error(verified.stderr)}
        head = run_git(path, "rev-parse", "HEAD").stdout.strip()
        self.state["lanes"][lane]["head"] = head
        output = definition["output"]
        if output["mode"] == "patch":
            relative = output.get("path", f"artifacts/{safe_name(node['id'])}.patch")
            destination = (self.run_dir / relative).resolve()
            if self.run_dir.resolve() not in destination.parents:
                return {"status": "failed", "error": "patch path escapes run directory"}
            destination.parent.mkdir(parents=True, exist_ok=True)
            patch = run_git(path, "diff", "--binary", f"{self.state['base_commit']}..HEAD").stdout
            destination.write_text(patch, encoding="utf-8")
            result_output = {"mode": "patch", "path": str(destination), "head": head}
        elif output["mode"] == "branch":
            result_output = {"mode": "branch", "branch": self.state["lanes"][lane]["branch"], "head": head}
        else:
            if not self.allow_source_update:
                return {"status": "failed", "error": "source output requires --allow-source-update"}
            if run_git(self.root, "status", "--porcelain").stdout.strip():
                return {"status": "failed", "error": "source worktree is not clean"}
            current = run_git(self.root, "rev-parse", "HEAD").stdout.strip()
            if current != self.state["base_commit"]:
                return {"status": "failed", "error": "source HEAD moved since run start"}
            branch = run_git(self.root, "branch", "--show-current").stdout.strip()
            if not branch:
                return {"status": "failed", "error": "source is detached"}
            run_git(self.root, "merge", "--ff-only", self.state["lanes"][lane]["branch"])
            result_output = {"mode": "source", "branch": branch, "head": head}
        return {"status": "success", "output": result_output}

    def descendant_lanes(self, group_id: str) -> list[str]:
        lanes: list[str] = []
        for candidate in self.state["nodes"].values():
            current = candidate
            is_descendant = False
            seen: set[str] = set()
            while current.get("parent") and current["id"] not in seen:
                seen.add(current["id"])
                if current["parent"] == group_id:
                    is_descendant = True
                    break
                current = self.state["nodes"].get(current["parent"], {})
            if is_descendant:
                lane = candidate.get("definition", {}).get("lane")
                if lane and lane in self.state.get("lanes", {}):
                    lanes.append(lane)
        return list(dict.fromkeys(lanes))


def trim_error(value: str, limit: int = 2000) -> str:
    value = value.strip()
    return value if len(value) <= limit else value[-limit:]


def safe_name(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-")
    if not normalized:
        normalized = hashlib.sha256(value.encode()).hexdigest()[:12]
    return normalized[:100]


def evaluate_condition(
    condition: dict[str, Any],
    outputs: dict[str, Any],
    runtime_nodes: dict[str, dict[str, Any]] | None = None,
    loop_id: str | None = None,
    iteration: int | None = None,
) -> bool:
    source = condition["source"]
    value = outputs.get(source)
    if "exit_code" in condition:
        if runtime_nodes is not None and loop_id is not None and iteration is not None:
            child = runtime_nodes.get(f"{loop_id}.{source}@{iteration}", {})
            return child.get("exit_code") == condition["exit_code"]
        if isinstance(value, dict):
            return value.get("exit_code") == condition["exit_code"]
        return False
    try:
        actual = resolve_path(value, condition["field"].split(".")) if condition.get("field") else value
    except WorkflowError:
        return False
    return actual == condition.get("equals")


def workflow_writes(spec: dict[str, Any]) -> bool:
    for node in spec["nodes"]:
        if node["type"] == "agent" and node.get("sandbox") == "workspace-write":
            return True
        if node["type"] == "command" and node.get("writes"):
            return True
        if node["type"] == "merge":
            return True
        if node["type"] == "foreach":
            template = node["template"]
            if template.get("sandbox") == "workspace-write" or template.get("writes"):
                return True
            if template.get("type") == "loop" and any(
                item.get("sandbox") == "workspace-write" or item.get("writes")
                for item in template["body"]
            ):
                return True
        if node["type"] == "loop":
            if any(item.get("sandbox") == "workspace-write" or item.get("writes") for item in node["body"]):
                return True
    return False


def resolve_base(root: Path, requested: str | None) -> str:
    revision = requested or "HEAD"
    result = run_git(root, "rev-parse", "--verify", f"{revision}^{{commit}}", check=False)
    if result.returncode != 0:
        raise WorkflowError(f"invalid base commit: {revision}")
    return result.stdout.strip()


def ensure_git_identity(root: Path) -> None:
    for key in ("user.name", "user.email"):
        result = run_git(root, "config", "--get", key, check=False)
        if result.returncode != 0 or not result.stdout.strip():
            raise WorkflowError(
                "writing workflow requires a configured Git user.name and user.email"
            )


def make_state(
    root: Path, spec: dict[str, Any], inputs: dict[str, Any], run_id: str, base_commit: str
) -> dict[str, Any]:
    return {
        "version": 1,
        "run_id": run_id,
        "workflow": spec["name"],
        "repository": str(root),
        "base_commit": base_commit,
        "inputs": inputs,
        "status": "created",
        "created_at": utc_now(),
        "updated_at": utc_now(),
        "agent_calls": 0,
        "nodes": initial_runtime_nodes(spec),
        "lanes": {},
    }


def load_run(root: Path, run_id: str) -> tuple[dict[str, Any], Path]:
    require_run_id(run_id)
    directory = state_root(root) / run_id
    state_file = directory / "state.json"
    if not state_file.is_file():
        raise WorkflowError(f"run not found: {run_id}")
    state = read_json(state_file)
    if state.get("run_id") != run_id or state.get("repository") != str(root):
        raise WorkflowError(f"run state identity mismatch: {run_id}")
    return state, directory


def list_runs(root: Path) -> list[tuple[dict[str, Any], Path]]:
    runs: list[tuple[dict[str, Any], Path]] = []
    directory = state_root(root)
    if not directory.is_dir():
        return runs
    for state_file in directory.glob("*/state.json"):
        try:
            state = read_json(state_file)
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        run_id = state.get("run_id")
        if (
            state.get("repository") == str(root)
            and isinstance(run_id, str)
            and RUN_ID_RE.fullmatch(run_id)
        ):
            runs.append((state, state_file.parent))
    return sorted(
        runs,
        key=lambda item: (item[0].get("created_at", ""), item[0].get("run_id", "")),
        reverse=True,
    )


def resolve_run(root: Path, run_id: str | None) -> tuple[dict[str, Any], Path]:
    if run_id:
        return load_run(root, run_id)
    runs = list_runs(root)
    if not runs:
        raise WorkflowError("no workflow runs found for this repository")
    return runs[0]


def recover_interrupted(state: dict[str, Any]) -> None:
    for node in state["nodes"].values():
        if node["status"] in {"running", "retry_wait"}:
            node["status"] = "ready"
            node["attempt"] = max(0, int(node.get("attempt", 0)) - 1)
            node["error"] = "recovered after runner interruption"
    if state["status"] in {"running", "blocked"}:
        state["status"] = "created"


def list_workflows(root: Path) -> list[str]:
    directory = root / ".agents" / "codex-dynamic-workflows"
    if not directory.is_dir():
        return []
    return sorted(path.parent.name for path in directory.glob("*/workflow.yaml"))


def plan_text(spec: dict[str, Any]) -> str:
    lines = [f"workflow: {spec['name']}", f"failure_policy: {spec['execution']['failure_policy']}", "nodes:"]
    for node in spec["nodes"]:
        dependencies = ", ".join(node.get("depends_on", [])) or "-"
        detail = f"  {node['id']} [{node['type']}] <- {dependencies}"
        if node["type"] == "loop":
            detail += f" (max {node['max_iterations']} iterations)"
        elif node["type"] == "foreach":
            detail += f" (max {node['max_items']} items)"
        elif node["type"] == "merge":
            sources = list(node.get("lanes", [])) + [f"group:{item}" for item in node.get("lane_groups", [])]
            detail += f" ({node['output']['mode']}: {', '.join(sources)})"
        lines.append(detail)
    return "\n".join(lines)


def prepare_new_run(
    root: Path, args: argparse.Namespace
) -> tuple[dict[str, Any], Path, dict[str, Any], Path]:
    spec, directory = load_workflow(root, args.name)
    errors = validate_workflow(spec, directory)
    if errors:
        raise WorkflowError("invalid workflow:\n  " + "\n  ".join(errors))
    inputs = parse_inputs(args.input, spec.get("inputs", {}))
    run_id = args.run_id or f"{spec['name']}-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"
    require_run_id(run_id)
    run_dir = state_root(root) / run_id
    if run_dir.exists():
        raise WorkflowError(f"run already exists: {run_id}")
    if workflow_writes(spec):
        ensure_git_identity(root)
        if not args.base and run_git(root, "status", "--porcelain").stdout.strip():
            raise WorkflowError("writing workflow requires a clean source worktree or explicit --base <commit>")
    base_commit = resolve_base(root, args.base)
    run_dir.mkdir(parents=True)
    state = make_state(root, spec, inputs, run_id, base_commit)
    atomic_json(run_dir / "workflow.snapshot.json", spec)
    shutil.copytree(directory, run_dir / "workflow")
    atomic_json(run_dir / "state.json", state)
    return state, run_dir, spec, run_dir / "workflow"


def register_runner(run_dir: Path, pid: int, mode: str) -> dict[str, Any]:
    metadata = {
        "pid": pid,
        "mode": mode,
        "started_at": utc_now(),
        "log_path": str(run_dir / "logs" / "runner.log"),
    }
    atomic_json(run_dir / "runner.json", metadata)
    return metadata


def spawn_worker(
    root: Path, run_dir: Path, run_id: str, allow_source_update: bool, mode: str
) -> subprocess.Popen[bytes]:
    logs = run_dir / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    log_handle = (logs / "runner.log").open("ab", buffering=0)
    argv = [sys.executable, str(Path(__file__).resolve()), "_worker", run_id]
    if allow_source_update:
        argv.append("--allow-source-update")
    try:
        process = subprocess.Popen(
            argv,
            cwd=str(root),
            stdin=subprocess.DEVNULL,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            close_fds=True,
        )
    except Exception:
        log_handle.close()
        raise
    log_handle.close()
    register_runner(run_dir, process.pid, mode)
    return process


def wait_for_worker_start(
    state: dict[str, Any], run_dir: Path, process: subprocess.Popen[bytes], timeout: float = 2.0
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        current = read_json(run_dir / "state.json")
        if current["status"] != "created":
            return current
        if process.poll() is not None:
            current["status"] = "failed"
            current["error"] = f"background runner exited during startup with code {process.returncode}"
            current["finished_at"] = utc_now()
            current["updated_at"] = utc_now()
            atomic_json(run_dir / "state.json", current)
            return current
        time.sleep(0.05)
    return read_json(run_dir / "state.json")


def command_start(args: argparse.Namespace) -> int:
    root = find_repo(Path.cwd())
    state, run_dir, _, _ = prepare_new_run(root, args)
    try:
        process = spawn_worker(
            root, run_dir, state["run_id"], args.allow_source_update, "detached-start"
        )
    except OSError as error:
        state["status"] = "failed"
        state["error"] = f"could not start background runner: {error}"
        state["finished_at"] = utc_now()
        state["updated_at"] = utc_now()
        atomic_json(run_dir / "state.json", state)
        raise WorkflowError(state["error"]) from error
    current = wait_for_worker_start(state, run_dir, process)
    output = summarize(current, run_dir)
    output["detached"] = True
    print(json.dumps(output, ensure_ascii=False, indent=2))
    return 0 if current["status"] not in {"failed", "cancelled"} else 1


async def command_run(args: argparse.Namespace, resume: bool = False) -> int:
    root = find_repo(Path.cwd())
    if resume:
        state, run_dir = load_run(root, args.run_id)
        if state["status"] in RUN_TERMINAL:
            raise WorkflowError(f"run is already {state['status']}")
        spec = read_json(run_dir / "workflow.snapshot.json")
        directory = run_dir / "workflow"
        if not directory.is_dir():
            raise WorkflowError("run is missing its workflow bundle")
        recover_interrupted(state)
    else:
        state, run_dir, spec, directory = prepare_new_run(root, args)
    errors = validate_workflow(spec, directory)
    if errors:
        raise WorkflowError("invalid workflow:\n  " + "\n  ".join(errors))
    if not (run_dir / "runner.json").exists():
        register_runner(run_dir, os.getpid(), "foreground-resume" if resume else "foreground-run")
    with RunLock(run_dir / "run.lock"):
        runner = Runner(root, directory, spec, run_dir, state, getattr(args, "allow_source_update", False))
        final = await runner.execute()
    print(json.dumps(summarize(final, run_dir), ensure_ascii=False, indent=2))
    return 0 if final["status"] == "completed" else 1


def build_result(state: dict[str, Any], run_dir: Path) -> dict[str, Any]:
    result_ready = state["status"] in RUN_TERMINAL
    spec = read_json(run_dir / "workflow.snapshot.json")
    runtime_outputs = {
        node_id: node.get("output")
        for node_id, node in state["nodes"].items()
        if "output" in node
    }
    outputs: dict[str, Any] = {}
    unresolved: dict[str, str] = {}
    declared = spec.get("result", {}).get("outputs")
    if result_ready and declared:
        context = template_context(state["inputs"], runtime_outputs)
        for name, value in declared.items():
            try:
                outputs[name] = render(value, context)
            except (KeyError, TypeError, ValueError, WorkflowError) as error:
                unresolved[name] = str(error)
    elif result_ready:
        top_level_ids = {node["id"] for node in spec["nodes"]}
        depended_on = {
            dependency
            for node in spec["nodes"]
            for dependency in node.get("depends_on", [])
        }
        fallback_ids = {
            node["id"]
            for node in spec["nodes"]
            if node["id"] not in depended_on or node["type"] == "merge"
        }
        outputs = {
            node_id: state["nodes"][node_id]["output"]
            for node_id in sorted(top_level_ids & fallback_ids)
            if state["nodes"][node_id]["status"] == "success"
            and "output" in state["nodes"][node_id]
        }
    artifacts = []
    for node in state["nodes"].values():
        output = node.get("output")
        if node["type"] == "merge" and node["status"] == "success" and isinstance(output, dict):
            artifacts.append({"node_id": node["id"], **output})
    failed_nodes = [
        {
            "id": node["id"],
            "error": node.get("error"),
            "exit_code": node.get("exit_code"),
        }
        for node in state["nodes"].values()
        if node["status"] == "failed"
    ]
    return {
        "run_id": state["run_id"],
        "workflow": state["workflow"],
        "status": state["status"],
        "result_ready": result_ready,
        "created_at": state["created_at"],
        "finished_at": state.get("finished_at"),
        "progress": summarize(state, run_dir),
        "outputs": outputs,
        "unresolved_outputs": unresolved,
        "artifacts": artifacts,
        "failed_nodes": failed_nodes,
        "cancelled_nodes": sorted(
            node["id"] for node in state["nodes"].values() if node["status"] == "cancelled"
        ),
        "skipped_nodes": sorted(
            node["id"] for node in state["nodes"].values() if node["status"] == "skipped"
        ),
    }


def command_result(args: argparse.Namespace) -> int:
    root = find_repo(Path.cwd())
    state, run_dir = resolve_run(root, args.run_id)
    result = build_result(state, run_dir)
    print(json.dumps(result, ensure_ascii=False, indent=None if args.json else 2))
    if not result["result_ready"]:
        return 3
    return 0 if state["status"] == "completed" else 1


def command_runs(args: argparse.Namespace) -> int:
    root = find_repo(Path.cwd())
    runs = []
    for state, run_dir in list_runs(root):
        if args.active and state["status"] in RUN_TERMINAL:
            continue
        runs.append(summarize(state, run_dir))
    if args.json:
        print(json.dumps(runs, ensure_ascii=False))
    else:
        print(json.dumps(runs, ensure_ascii=False, indent=2))
    return 0


def command_status(args: argparse.Namespace) -> int:
    root = find_repo(Path.cwd())
    state, run_dir = resolve_run(root, args.run_id)
    while True:
        current = summarize(state, run_dir)
        print(json.dumps(current, ensure_ascii=False, indent=None if args.json else 2))
        if not args.watch or state["status"] in RUN_TERMINAL | {"blocked"}:
            return 0
        time.sleep(args.interval)
        state = read_json(run_dir / "state.json")


def command_cancel(args: argparse.Namespace) -> int:
    root = find_repo(Path.cwd())
    state, run_dir = resolve_run(root, args.run_id)
    if state["status"] in RUN_TERMINAL:
        raise WorkflowError(f"run is already {state['status']}")
    (run_dir / "cancel.requested").write_text(utc_now() + "\n", encoding="utf-8")
    print(f"cancel requested: {state['run_id']}")
    return 0


def command_resume(args: argparse.Namespace) -> int:
    root = find_repo(Path.cwd())
    state, run_dir = resolve_run(root, args.run_id)
    args.run_id = state["run_id"]
    if not args.detach:
        return asyncio.run(command_run(args, resume=True))
    if state["status"] in RUN_TERMINAL:
        raise WorkflowError(f"run is already {state['status']}")
    metadata = runner_metadata(run_dir)
    if metadata.get("alive"):
        raise WorkflowError(f"run is already owned by pid {metadata['pid']}")
    process = spawn_worker(
        root, run_dir, state["run_id"], args.allow_source_update, "detached-resume"
    )
    current = wait_for_worker_start(state, run_dir, process)
    output = summarize(current, run_dir)
    output["detached"] = True
    print(json.dumps(output, ensure_ascii=False, indent=2))
    return 0 if current["status"] not in {"failed", "cancelled"} else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("list")
    validate = sub.add_parser("validate")
    validate.add_argument("name")
    plan = sub.add_parser("plan")
    plan.add_argument("name")
    plan.add_argument("--input", action="append", default=[])
    start = sub.add_parser("start")
    start.add_argument("name")
    start.add_argument("--input", action="append", default=[])
    start.add_argument("--run-id")
    start.add_argument("--base")
    start.add_argument("--allow-source-update", action="store_true")
    run = sub.add_parser("run")
    run.add_argument("name")
    run.add_argument("--input", action="append", default=[])
    run.add_argument("--run-id")
    run.add_argument("--base")
    run.add_argument("--allow-source-update", action="store_true")
    runs = sub.add_parser("runs")
    runs.add_argument("--active", action="store_true")
    runs.add_argument("--json", action="store_true")
    status = sub.add_parser("status")
    status.add_argument("run_id", nargs="?")
    status.add_argument("--json", action="store_true")
    status.add_argument("--watch", action="store_true")
    status.add_argument("--interval", type=float, default=2.0)
    result = sub.add_parser("result")
    result.add_argument("run_id", nargs="?")
    result.add_argument("--json", action="store_true")
    resume = sub.add_parser("resume")
    resume.add_argument("run_id", nargs="?")
    resume.add_argument("--detach", action="store_true")
    resume.add_argument("--allow-source-update", action="store_true")
    cancel = sub.add_parser("cancel")
    cancel.add_argument("run_id", nargs="?")
    worker = sub.add_parser("_worker", help=argparse.SUPPRESS)
    worker.add_argument("run_id")
    worker.add_argument("--allow-source-update", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        root = find_repo(Path.cwd())
        if args.command == "list":
            for name in list_workflows(root):
                print(name)
            return 0
        if args.command in {"validate", "plan"}:
            spec, directory = load_workflow(root, args.name)
            errors = validate_workflow(spec, directory)
            if errors:
                for error in errors:
                    print(error, file=sys.stderr)
                return 1
            if args.command == "plan":
                parse_inputs(args.input, spec.get("inputs", {}))
                print(plan_text(spec))
            else:
                print(f"valid: {args.name}")
            return 0
        if args.command == "run":
            return asyncio.run(command_run(args))
        if args.command == "start":
            return command_start(args)
        if args.command == "runs":
            return command_runs(args)
        if args.command == "result":
            return command_result(args)
        if args.command == "resume":
            return command_resume(args)
        if args.command == "_worker":
            return asyncio.run(command_run(args, resume=True))
        if args.command == "cancel":
            return command_cancel(args)
        if args.command == "status":
            return command_status(args)
    except (WorkflowError, OSError, ValueError, yaml.YAMLError) as error:
        if args.command == "_worker":
            try:
                root = find_repo(Path.cwd())
                state, run_dir = load_run(root, args.run_id)
                metadata = runner_metadata(run_dir)
                if metadata.get("pid") == os.getpid() and state["status"] not in RUN_TERMINAL:
                    state["status"] = "failed"
                    state["error"] = f"background runner failed: {error}"
                    state["finished_at"] = utc_now()
                    state["updated_at"] = utc_now()
                    atomic_json(run_dir / "state.json", state)
            except Exception:
                pass
        print(f"error: {error}", file=sys.stderr)
        return 2
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
