---
name: codex-dynamic-workflow
description: >-
  Create, update, validate, start, inspect, cancel, resume, retrieve results from, and continue reusable project-local Codex headless workflows. Use when Codex should turn a stable multi-step process into a version-controlled YAML DAG; orchestrate serial, parallel, barrier, foreach, bounded-loop, command, agent, or Git-worktree merge nodes; run a workflow in the background; report DAG-only progress; or continue the user's task from a completed workflow result. Do not use for a one-off task that does not benefit from reuse.
---

# Codex Dynamic Workflow

Keep the single global Skill as the command surface. Store only project-owned definitions under `.agents/codex-dynamic-workflows/<name>/`; execute them with `scripts/workflow.py` from this Skill.

## Interpret explicit actions

Treat the first token after `$codex-dynamic-workflow` as an action:

- `start <workflow> [key=value ...]`: validate and start a detached run; return its run ID immediately.
- `status [run-id]`: report deterministic DAG progress only.
- `result [run-id]`: return terminal structured results without continuing work.
- `continue [run-id] [instruction]`: retrieve a terminal result, then continue the user's original task in this turn.
- `runs`: list recent runs for the current repository.
- `cancel [run-id]`: request cancellation.
- `resume [run-id]`: resume an interrupted run in detached mode.
- `list`, `validate`, and `plan`: inspect available workflow definitions.

When a run ID is omitted, use the newest run for the current Git repository. For `continue`, if the run is not terminal, report its progress and stop. If it failed, diagnose before taking further action. If it completed, use the result and any supplied instruction to continue the original goal. Preserve the original task's authorization: do not apply patches, push, publish, or broaden side effects unless already authorized. In a different conversation without the original goal, present the result and ask what to do next.

For `start`, translate every positional `key=value` after the workflow name into a separate runner `--input key=value` argument. Do not forward unrecognized free text as an input.

`status` and `result` are read-only. Never infer progress or results from Codex reasoning, prompts, worker logs, or file diffs.

## Run lifecycle commands

Resolve the repository root and run from it. Set `RUNNER` to this Skill's absolute `scripts/workflow.py` path, then use:

```bash
uv run "$RUNNER" list
uv run "$RUNNER" validate <name>
uv run "$RUNNER" plan <name> --input key=value
uv run "$RUNNER" start <name> --input key=value
uv run "$RUNNER" runs --json
uv run "$RUNNER" status [run-id] --json
uv run "$RUNNER" result [run-id] --json
uv run "$RUNNER" cancel [run-id]
uv run "$RUNNER" resume [run-id] --detach
```

Use foreground `run <name>` only when explicitly requested. For a writing workflow in a deliberately dirty checkout, pin an immutable base with `--base <commit>`; isolated lanes never include uncommitted source changes.

## Author a workflow

Read `references/workflow-format.md` before creating or changing `workflow.yaml`.

1. Extract stable steps, variable inputs, dependencies, concurrency, failure behavior, loop termination, result outputs, and merge points.
2. Put prompts and JSON Schemas below `.agents/codex-dynamic-workflows/<name>/`.
3. Declare every workflow's `execution.failure_policy`.
4. Bound every `foreach` and `loop`; use an explicit `merge` node for Git integration.
5. Give machine-consumed agent outputs a JSON Schema.
6. Prefer an explicit top-level `result.outputs` contract for values consumed by `$codex-dynamic-workflow result` or `continue`.
7. Reject arbitrary Python, shell interpolation, Jinja, and model-generated DAG definitions in YAML.

After editing, run `validate` and `plan`. Do not execute unless requested. Report the DAG, inputs, failure policy, loop bounds, lanes, merge output, and result contract.

## Maintain project boundaries

- Do not install a project-local `codex-dynamic-workflow` Skill or Runner. Projects own workflow definitions only.
- Treat workflow definitions as executable code and run only definitions from trusted repositories.
- Keep state outside the repository under `CODEX_WORKFLOW_STATE_DIR` or `~/.codex/workflow-runs/`.
- Allow only `read-only` and `workspace-write` agent sandboxes.
- Run write nodes in distinct worktree lanes and combine them only through explicit merge nodes.
- Require the repository's effective Git `user.name` and `user.email` for writing workflows; never replace them with a synthetic identity.
- Prefer patch or branch merge output. Source updates additionally require explicit runtime authorization and a clean source at the recorded base.
- Keep secrets out of workflow definitions, prompts, state, events, and results.
