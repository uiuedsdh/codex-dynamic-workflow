# Workflow format

## Contents

- Layout
- Top-level contract
- Node types
- Templates and outputs
- Worktree lanes and merge
- Runtime state

## Layout

Treat workflow definitions and their prompts as executable code. Validate and run only definitions from trusted repositories.

```text
.agents/
  codex-dynamic-workflows/
    <name>/
      workflow.yaml
      prompts/
      schemas/
```

## Top-level contract

```yaml
api_version: codex.workflow/v1
name: repair-and-review
description: Repair discovered targets until their verifier passes.

inputs:
  branch:
    type: string
    required: true

execution:
  max_concurrency: 4
  failure_policy: continue_independent
  timeout_seconds: 7200
  max_agent_calls: 40

result:
  outputs:
    report: "${{ nodes.report.output }}"
    deliverable: "${{ nodes.integrate.output }}"

nodes: []
```

The project stores definitions only. The global `codex-dynamic-workflow` Skill supplies the runner and Workflow Schema.

`result.outputs` is optional but recommended when a main agent will later call `$codex-dynamic-workflow result` or `continue`. Values use the same deterministic templates as node definitions. When omitted, the runner returns successful top-level leaf-node outputs and successful merge outputs.

Node IDs use lowercase letters, digits, `_`, and `-`. Dependencies must reference earlier or later declared nodes in the same workflow; validation rejects cycles and unknown references.

`failure_policy` is required:

- `fail_fast`: stop scheduling after a permanently failed node and cancel running workers.
- `continue_independent`: skip only nodes transitively dependent on failures and continue independent branches.

## Node types

### Agent

```yaml
- id: inspect
  type: agent
  prompt: prompts/inspect.md
  output_schema: schemas/inspection.json
  sandbox: read-only
  timeout_seconds: 900
  retry:
    max_attempts: 2
    delay_seconds: 5
```

Use `workspace-write` only with a `lane`. A successful write node receives a checkpoint commit.

```yaml
- id: implement-api
  type: agent
  depends_on: [inspect]
  prompt: prompts/implement.md
  sandbox: workspace-write
  lane: api
```

### Command

Command nodes use argv arrays and never invoke a shell implicitly.

```yaml
- id: unit-tests
  type: command
  depends_on: [implement-api]
  lane: api
  command: ["go", "test", "./..."]
  timeout_seconds: 1200
```

Use `capture_json: true` when stdout is JSON consumed by a template or condition.

### Barrier

Ordinary multiple dependencies already form an all-success barrier. Use an explicit barrier for other policies:

```yaml
- id: review-gate
  type: barrier
  depends_on: [review-a, review-b, review-c]
  policy:
    mode: minimum_success
    count: 2
```

Modes are `all_success`, `all_terminal`, and `minimum_success`.

### Foreach

`foreach` expands a reviewed node template from an upstream array. The group is atomically sealed after all instances are recorded.

```yaml
- id: inspect-targets
  type: agent
  prompt: prompts/discover.md
  output_schema: schemas/targets.json

- id: repair-targets
  type: foreach
  depends_on: [inspect-targets]
  items: "${{ nodes.inspect-targets.output.targets }}"
  template:
    type: agent
    prompt: prompts/repair.md
    sandbox: workspace-write
    lane: "target-${{ item.id }}"
    output_schema: schemas/repair.json
  max_items: 50
```

Every item must contain a stable string `id`. Instance IDs become `repair-targets[item-id]`.

The template may itself be a bounded `loop`, allowing each item to run an independent repair/verify cycle. References to `${{ item.* }}` are resolved before the loop is instantiated:

```yaml
template:
  type: loop
  max_iterations: 3
  on_exhausted: fail
  body:
    - id: fix
      type: agent
      prompt: prompts/fix.md
      sandbox: workspace-write
      lane: "repair-${{ item.id }}"
    - id: verify
      type: agent
      prompt: prompts/verify.md
      output_schema: schemas/verdict.json
      sandbox: read-only
      lane: "repair-${{ item.id }}"
  until:
    source: verify
    field: passed
    equals: true
```

### Loop

Loops repeat a predeclared sequential body. Conditions may inspect structured output or a command exit code.

```yaml
- id: repair-loop
  type: loop
  depends_on: [inspect]
  max_iterations: 5
  on_exhausted: fail
  body:
    - id: fix
      type: agent
      prompt: prompts/fix.md
      sandbox: workspace-write
      lane: repair
    - id: verify
      type: agent
      prompt: prompts/verify.md
      output_schema: schemas/verdict.json
      sandbox: read-only
  until:
    source: verify
    field: passed
    equals: true
```

`on_exhausted` is `fail` or `continue_partial`. A loop body is intentionally sequential in v1. Runtime IDs are `repair-loop.fix@1`, `repair-loop.verify@1`, and so on.

For deterministic command completion:

```yaml
until:
  source: verify
  exit_code: 0
```

### Merge

Merge nodes explicitly integrate Git worktree lanes:

```yaml
- id: integrate
  type: merge
  depends_on: [implement-api, implement-ui]
  lanes: [api, ui]
  output:
    mode: patch
    path: artifacts/integrated.patch
  verify_command: ["make", "test"]
  conflict_prompt: prompts/resolve-conflicts.md
```

Output modes:

- `patch`: produce a patch relative to the recorded base commit.
- `branch`: leave an integration branch and report its name.
- `source`: fast-forward the caller's branch only with runtime authorization and safety checks.

Use `lane_groups` to merge every runtime lane discovered beneath a `foreach` or `loop` control node:

```yaml
- id: integrate-dynamic-repairs
  type: merge
  depends_on: [repair-targets]
  lane_groups: [repair-targets]
  output:
    mode: patch
    path: artifacts/repairs.patch
```

## Templates and outputs

Supported exact or embedded template references:

```text
${{ inputs.name }}
${{ nodes.node-id.output.field }}
${{ item.field }}
${{ loop.iteration }}
```

Do not use arbitrary expressions. When an exact template resolves to an object or array, it retains its type; embedded values become strings.

Agent prompts automatically receive a JSON context block containing workflow inputs, dependency outputs, current foreach item, and loop iteration. The runner saves only the final structured result, not model reasoning.

## Worktree lanes and merge

- A lane is created from the immutable base commit on first use.
- A writing workflow requires a clean source worktree unless `run --base <commit>` explicitly pins an immutable base; uncommitted source changes are never copied into lanes.
- Nodes in the same lane execute serially and reuse its worktree.
- Successful write nodes create checkpoint commits using the repository's effective Git `user.name` and `user.email`. A writing workflow fails before execution if that identity is unavailable.
- Parallel lanes never edit the source worktree.
- Merge nodes create a separate integration lane and cherry-pick each lane head in declared order.
- A conflict resolver runs only when `conflict_prompt` is declared; otherwise the merge fails safely.

## Runtime state

State defaults to `~/.codex/workflow-runs/<repository-id>/<run-id>/` and can be redirected with `CODEX_WORKFLOW_STATE_DIR`.

```text
state.json       atomic latest snapshot
events.jsonl     append-only DAG lifecycle events
results/         structured node results
logs/            worker stderr and retained diagnostics
artifacts/       patches and other declared deliverables
worktrees/       isolated Git worktrees
run.lock         single-runner lock
runner.json      detached/foreground runner PID and log path
```

Progress is derived from node states and sealed dynamic groups. Before all dynamic groups are sealed, report counts without a stable percentage.

Use the global runner lifecycle commands from the repository root:

```bash
uv run <global-skill>/scripts/workflow.py start <name> --input key=value
uv run <global-skill>/scripts/workflow.py status [run-id] --json
uv run <global-skill>/scripts/workflow.py result [run-id] --json
```

Omitting the run ID selects the newest run for the current repository. `start` returns immediately after launching a detached runner. `result` returns exit code `3` while a run is non-terminal, `0` for completed runs, and `1` for terminal unsuccessful runs.
