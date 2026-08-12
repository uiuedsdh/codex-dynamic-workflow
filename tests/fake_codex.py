#!/usr/bin/env python3
"""Small codex-exec double used by the workflow runtime tests."""

from __future__ import annotations

import json
import sys
import uuid
from pathlib import Path


def option(name: str) -> str | None:
    try:
        return sys.argv[sys.argv.index(name) + 1]
    except (ValueError, IndexError):
        return None


def main() -> int:
    output = option("-o") or option("--output-last-message")
    prompt = sys.argv[-1]
    print(json.dumps({"type": "thread.started", "thread_id": str(uuid.uuid4())}), flush=True)
    print(json.dumps({"type": "turn.started"}), flush=True)
    if "FAIL_NODE" in prompt:
        print(json.dumps({"type": "turn.failed", "error": "requested failure"}), flush=True)
        return 1
    if "DISCOVER_TARGETS" in prompt:
        result: object = {"targets": [{"id": "alpha"}, {"id": "beta"}]}
    elif "REPAIR_TARGET" in prompt:
        result = {"ok": True}
    elif "RESOLVE_CONFLICT" in prompt:
        result = "resolved"
    else:
        result = {"ok": True}
    if output:
        path = Path(output)
        path.write_text(json.dumps(result), encoding="utf-8")
    print(json.dumps({
        "type": "turn.completed",
        "usage": {"input_tokens": 10, "output_tokens": 2, "cached_input_tokens": 0},
    }), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
