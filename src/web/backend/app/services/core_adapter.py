from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import subprocess
import threading
import time
from typing import Callable


STEP_FILES = {
    "schema_snapshot",
    "step1_kb_bootstrap",
    "step2_extract_requirements",
    "step3_external_field_rules",
    "step4_merge_requirements_hardness",
    "step5_build_sql",
    "step6_execute_sql",
    "step7_rank_candidates",
    "step_index",
    "final_output",
}
LLM_PROGRESS_PREFIX = "LLM_PROGRESS::"


@dataclass
class CoreRunResult:
    return_code: int
    runtime_dir: Path | None
    stderr_tail: str



def _discover_run_dir(runtime_root: Path) -> Path | None:
    if not runtime_root.exists():
        return None
    candidates = [path for path in runtime_root.iterdir() if path.is_dir()]
    if not candidates:
        return None
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]



def _scan_step_payloads(run_dir: Path, seen: dict[str, tuple[int, int]]) -> list[tuple[str, dict]]:
    updates: list[tuple[str, dict]] = []
    if not run_dir.exists():
        return updates

    for path in sorted(run_dir.glob("*.json")):
        step_name = path.stem
        if step_name not in STEP_FILES:
            continue
        stat = path.stat()
        sig = (stat.st_mtime_ns, stat.st_size)
        if seen.get(step_name) == sig:
            continue
        seen[step_name] = sig

        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            payload = {
                "step": step_name,
                "status": "error",
                "errors": [{"code": "JSON_PARSE_FAILED", "message": f"Failed to parse {path.name}"}],
            }
        updates.append((step_name, payload))

    return updates



def _read_tail(path: Path, max_chars: int = 4000) -> str:
    if not path.exists():
        return ""
    text = path.read_text(encoding="utf-8", errors="replace")
    if len(text) <= max_chars:
        return text
    return text[-max_chars:]



def _parse_llm_progress_line(line: str) -> dict | None:
    text = line.strip()
    if not text.startswith(LLM_PROGRESS_PREFIX):
        return None
    raw_payload = text[len(LLM_PROGRESS_PREFIX) :].strip()
    if not raw_payload:
        return None
    try:
        payload = json.loads(raw_payload)
    except Exception:
        return {"kind": "status", "status": "parse_failed", "message": raw_payload}
    if not isinstance(payload, dict):
        return None
    return payload


def run_core_pipeline(
    *,
    command: list[str],
    runtime_root: Path,
    output_root: Path,
    working_dir: Path | None,
    scan_interval_seconds: float,
    on_step_update: Callable[[str, dict], None],
    on_llm_progress: Callable[[dict], None] | None = None,
) -> CoreRunResult:
    output_root.mkdir(parents=True, exist_ok=True)
    stdout_path = output_root / "core_stdout.log"
    stderr_path = output_root / "core_stderr.log"

    with stdout_path.open("w", encoding="utf-8") as stdout_file, stderr_path.open("w", encoding="utf-8") as stderr_file:
        process = subprocess.Popen(
            command,
            cwd=str(working_dir) if working_dir else None,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        seen_signatures: dict[str, tuple[int, int]] = {}
        active_run_dir: Path | None = None

        def _pump_stdout() -> None:
            if process.stdout is None:
                return
            for line in process.stdout:
                stdout_file.write(line)
                stdout_file.flush()
                payload = _parse_llm_progress_line(line)
                if payload is None or on_llm_progress is None:
                    continue
                try:
                    on_llm_progress(payload)
                except Exception:
                    continue

        def _pump_stderr() -> None:
            if process.stderr is None:
                return
            for line in process.stderr:
                stderr_file.write(line)
                stderr_file.flush()

        stdout_thread = threading.Thread(target=_pump_stdout, daemon=True)
        stderr_thread = threading.Thread(target=_pump_stderr, daemon=True)
        stdout_thread.start()
        stderr_thread.start()

        while process.poll() is None:
            active_run_dir = active_run_dir or _discover_run_dir(runtime_root)
            if active_run_dir:
                for step_name, payload in _scan_step_payloads(active_run_dir, seen_signatures):
                    on_step_update(step_name, payload)
            time.sleep(scan_interval_seconds)

        process.wait()
        stdout_thread.join(timeout=2)
        stderr_thread.join(timeout=2)

        active_run_dir = active_run_dir or _discover_run_dir(runtime_root)
        if active_run_dir:
            for step_name, payload in _scan_step_payloads(active_run_dir, seen_signatures):
                on_step_update(step_name, payload)

    stderr_tail = _read_tail(stderr_path)
    return CoreRunResult(
        return_code=process.returncode,
        runtime_dir=active_run_dir,
        stderr_tail=stderr_tail,
    )
