from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import threading
from typing import Any

from fastapi import HTTPException
import yaml

from ..config import Settings, get_settings
from ..db import SessionLocal
from ..models import JobStatus
from ..repositories.app_settings import AppSettingsRepository
from ..repositories.jobs import JobRepository
from ..repositories.rules import RuleRepository
from .core_adapter import run_core_pipeline


class JobExecutor:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._pool = ThreadPoolExecutor(max_workers=settings.max_concurrent_jobs)
        self._running_jobs: set[str] = set()
        self._lock = threading.Lock()

    def start_job(self, job_id: str, rule_version_id: str | None = None) -> None:
        with self._lock:
            if job_id in self._running_jobs:
                raise HTTPException(status_code=409, detail="job is already running")
            self._running_jobs.add(job_id)

        future = self._pool.submit(self._run_job, job_id, rule_version_id)

        def _cleanup(_):
            with self._lock:
                self._running_jobs.discard(job_id)

        future.add_done_callback(_cleanup)

    def _run_job(self, job_id: str, requested_rule_version_id: str | None) -> None:
        settings = self.settings
        job_dir = settings.jobs_root / job_id
        input_dir = job_dir / "input"
        runtime_root = job_dir / "core_runtime"
        output_root = job_dir / "output"
        model_snapshot: str | None = None

        with SessionLocal() as db:
            job_repo = JobRepository(db)
            rule_repo = RuleRepository(db)
            app_settings_repo = AppSettingsRepository(db)
            job = job_repo.get_job(job_id)
            if job is None:
                return

            if not settings.openai_api_key:
                job_repo.set_job_status(
                    job,
                    JobStatus.failed,
                    error_message="OpenAI API key is not configured",
                    finished=True,
                )
                job_repo.append_event(
                    job_id=job_id,
                    event_type="job_failed",
                    payload={"message": "OpenAI API key is not configured"},
                )
                return

            files = job_repo.list_job_files(job_id)
            if not files:
                job_repo.set_job_status(job, JobStatus.failed, error_message="no input files", finished=True)
                job_repo.append_event(job_id=job_id, event_type="job_failed", payload={"message": "no input files"})
                return

            if requested_rule_version_id:
                rule_version = rule_repo.get_version(requested_rule_version_id)
            else:
                rule_version = rule_repo.get_current_published()
            model_snapshot = app_settings_repo.get_current_openai_model(
                default_model=settings.openai_model,
                allowed_models=settings.allowed_openai_models,
            )

            if rule_version is None:
                job_repo.set_job_status(job, JobStatus.failed, error_message="no published rules available", finished=True)
                job_repo.append_event(
                    job_id=job_id,
                    event_type="job_failed",
                    payload={"message": "no published rules available"},
                )
                return

            job_repo.set_job_status(
                job,
                JobStatus.running,
                error_message="",
                rule_version_id=rule_version.id,
                started=True,
            )
            job_repo.append_event(
                job_id=job_id,
                event_type="job_started",
                payload={
                    "rule_version_id": rule_version.id,
                    "rule_version_number": rule_version.version_number,
                    "model_snapshot": model_snapshot,
                },
            )

        try:
            output_root.mkdir(parents=True, exist_ok=True)
            runtime_root.mkdir(parents=True, exist_ok=True)

            with SessionLocal() as db:
                rule_repo = RuleRepository(db)
                active_rule = rule_repo.get_version(requested_rule_version_id) if requested_rule_version_id else rule_repo.get_current_published()
                if active_rule is None:
                    raise RuntimeError("rule version disappeared before execution")
                rules_payload = active_rule.payload

            effective_rules_path = output_root / "field_rules.effective.json"
            effective_rules_path.write_text(json.dumps(rules_payload, ensure_ascii=False, indent=2), encoding="utf-8")

            pipeline_config = self._build_effective_pipeline_config(
                runtime_root,
                model_snapshot=model_snapshot or self.settings.openai_model,
            )
            effective_pipeline_path = output_root / "pipeline.effective.yaml"
            effective_pipeline_path.write_text(yaml.safe_dump(pipeline_config, allow_unicode=True, sort_keys=False), encoding="utf-8")

            final_output_path = output_root / "final_output.json"
            command = [
                self.settings.core_python_executable,
                str(self.settings.core_main_path),
                str(input_dir),
                "--config",
                str(effective_pipeline_path),
                "--field-rules-json",
                str(effective_rules_path),
                "--output",
                str(final_output_path),
            ]
            if self.settings.core_skip_kb_bootstrap:
                command.append("--skip-kb-bootstrap")

            def _on_step(step_name: str, payload: dict[str, Any]) -> None:
                step_status = str(payload.get("status") or "ok")
                with SessionLocal() as db:
                    job_repo = JobRepository(db)
                    step_row = job_repo.upsert_step(
                        job_id=job_id,
                        step_name=step_name,
                        step_status=step_status,
                        payload=payload,
                    )
                    job_repo.append_event(
                        job_id=job_id,
                        event_type="step_update",
                        payload={
                            "step_name": step_name,
                            "step_status": step_status,
                            "updated_at": step_row.updated_at.isoformat() if step_row.updated_at else None,
                            "data": payload,
                        },
                    )

            def _on_llm_progress(payload: dict[str, Any]) -> None:
                with SessionLocal() as db:
                    job_repo = JobRepository(db)
                    job_repo.append_event(
                        job_id=job_id,
                        event_type="llm_progress",
                        payload=payload,
                    )

            result = run_core_pipeline(
                command=command,
                runtime_root=runtime_root,
                output_root=output_root,
                working_dir=self.settings.project_root,
                scan_interval_seconds=self.settings.scan_interval_seconds,
                on_step_update=_on_step,
                on_llm_progress=_on_llm_progress,
            )

            with SessionLocal() as db:
                job_repo = JobRepository(db)
                job = job_repo.get_job(job_id)
                if job is None:
                    return

                if result.return_code != 0:
                    message = result.stderr_tail or f"core process failed with return code {result.return_code}"
                    job_repo.set_job_status(
                        job,
                        JobStatus.failed,
                        error_message=message,
                        runtime_dir=str(result.runtime_dir) if result.runtime_dir else None,
                        finished=True,
                    )
                    job_repo.append_event(
                        job_id=job_id,
                        event_type="job_failed",
                        payload={"message": message, "return_code": result.return_code},
                    )
                    return

                if not final_output_path.exists():
                    message = "core completed but final output file was not found"
                    job_repo.set_job_status(
                        job,
                        JobStatus.failed,
                        error_message=message,
                        runtime_dir=str(result.runtime_dir) if result.runtime_dir else None,
                        finished=True,
                    )
                    job_repo.append_event(job_id=job_id, event_type="job_failed", payload={"message": message})
                    return

                final_payload = json.loads(final_output_path.read_text(encoding="utf-8"))
                job_repo.set_job_status(
                    job,
                    JobStatus.succeeded,
                    runtime_dir=str(result.runtime_dir) if result.runtime_dir else None,
                    final_output_path=str(final_output_path),
                    finished=True,
                )
                job_repo.append_event(
                    job_id=job_id,
                    event_type="job_completed",
                    payload={
                        "run_id": final_payload.get("run_id"),
                        "runtime_dir": final_payload.get("runtime_dir"),
                        "result_summary": {
                            "tender_products": len(final_payload.get("tender_products", [])),
                            "match_results": len(final_payload.get("match_results", [])),
                        },
                    },
                )
        except Exception as exc:
            with SessionLocal() as db:
                job_repo = JobRepository(db)
                job = job_repo.get_job(job_id)
                if job is None:
                    return
                job_repo.set_job_status(job, JobStatus.failed, error_message=str(exc), finished=True)
                job_repo.append_event(
                    job_id=job_id,
                    event_type="job_failed",
                    payload={"message": str(exc)},
                )

    def _build_effective_pipeline_config(self, runtime_root: Path, *, model_snapshot: str) -> dict[str, Any]:
        source_path = self.settings.core_pipeline_config_path
        raw = source_path.read_text(encoding="utf-8")
        payload = yaml.safe_load(raw) or {}

        openai_cfg = payload.setdefault("openai", {})
        openai_cfg["model"] = model_snapshot
        openai_cfg["base_url"] = self.settings.openai_base_url

        db_cfg = payload.setdefault("db", {})
        db_cfg["container"] = self.settings.pim_mysql_host
        db_cfg["user"] = self.settings.pim_mysql_user
        db_cfg["password"] = self.settings.pim_mysql_password
        db_cfg["database"] = self.settings.pim_mysql_db
        db_cfg["tables"] = self.settings.mysql_schema_tables
        db_cfg["join_key"] = "product_id"

        runtime_cfg = payload.setdefault("runtime", {})
        runtime_cfg["output_dir"] = str(runtime_root)
        runtime_cfg["keep_intermediate"] = True
        return payload


_executor: JobExecutor | None = None



def get_job_executor() -> JobExecutor:
    global _executor
    if _executor is None:
        _executor = JobExecutor(get_settings())
    return _executor
