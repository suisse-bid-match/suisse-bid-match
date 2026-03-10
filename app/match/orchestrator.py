from __future__ import annotations

from app.core.models import MatchRun
from app.core.settings import settings
from app.core.storage import save_match_run

from .workflow import MatchWorkflow


def run_match(
    *,
    package_id: str,
    domain: str | None = None,
    top_k: int | None = None,
    strict_hard_constraints: bool = True,
    progress_callback=None,
) -> MatchRun:
    selected_domain = (domain or settings.match_default_domain).strip().lower()
    if not selected_domain:
        selected_domain = settings.match_default_domain

    selected_top_k = top_k if top_k is not None else settings.match_default_top_k
    if selected_top_k <= 0:
        selected_top_k = settings.match_default_top_k

    workflow = MatchWorkflow()
    run = workflow.run(
        package_id=package_id,
        domain=selected_domain,
        top_k=selected_top_k,
        strict_hard_constraints=strict_hard_constraints,
        progress_callback=progress_callback,
    )
    return save_match_run(run)
