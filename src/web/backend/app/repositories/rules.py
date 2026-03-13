from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from ..models import RuleSource, RuleStatus, RuleVersion



def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class RuleRepository:
    def __init__(self, db: Session):
        self.db = db

    def list_versions(
        self,
        *,
        status: RuleStatus | None = None,
        source: RuleSource | None = None,
        query: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[RuleVersion]:
        stmt = select(RuleVersion)
        if status is not None:
            stmt = stmt.where(RuleVersion.status == status)
        if source is not None:
            stmt = stmt.where(RuleVersion.source == source)
        if query:
            normalized = query.strip()
            if normalized:
                pattern = f"%{normalized}%"
                stmt = stmt.where(or_(RuleVersion.id.ilike(pattern), RuleVersion.note.ilike(pattern)))
        stmt = stmt.order_by(RuleVersion.version_number.desc())
        if offset is not None:
            stmt = stmt.offset(max(offset, 0))
        if limit is not None:
            stmt = stmt.limit(max(limit, 1))
        return list(self.db.scalars(stmt).all())

    def get_version(self, version_id: str) -> RuleVersion | None:
        return self.db.get(RuleVersion, version_id)

    def get_current_published(self) -> RuleVersion | None:
        stmt = (
            select(RuleVersion)
            .where(RuleVersion.status == RuleStatus.published)
            .order_by(RuleVersion.version_number.desc())
            .limit(1)
        )
        return self.db.scalars(stmt).first()

    def next_version_number(self) -> int:
        stmt = select(func.max(RuleVersion.version_number))
        max_value = self.db.scalar(stmt)
        return int(max_value or 0) + 1

    def create_version(
        self,
        *,
        payload: dict,
        status: RuleStatus,
        source: RuleSource,
        validation_report: dict,
        copilot_log: dict | None = None,
        note: str | None = None,
    ) -> RuleVersion:
        row = RuleVersion(
            version_number=self.next_version_number(),
            payload=payload,
            status=status,
            source=source,
            validation_report=validation_report,
            copilot_log=copilot_log,
            note=note,
            published_at=utc_now() if status == RuleStatus.published else None,
        )
        self.db.add(row)
        self.db.commit()
        self.db.refresh(row)
        return row

    def publish(self, version_id: str) -> RuleVersion:
        target = self.get_version(version_id)
        if target is None:
            raise ValueError("rule version not found")
        if target.status == RuleStatus.published:
            self.db.refresh(target)
            return target

        # Archive existing published versions first and flush, then publish target.
        # This ordering avoids violating the "single published" unique index.
        stmt = select(RuleVersion).where(
            RuleVersion.status == RuleStatus.published,
            RuleVersion.id != target.id,
        )
        current_published = list(self.db.scalars(stmt).all())
        for row in current_published:
            row.status = RuleStatus.archived
            row.published_at = None
            self.db.add(row)
        if current_published:
            self.db.flush()

        target.status = RuleStatus.published
        target.published_at = utc_now()
        self.db.add(target)
        self.db.commit()
        self.db.refresh(target)
        return target
