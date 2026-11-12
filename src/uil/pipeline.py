"""One generic pipeline for every source: read -> source contract -> map -> canonical contract ->
sink contract -> upsert into Iceberg. Failures at any contract go to quarantine with the reason;
they never stop the batch and never reach the canonical tables."""

from __future__ import annotations

import uuid
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from pydantic import ValidationError

from .lake import Lake
from .mapping import ContractViolation, Mapping
from .readers import read
from .transforms import TransformError


@dataclass
class RunStats:
    source: str
    entity: str
    batch_id: str
    read: int = 0
    skipped: int = 0  # filtered out by the mapping (other record types on a shared feed)
    mapped: int = 0
    quarantined: int = 0
    inserted: int = 0
    updated: int = 0
    errors: Counter = field(default_factory=Counter)

    def as_dict(self) -> dict:
        d = asdict(self)
        d["errors"] = dict(self.errors)
        return d


def _reason(e: Exception) -> str:
    if isinstance(e, ValidationError):
        err = e.errors()[0]
        return f"canonical contract: {'.'.join(map(str, err['loc']))}: {err['msg']}"
    if isinstance(e, ContractViolation):
        return f"source contract: {e}"
    return f"transform: {e}"


def run(mapping: Mapping, path: Path, lake: Lake, batch_id: str | None = None) -> RunStats:
    batch_id = batch_id or uuid.uuid4().hex[:12]
    stats = RunStats(mapping.source, mapping.entity, batch_id)
    now = datetime.now(UTC)
    good: list[dict] = []
    bad: list[tuple[dict, str]] = []
    for rec in read(mapping.reader, path):
        stats.read += 1
        if not mapping.matches(rec):
            stats.skipped += 1
            continue
        try:
            row = mapping.map_record(rec)
        except (ContractViolation, TransformError, ValidationError) as e:
            reason = _reason(e)
            bad.append((rec, reason))
            stats.errors[reason.split(":")[0]] += 1
            continue
        row.update(_source=mapping.source, _batch_id=batch_id, _ingested_at=now)
        good.append(row)
    stats.mapped = len(good)
    stats.quarantined = lake.quarantine(mapping.source, mapping.entity, batch_id, bad)
    stats.inserted, stats.updated = lake.upsert(mapping.entity, good)
    return stats
