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

from .canonical import KEYS
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
    collapsed: int = 0  # superseded versions of a key within the batch (redeliveries, updates)
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
    key = KEYS[mapping.entity]
    latest: dict[str, tuple[tuple, dict]] = {}
    bad: list[tuple[dict, str]] = []
    for pos, rec in enumerate(read(mapping.reader, path)):
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
        stats.mapped += 1
        # A batch can hold several versions of one key: Kafka redelivers (at-least-once) and an
        # order emits created/updated/cancelled events, which arrive out of order across
        # partitions. Keep the newest by business time; ties go to the later log position.
        version = (row["updated_at"], rec.get("_partition", 0), rec.get("_offset", pos))
        prev = latest.get(row[key])
        if prev is None or version > prev[0]:
            latest[row[key]] = (version, row)
    good = [row for _, row in latest.values()]
    stats.collapsed = stats.mapped - len(good)
    stats.quarantined = lake.quarantine(mapping.source, mapping.entity, batch_id, bad)
    stats.inserted, stats.updated = lake.upsert(mapping.entity, good)
    return stats
