import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from pydantic import ValidationError

from uil.canonical import Order

STATUS = {"open", "shipped", "cancelled"}
REQUIRED = [
    "order_id",
    "customer_ref",
    "order_ts",
    "currency",
    "amount_cents",
    "status",
    "event_ts",
]


def _ms(v):
    try:
        return datetime.fromtimestamp(int(v) / 1000, tz=UTC)
    except (TypeError, ValueError, OverflowError) as e:
        raise ValueError(f"bad epoch millis {v!r}") from e


def rows(path: Path):
    good, bad = [], []
    for f in sorted(path.glob("*.jsonl")):
        for line in f.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            msg = json.loads(line)
            rec = {**msg["value"], "_partition": msg["partition"], "_offset": msg["offset"]}
            try:
                missing = [k for k in REQUIRED if rec.get(k) in (None, "")]
                if missing:
                    raise ValueError(f"missing {missing}")
                if rec["status"] not in STATUS:
                    raise ValueError(f"unknown status {rec['status']!r}")
                if isinstance(rec["amount_cents"], bool) or not isinstance(
                    rec["amount_cents"], int
                ):
                    raise ValueError(f"bad cents {rec['amount_cents']!r}")
                o = Order(
                    order_id="retail:" + rec["order_id"],
                    customer_id="retail:" + rec["customer_ref"],
                    order_date=_ms(rec["order_ts"]).date(),
                    currency=rec["currency"].upper(),
                    amount=Decimal(rec["amount_cents"]) / 100,
                    status=rec["status"],
                    updated_at=_ms(rec["event_ts"]),
                )
                good.append(o.model_dump())
            except (ValueError, ValidationError) as e:
                bad.append((rec, str(e)))
    return good, bad


def run(path: Path, lake, batch_id: str):
    good, bad = rows(path)
    now = datetime.now(UTC)
    # at-least-once topic: several versions per order; keep the newest (event time, offset)
    latest = {}
    for i, r in enumerate(good):
        version = (r["updated_at"], i)
        if r["order_id"] not in latest or version > latest[r["order_id"]][0]:
            latest[r["order_id"]] = (version, r)
    good = [r for _, r in latest.values()]
    for r in good:
        r.update(_source="kafka.orders", _batch_id=batch_id, _ingested_at=now)
    quarantined = lake.quarantine("kafka.orders", "order", batch_id, bad)
    inserted, updated, stale = lake.upsert("order", good)
    return {
        "read": len(good) + len(bad),
        "quarantined": quarantined,
        "inserted": inserted,
        "updated": updated,
        "stale": stale,
    }
