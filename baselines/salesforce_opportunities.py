import json
from datetime import UTC, date, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path

from pydantic import ValidationError

from uil.canonical import Order

STAGES = {
    "Prospecting": "open",
    "Negotiation": "open",
    "Closed Won": "won",
    "Closed Lost": "closed_lost",
}
REQUIRED = ["Id", "AccountId", "CloseDate", "StageName"]
ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ012345"


def _id18(v):
    v = v.strip()
    if len(v) == 18:
        return v
    if len(v) != 15 or not v.isalnum():
        raise ValueError(f"bad Salesforce id {v!r}")
    suffix = ""
    for chunk in (v[0:5], v[5:10], v[10:15]):
        bits = sum(1 << i for i, ch in enumerate(chunk) if ch.isupper())
        suffix += ALPHABET[bits]
    return v + suffix


def _ts(v):
    if v is None:
        return None
    dt = datetime.fromisoformat(v)
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)


def _amount(v):
    if v is None:
        return None
    try:
        return Decimal(str(v)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except InvalidOperation as e:
        raise ValueError(f"bad amount {v!r}") from e


def _strip_attributes(rec):
    if isinstance(rec, dict):
        return {k: _strip_attributes(v) for k, v in rec.items() if k != "attributes"}
    return rec


def rows(path: Path):
    good, bad = [], []
    for f in sorted(path.glob("*.json")):
        for rec in json.loads(f.read_text(encoding="utf-8"))["records"]:
            rec = _strip_attributes(rec)
            try:
                missing = [k for k in REQUIRED if rec.get(k) in (None, "")]
                if missing:
                    raise ValueError(f"missing {missing}")
                if rec["StageName"] not in STAGES:
                    raise ValueError(f"unknown stage {rec['StageName']!r}")
                cur = rec.get("CurrencyIsoCode")
                o = Order(
                    order_id="sf:" + _id18(rec["Id"]),
                    customer_id="sf:" + _id18(rec["AccountId"]),
                    order_date=date.fromisoformat(rec["CloseDate"]),
                    currency=cur.upper() if cur else None,
                    amount=_amount(rec.get("Amount")),
                    status=STAGES[rec["StageName"]],
                    updated_at=_ts(rec.get("LastModifiedDate")),
                )
                good.append(o.model_dump())
            except (ValueError, ValidationError) as e:
                bad.append((rec, str(e)))
    return good, bad


def run(path: Path, lake, batch_id: str):
    good, bad = rows(path)
    now = datetime.now(UTC)
    for r in good:
        r.update(_source="salesforce.opportunities", _batch_id=batch_id, _ingested_at=now)
    quarantined = lake.quarantine("salesforce.opportunities", "order", batch_id, bad)
    inserted, updated, stale = lake.upsert("order", good)
    return {
        "read": len(good) + len(bad),
        "quarantined": quarantined,
        "inserted": inserted,
        "updated": updated,
        "stale": stale,
    }
