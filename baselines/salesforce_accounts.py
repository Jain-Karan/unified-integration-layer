import json
from datetime import UTC, datetime
from pathlib import Path

from pydantic import ValidationError

from uil.canonical import Customer

COUNTRIES = {
    "Germany": "DE",
    "United States": "US",
    "USA": "US",
    "France": "FR",
    "India": "IN",
    "United Kingdom": "GB",
    "Netherlands": "NL",
    "Japan": "JP",
}
REQUIRED = ["Id", "Name", "BillingCountry", "CreatedDate"]
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
                country = rec["BillingCountry"].strip()
                if country not in COUNTRIES:
                    raise ValueError(f"unmapped country {country!r}")
                city = (rec.get("BillingCity") or "").strip()
                c = Customer(
                    customer_id="sf:" + _id18(rec["Id"]),
                    name=rec["Name"].strip(),
                    country=COUNTRIES[country],
                    city=city or None,
                    segment=rec["Industry"].lower() if rec.get("Industry") else None,
                    created_at=_ts(rec["CreatedDate"]),
                    updated_at=_ts(rec.get("LastModifiedDate")),
                )
                good.append(c.model_dump())
            except (ValueError, ValidationError) as e:
                bad.append((rec, str(e)))
    return good, bad


def run(path: Path, lake, batch_id: str):
    good, bad = rows(path)
    now = datetime.now(UTC)
    for r in good:
        r.update(_source="salesforce.accounts", _batch_id=batch_id, _ingested_at=now)
    quarantined = lake.quarantine("salesforce.accounts", "customer", batch_id, bad)
    inserted, updated, stale = lake.upsert("customer", good)
    return {
        "read": len(good) + len(bad),
        "quarantined": quarantined,
        "inserted": inserted,
        "updated": updated,
        "stale": stale,
    }
