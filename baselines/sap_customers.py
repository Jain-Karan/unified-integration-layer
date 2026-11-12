import json
import re
from datetime import UTC, datetime
from pathlib import Path

from pydantic import ValidationError

from uil.canonical import Customer

SEGMENTS = {"KUNA": "domestic", "KUNE": "export", "CPD": "one_time"}
REQUIRED = ["Customer", "CustomerName", "Country", "CreationDate"]


def _sap_ts(v):
    if v is None:
        return None
    m = re.fullmatch(r"/Date\((-?\d+)([+-]\d{4})?\)/", v)
    if not m:
        raise ValueError(f"bad SAP date {v!r}")
    return datetime.fromtimestamp(int(m.group(1)) / 1000, tz=UTC)


def _blank(v):
    return None if v is None or v.strip() == "" else v.strip()


def rows(path: Path):
    good, bad = [], []
    for f in sorted(path.glob("*.json")):
        for rec in json.loads(f.read_text(encoding="utf-8"))["d"]["results"]:
            rec = {k: v for k, v in rec.items() if k != "__metadata"}
            try:
                missing = [k for k in REQUIRED if rec.get(k) in (None, "")]
                if missing:
                    raise ValueError(f"missing {missing}")
                if not re.fullmatch(r"[0-9]{10}", rec["Customer"]):
                    raise ValueError("bad Customer")
                group = rec.get("CustomerAccountGroup")
                if group is not None and group not in SEGMENTS:
                    raise ValueError(f"unknown account group {group!r}")
                email = _blank(rec.get("EmailAddress"))
                c = Customer(
                    customer_id="sap:" + rec["Customer"].strip(),
                    name=rec["CustomerName"].strip(),
                    country=rec["Country"].strip().upper(),
                    city=_blank(rec.get("CityName")),
                    email=email.lower() if email else None,
                    segment=SEGMENTS.get(group) if group else None,
                    created_at=_sap_ts(rec["CreationDate"]),
                    updated_at=_sap_ts(rec.get("LastChangeDateTime")),
                )
                good.append(c.model_dump())
            except (ValueError, ValidationError) as e:
                bad.append((rec, str(e)))
    return good, bad


def run(path: Path, lake, batch_id: str):
    good, bad = rows(path)
    now = datetime.now(UTC)
    for r in good:
        r.update(_source="sap.customers", _batch_id=batch_id, _ingested_at=now)
    quarantined = lake.quarantine("sap.customers", "customer", batch_id, bad)
    inserted, updated, stale = lake.upsert("customer", good)
    return {
        "read": len(good) + len(bad),
        "quarantined": quarantined,
        "inserted": inserted,
        "updated": updated,
        "stale": stale,
    }
