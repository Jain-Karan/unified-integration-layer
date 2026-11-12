import json
import re
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path

from pydantic import ValidationError

from uil.canonical import Order

STATUS = {"A": "open", "B": "open", "C": "shipped"}
REQUIRED = [
    "SalesOrder",
    "SoldToParty",
    "SalesOrderDate",
    "TotalNetAmount",
    "OverallSDProcessStatus",
]


def _sap_ts(v):
    if v is None:
        return None
    m = re.fullmatch(r"/Date\((-?\d+)([+-]\d{4})?\)/", v)
    if not m:
        raise ValueError(f"bad SAP date {v!r}")
    return datetime.fromtimestamp(int(m.group(1)) / 1000, tz=UTC)


def _amount(v):
    try:
        return Decimal(str(v)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except InvalidOperation as e:
        raise ValueError(f"bad amount {v!r}") from e


def rows(path: Path):
    good, bad = [], []
    for f in sorted(path.glob("*.json")):
        for rec in json.loads(f.read_text(encoding="utf-8"))["d"]["results"]:
            rec = {k: v for k, v in rec.items() if k != "__metadata"}
            try:
                missing = [k for k in REQUIRED if rec.get(k) in (None, "")]
                if missing:
                    raise ValueError(f"missing {missing}")
                if rec["OverallSDProcessStatus"] not in STATUS:
                    raise ValueError(f"unknown status {rec['OverallSDProcessStatus']!r}")
                cur = rec.get("TransactionCurrency")
                o = Order(
                    order_id="sap:" + rec["SalesOrder"].strip(),
                    customer_id="sap:" + rec["SoldToParty"].strip(),
                    order_date=_sap_ts(rec["SalesOrderDate"]).date(),
                    currency=cur.strip().upper() if cur else None,
                    amount=_amount(rec["TotalNetAmount"]),
                    status=STATUS[rec["OverallSDProcessStatus"]],
                    updated_at=_sap_ts(rec.get("LastChangeDateTime")),
                )
                good.append(o.model_dump())
            except (ValueError, ValidationError) as e:
                bad.append((rec, str(e)))
    return good, bad


def run(path: Path, lake, batch_id: str):
    good, bad = rows(path)
    now = datetime.now(UTC)
    for r in good:
        r.update(_source="sap.sales_orders", _batch_id=batch_id, _ingested_at=now)
    quarantined = lake.quarantine("sap.sales_orders", "order", batch_id, bad)
    inserted, updated, stale = lake.upsert("order", good)
    return {
        "read": len(good) + len(bad),
        "quarantined": quarantined,
        "inserted": inserted,
        "updated": updated,
        "stale": stale,
    }
