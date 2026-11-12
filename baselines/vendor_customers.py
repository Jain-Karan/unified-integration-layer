import csv
import io
import re
from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from pydantic import ValidationError

from uil.canonical import Customer

SEGMENTS = {"A": "key_account", "B": "core", "C": "tail"}
REQUIRED = ["Kundennr", "Name", "Land", "Angelegt"]
BERLIN = ZoneInfo("Europe/Berlin")


def _blank(v):
    return None if v is None or v.strip() == "" else v.strip()


def rows(path: Path):
    good, bad = [], []
    for f in sorted(p for p in path.iterdir() if p.is_file()):
        lines = f.read_bytes().decode("latin-1").splitlines()[1:]  # vendor preamble line
        for rec in csv.DictReader(io.StringIO("\n".join(lines)), delimiter=";"):
            try:
                missing = [k for k in REQUIRED if rec.get(k) in (None, "")]
                if missing:
                    raise ValueError(f"missing {missing}")
                if not re.fullmatch(r"R[0-9]{5}", rec["Kundennr"]):
                    raise ValueError("bad Kundennr")
                cls = rec.get("Umsatzklasse")
                if cls is not None and cls not in SEGMENTS:
                    raise ValueError(f"unknown Umsatzklasse {cls!r}")
                created = datetime.strptime(rec["Angelegt"].strip(), "%d.%m.%Y")
                created = created.replace(tzinfo=BERLIN).astimezone(UTC)
                email = _blank(rec.get("E-Mail"))
                c = Customer(
                    customer_id="retail:" + rec["Kundennr"].strip(),
                    name=rec["Name"].strip(),
                    country=rec["Land"].strip().upper(),
                    city=_blank(rec.get("Ort")),
                    email=email.lower() if email else None,
                    segment=SEGMENTS.get(cls) if cls else None,
                    created_at=created,
                    updated_at=created,
                )
                good.append(c.model_dump())
            except (ValueError, ValidationError) as e:
                bad.append((rec, str(e)))
    return good, bad


def run(path: Path, lake, batch_id: str):
    good, bad = rows(path)
    now = datetime.now(UTC)
    for r in good:
        r.update(_source="vendor.distributor_customers", _batch_id=batch_id, _ingested_at=now)
    quarantined = lake.quarantine("vendor.distributor_customers", "customer", batch_id, bad)
    inserted, updated, stale = lake.upsert("customer", good)
    return {
        "read": len(good) + len(bad),
        "quarantined": quarantined,
        "inserted": inserted,
        "updated": updated,
        "stale": stale,
    }
