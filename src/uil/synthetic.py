"""Seeded synthetic feeds for three modeled industry domains, in each system's native format.

- manufacturing: SAP S/4HANA OData v2 (A_Customer, A_SalesOrder): `/Date(ms)/` timestamps, amounts
  as strings, single-letter status codes, paged with `__next`
- b2b_sales: Salesforce REST (Account, Opportunity): `attributes` blocks, country *names*, stage
  names, opportunities that reference accounts by 15-character IDs
- retail: a Kafka `orders` topic (created / updated / cancelled events, amounts in cents, epoch
  millis) and a distributor's customer file (Latin-1, semicolons, a preamble line, German dates)

About 3% of records carry a known defect (missing field, malformed date, unmapped code, bad
email, negative amount...). `truth.json` lists them so quarantine precision and recall can be
measured, and lists the expected final state of every order.
"""

from __future__ import annotations

import json
import random
import zlib
from datetime import UTC, datetime, timedelta
from pathlib import Path

COUNTRIES = [
    ("DE", "Germany"),
    ("US", "United States"),
    ("FR", "France"),
    ("IN", "India"),
    ("GB", "United Kingdom"),
    ("NL", "Netherlands"),
    ("JP", "Japan"),
]
CITIES = {
    "DE": ["Berlin", "München", "Köln"],
    "US": ["Austin", "Chicago", "San Jose"],
    "FR": ["Lyon", "Paris"],
    "IN": ["Pune", "Hyderabad"],
    "GB": ["Leeds", "London"],
    "NL": ["Utrecht"],
    "JP": ["Osaka"],
}
NAMES = ["Müller", "Okafor", "Shah", "Lefèvre", "Nakamura", "Brown", "de Vries", "Kowalski"]
COMPANIES = ["Industrie", "Systems", "Holdings", "GmbH", "Logistics", "Foods", "Tech", "Retail"]
EPOCH = datetime(2026, 1, 1, tzinfo=UTC)


def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _sf_ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000+0000")


def _sf_id(rng: random.Random, prefix: str) -> str:
    chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    return prefix + "".join(rng.choice(chars) for _ in range(12))  # 15-char form


def _paged(records: list[dict], size: int, wrap) -> list[dict]:  # noqa: ANN001
    return [
        wrap(records[i : i + size], i + size < len(records)) for i in range(0, len(records), size)
    ]


class Builder:
    def __init__(self, root: Path, seed: int, defect_rate: float) -> None:
        self.root, self.rng, self.rate = root, random.Random(seed), defect_rate
        self.defects: list[dict] = []
        self.orders: dict[str, dict] = {}  # canonical order_id -> expected final state

    def when(self, days: int = 300) -> datetime:
        return EPOCH + timedelta(seconds=self.rng.randint(0, days * 86400))

    def defect(self, source: str, native_id: str, kinds: list[str]) -> str | None:
        if self.rng.random() >= self.rate:
            return None
        kind = self.rng.choice(kinds)
        self.defects.append({"source": source, "id": native_id, "defect": kind})
        return kind

    def write_json(self, rel: str, pages: list[dict]) -> None:
        d = self.root / rel
        d.mkdir(parents=True, exist_ok=True)
        for i, p in enumerate(pages, 1):
            (d / f"page_{i:03d}.json").write_text(
                json.dumps(p, ensure_ascii=False, indent=1), encoding="utf-8"
            )

    # --- manufacturing: SAP --------------------------------------------------------------------
    def sap(self, n_customers: int, n_orders: int) -> None:
        src = "sap"
        custs = []
        for i in range(n_customers):
            cid = f"{1000000 + i:010d}"
            iso, _ = self.rng.choice(COUNTRIES)
            created = self.when()
            c = {
                "__metadata": {"type": "API_BUSINESS_PARTNER.A_CustomerType"},
                "Customer": cid,
                "CustomerName": f"{self.rng.choice(NAMES)} {self.rng.choice(COMPANIES)}",
                "Country": iso,
                "CityName": self.rng.choice(CITIES[iso]),
                "EmailAddress": f"ap{i}@customer{i}.example",
                "CustomerAccountGroup": self.rng.choice(["KUNA", "KUNE", "CPD"]),
                "CreationDate": f"/Date({_ms(created)})/",
                "LastChangeDateTime": f"/Date({_ms(created + timedelta(days=3))}+0000)/",
            }
            match self.defect(src, cid, ["missing_name", "bad_date", "bad_email"]):
                case "missing_name":
                    c["CustomerName"] = ""
                case "bad_date":
                    c["CreationDate"] = "2026-13-45"
                case "bad_email":
                    c["EmailAddress"] = "not-an-email"
            custs.append(c)
        self.write_json(
            "manufacturing/sap_customers",
            _paged(
                custs,
                200,
                lambda rs, more: {
                    "d": {
                        "results": rs,
                        **({"__next": "A_Customer?$skiptoken=next"} if more else {}),
                    }
                },
            ),
        )
        orders = []
        for i in range(n_orders):
            oid = f"{5000000 + i}"
            created = self.when()
            status = self.rng.choice(["A", "B", "C", "C"])
            amount = f"{self.rng.uniform(50, 25000):.2f}"
            o = {
                "SalesOrder": oid,
                "SoldToParty": custs[self.rng.randrange(len(custs))]["Customer"],
                "SalesOrderDate": f"/Date({_ms(created)})/",
                "TransactionCurrency": "EUR",
                "TotalNetAmount": amount,
                "OverallSDProcessStatus": status,
                "LastChangeDateTime": f"/Date({_ms(created + timedelta(hours=5))}+0000)/",
            }
            kind = self.defect(src, oid, ["unknown_status", "negative_amount", "missing_customer"])
            match kind:
                case "unknown_status":
                    o["OverallSDProcessStatus"] = "Z"
                case "negative_amount":
                    o["TotalNetAmount"] = f"-{amount}"
                case "missing_customer":
                    o["SoldToParty"] = ""
            orders.append(o)
            if not kind:
                self.orders[f"sap:{oid}"] = {
                    "status": {"A": "open", "B": "open", "C": "shipped"}[status],
                    "amount": amount,
                }
        self.write_json(
            "manufacturing/sap_orders", _paged(orders, 250, lambda rs, more: {"d": {"results": rs}})
        )

    # --- b2b sales: Salesforce -----------------------------------------------------------------
    def salesforce(self, n_accounts: int, n_opps: int) -> None:
        src = "salesforce"
        accts = []
        from uil.transforms import sf_id18

        for _ in range(n_accounts):
            aid15 = _sf_id(self.rng, "001")
            iso, cname = self.rng.choice(COUNTRIES)
            created = self.when()
            a = {
                "attributes": {
                    "type": "Account",
                    "url": f"/services/data/v60.0/sobjects/Account/{aid15}",
                },
                "Id": sf_id18(aid15),
                "Name": f"{self.rng.choice(NAMES)} {self.rng.choice(COMPANIES)}",
                "BillingCountry": cname,
                "BillingCity": self.rng.choice(CITIES[iso]),
                "Industry": self.rng.choice(["Manufacturing", "Insurance", "Retail", None]),
                "CreatedDate": _sf_ts(created),
                "LastModifiedDate": _sf_ts(created + timedelta(days=9)),
            }
            match self.defect(src, a["Id"], ["unmapped_country", "missing_name"]):
                case "unmapped_country":
                    a["BillingCountry"] = "Atlantis"
                case "missing_name":
                    a["Name"] = None
            accts.append((aid15, a))
        self.write_json(
            "b2b_sales/salesforce_accounts",
            _paged(
                [a for _, a in accts],
                200,
                lambda rs, more: {
                    "totalSize": len(rs),
                    "done": not more,
                    "records": rs,
                    **({"nextRecordsUrl": "/services/data/v60.0/query/01g-2000"} if more else {}),
                },
            ),
        )
        stages = {
            "Prospecting": "open",
            "Negotiation": "open",
            "Closed Won": "won",
            "Closed Lost": "closed_lost",
        }
        opps = []
        for _ in range(n_opps):
            oid15 = _sf_id(self.rng, "006")
            oid = sf_id18(oid15)
            aid15, _a = accts[self.rng.randrange(len(accts))]
            stage = self.rng.choice(list(stages))
            close = self.when()
            amount = round(self.rng.uniform(1000, 250000), 2)
            o = {
                "attributes": {"type": "Opportunity"},
                "Id": oid,
                # many orgs' integrations and report exports carry the 15-character form
                "AccountId": aid15 if self.rng.random() < 0.4 else sf_id18(aid15),
                "CloseDate": close.date().isoformat(),
                "CurrencyIsoCode": self.rng.choice(["USD", "EUR", "GBP"]),
                "Amount": amount,
                "StageName": stage,
                "LastModifiedDate": _sf_ts(close),
            }
            kind = self.defect(src, oid, ["unknown_stage", "missing_close_date"])
            match kind:
                case "unknown_stage":
                    o["StageName"] = "Id. Decision Makers"
                case "missing_close_date":
                    o["CloseDate"] = None
            opps.append(o)
            if not kind:
                self.orders[f"sf:{oid}"] = {"status": stages[stage], "amount": f"{amount:.2f}"}
        self.write_json(
            "b2b_sales/salesforce_opportunities",
            _paged(
                opps, 250, lambda rs, more: {"totalSize": len(rs), "done": not more, "records": rs}
            ),
        )

    # --- retail: distributor file + Kafka order events -----------------------------------------
    def retail(self, n_customers: int, n_orders: int) -> None:
        src_file, src_kafka = "vendor_file", "kafka"
        lines = [
            "Export Kundenstamm Haendler Nord;erstellt 2026-11-01",
            "Kundennr;Name;Land;Ort;E-Mail;Angelegt;Umsatzklasse",
        ]
        refs = []
        for i in range(n_customers):
            ref = f"R{70000 + i}"
            iso, _ = self.rng.choice(COUNTRIES[:3])
            created = self.when()
            row = [
                ref,
                f"{self.rng.choice(NAMES)} {self.rng.choice(COMPANIES)}",
                iso,
                self.rng.choice(CITIES[iso]),
                f"einkauf{i}@haendler{i}.example",
                created.strftime("%d.%m.%Y"),
                self.rng.choice(["A", "B", "C"]),
            ]
            match self.defect(src_file, ref, ["bad_date", "unmapped_country"]):
                case "bad_date":
                    row[5] = "31.02.2026"
                case "unmapped_country":
                    row[2] = "XX"
            lines.append(";".join(row))
            refs.append(ref)
        d = self.root / "retail/vendor_customers"
        d.mkdir(parents=True, exist_ok=True)
        (d / "kunden_2026-11-01.csv").write_bytes("\r\n".join(lines).encode("latin-1"))

        events, offset = [], 0
        for i in range(n_orders):
            oid = f"WEB-{900000 + i}"
            ts = self.when()
            cents = self.rng.randint(500, 300000)
            cust = self.rng.choice(refs)
            seq = [("order_created", "open", ts)]
            r = self.rng.random()
            if r < 0.5:
                seq.append(
                    ("order_updated", "shipped", ts + timedelta(hours=self.rng.randint(2, 72)))
                )
            elif r < 0.6:
                seq.append(
                    ("order_cancelled", "cancelled", ts + timedelta(hours=self.rng.randint(1, 24)))
                )
            kind = self.defect(src_kafka, oid, ["missing_currency", "bad_amount"])
            for etype, status, at in seq:
                ev = {
                    "event_type": etype,
                    "order_id": oid,
                    "customer_ref": cust,
                    "order_ts": _ms(ts),
                    "currency": "EUR",
                    "amount_cents": cents,
                    "status": status,
                    "event_ts": _ms(at),
                }
                if kind == "missing_currency":
                    ev["currency"] = None
                elif kind == "bad_amount":
                    ev["amount_cents"] = "12,50"
                events.append(ev)
            if not kind:
                self.orders[f"retail:{oid}"] = {
                    "status": seq[-1][1],
                    "amount": f"{cents / 100:.2f}",
                }
        # at-least-once delivery: ~2% of events are redelivered later in the topic
        for ev in self.rng.sample(events, k=max(1, len(events) // 50)):
            events.append(dict(ev))
        # events for one order can land on different partitions and be read out of order
        self.rng.shuffle(events)
        d = self.root / "retail/kafka_orders"
        d.mkdir(parents=True, exist_ok=True)
        with (d / "orders.jsonl").open("w", encoding="utf-8") as f:
            for ev in events:
                f.write(
                    json.dumps(
                        {
                            "partition": zlib.crc32(ev["order_id"].encode()) % 3,
                            "offset": offset,
                            "key": ev["order_id"],
                            "value": ev,
                        }
                    )
                    + "\n"
                )
                offset += 1


def build(root: str | Path, seed: int = 7, scale: int = 1, defect_rate: float = 0.03) -> dict:
    root = Path(root)
    b = Builder(root, seed, defect_rate)
    b.sap(400 * scale, 1500 * scale)
    b.salesforce(300 * scale, 1200 * scale)
    b.retail(250 * scale, 1500 * scale)
    truth = {"defects": b.defects, "orders": b.orders}
    (root / "truth.json").write_text(json.dumps(truth, indent=1))
    return truth
