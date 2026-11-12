"""Declarative source mappings.

One YAML file per source feed describes how to read it, what the raw records must look like (the
source contract), and how each canonical field is derived. Onboarding a new feed is a new YAML
file plus a sample fixture for its contract test; no pipeline code.

    source: sap_manufacturing.customers
    entity: customer
    reader: {type: sap_odata}
    contract:
      required: [KUNNR, NAME1, LAND1, ERDAT]
      patterns: {KUNNR: "^[0-9]{10}$"}
    fields:
      customer_id: {from: KUNNR, transform: [strip, {prefix: "sap:"}]}
      created_at:  {from: ERDAT, transform: [sap_datetime]}
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from . import transforms as T
from .canonical import ENTITIES

MAPPINGS_DIR = Path(__file__).with_name("mappings")


class ContractViolation(ValueError):
    pass


def get_path(record: dict, path: str) -> Any:
    """`Account.Owner.Name` style lookups into nested records; missing -> None."""
    cur: Any = record
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


@dataclass
class FieldSpec:
    name: str
    source: str | list[str] | None = None
    transform: list = field(default_factory=list)
    const: Any = None
    default: Any = None

    def value(self, record: dict) -> Any:
        if self.const is not None:
            return self.const
        if isinstance(self.source, list):
            raw: Any = [get_path(record, p) for p in self.source]
        elif self.source is not None:
            raw = get_path(record, self.source)
        else:
            raw = None
        out = T.apply(self.transform, raw)
        return self.default if out is None else out


@dataclass
class SourceContract:
    required: list[str] = field(default_factory=list)
    patterns: dict[str, str] = field(default_factory=dict)

    def check(self, record: dict) -> None:
        missing = [f for f in self.required if get_path(record, f) in (None, "")]
        if missing:
            raise ContractViolation(f"missing required source fields: {', '.join(missing)}")
        for f, pat in self.patterns.items():
            v = get_path(record, f)
            if v is not None and not re.fullmatch(pat, str(v)):
                raise ContractViolation(f"{f}={v!r} does not match {pat}")


@dataclass
class Mapping:
    source: str
    entity: str
    reader: dict
    fields: list[FieldSpec]
    contract: SourceContract
    domain: str = ""
    filter: dict | None = None  # {field: value} records must match to be mapped
    feed: str = ""  # feed location relative to the landing root, e.g. retail/kafka_orders

    @classmethod
    def load(cls, path: str | Path) -> Mapping:
        p = Path(path)
        if not p.exists():
            p = MAPPINGS_DIR / f"{path}.yaml"
        d = yaml.safe_load(p.read_text())
        if d["entity"] not in ENTITIES:
            raise ValueError(f"{p.name}: unknown entity {d['entity']!r}")
        model_fields = set(ENTITIES[d["entity"]].model_fields)
        unknown = set(d["fields"]) - model_fields
        if unknown:
            raise ValueError(f"{p.name}: fields not in canonical {d['entity']}: {sorted(unknown)}")
        specs = [
            FieldSpec(name, s.get("from"), s.get("transform", []), s.get("const"), s.get("default"))
            for name, s in d["fields"].items()
        ]
        c = d.get("contract", {})
        return cls(
            d["source"],
            d["entity"],
            d.get("reader", {}),
            specs,
            SourceContract(c.get("required", []), c.get("patterns", {})),
            d.get("domain", ""),
            d.get("filter"),
            d.get("feed", ""),
        )

    def matches(self, record: dict) -> bool:
        return not self.filter or all(get_path(record, k) == v for k, v in self.filter.items())

    def map_record(self, record: dict) -> dict:
        """Source contract -> field mapping -> canonical contract. Raises on any violation."""
        self.contract.check(record)
        out = {}
        for f in self.fields:
            try:
                out[f.name] = f.value(record)
            except T.TransformError as e:
                raise T.TransformError(f"{f.name}: {e}") from e
        model = ENTITIES[self.entity].model_validate(out)
        return model.model_dump()


def all_mappings() -> list[Mapping]:
    return [Mapping.load(p) for p in sorted(MAPPINGS_DIR.glob("*.yaml"))]
