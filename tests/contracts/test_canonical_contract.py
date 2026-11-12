"""Contract tests on both sides of the canonical model.

Source side: each mapping turns the synthetic feed into rows that satisfy the canonical model,
and every record it refuses corresponds to a defect the generator injected on purpose.
Sink side: the Iceberg tables keep exactly the canonical schema.
"""

import json
from collections import Counter

import pyarrow as pa
import pytest
from pyiceberg.types import StringType

from uil import pipeline
from uil.canonical import ARROW_SCHEMAS
from uil.lake import SinkContractError
from uil.mapping import Mapping

BATCH_SOURCES = {  # mapping -> defect source label used by the generator
    "sap_customers": ("sap", "customer"),
    "sap_orders": ("sap", "order"),
    "salesforce_accounts": ("salesforce", "customer"),
    "salesforce_opportunities": ("salesforce", "order"),
}


@pytest.mark.parametrize("name", list(BATCH_SOURCES))
def test_quarantine_is_exactly_the_injected_defects(name, feeds, truth, lake):
    m = Mapping.load(name)
    stats = pipeline.run(m, feeds / m.feed, lake)
    src, _ = BATCH_SOURCES[name]
    key = m.fields[0].source  # first field is the entity key in every mapping
    refused = {json.loads(r["record"])[key] for r in lake.rows("quarantine")}
    injected = {d["id"] for d in truth["defects"] if d["source"] == src}
    assert refused <= injected, "quarantined a record that had no injected defect"
    assert len(refused) == stats.quarantined > 0
    assert stats.mapped + stats.quarantined == stats.read


def test_mapped_orders_match_ground_truth(feeds, truth, lake):
    for name in ("sap_orders", "salesforce_opportunities"):
        m = Mapping.load(name)
        pipeline.run(m, feeds / m.feed, lake)
    rows = {r["order_id"]: r for r in lake.rows("order")}
    expected = {k: v for k, v in truth["orders"].items() if not k.startswith("retail:")}
    assert set(rows) == set(expected)
    wrong = [
        k
        for k, v in expected.items()
        if rows[k]["status"] != v["status"] or f"{rows[k]['amount']:.2f}" != v["amount"]
    ]
    assert wrong == []


def test_reingesting_the_same_batch_is_idempotent(feeds, lake):
    m = Mapping.load("sap_customers")
    first = pipeline.run(m, feeds / m.feed, lake)
    again = pipeline.run(m, feeds / m.feed, lake)
    assert again.inserted == 0
    assert len(lake.rows("customer")) == first.inserted


def test_sink_contract_detects_out_of_band_schema_change(lake):
    lake.table("customer")
    with lake.table("customer").update_schema() as u:
        u.add_column("vip_flag", StringType())
    with pytest.raises(SinkContractError, match="vip_flag"):
        lake.upsert("customer", [{}])


def test_arrow_schemas_carry_lineage_columns():
    for schema in ARROW_SCHEMAS.values():
        assert {"_source", "_batch_id", "_ingested_at"} <= set(schema.names)
        assert schema.field("_ingested_at").type == pa.timestamp("us", tz="UTC")


def test_error_categories_are_reported(feeds, lake):
    m = Mapping.load("sap_orders")
    stats = pipeline.run(m, feeds / m.feed, lake)
    assert sum(stats.errors.values()) == stats.quarantined
    assert set(stats.errors) <= {"source contract", "transform", "canonical contract"}
    assert Counter(stats.errors)
