"""Regression tests for the Kafka order topic (at-least-once delivery, several events per order)."""

from uil import pipeline
from uil.mapping import Mapping


def test_topic_with_redeliveries_and_multiple_events_per_order_ingests(feeds, truth, lake):
    m = Mapping.load("kafka_orders")
    stats = pipeline.run(m, feeds / m.feed, lake)
    assert stats.inserted > 0
    rows = {r["order_id"]: r for r in lake.rows("order")}
    expected = {k: v for k, v in truth["orders"].items() if k.startswith("retail:")}
    assert set(rows) == set(expected)
    # one row per order, holding the state of its latest event, whatever order events arrived in
    wrong = {
        k: (rows[k]["status"], v["status"])
        for k, v in expected.items()
        if rows[k]["status"] != v["status"]
    }
    assert wrong == {}
    assert stats.collapsed == stats.mapped - stats.inserted
