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


def test_late_event_in_a_later_batch_does_not_roll_back_newer_state(tmp_path, lake):
    """A redelivered `order_created` lands in the next micro-batch after the `shipped` event."""
    import json

    def event(status, ts, offset):
        value = {
            "event_type": "x",
            "order_id": "WEB-1",
            "customer_ref": "R70001",
            "order_ts": 1_780_000_000_000,
            "currency": "EUR",
            "amount_cents": 1000,
            "status": status,
            "event_ts": ts,
        }
        return json.dumps({"partition": 0, "offset": offset, "key": "WEB-1", "value": value})

    m = Mapping.load("kafka_orders")
    first, second = tmp_path / "b1.jsonl", tmp_path / "b2.jsonl"
    first.write_text(
        event("open", 1_780_000_000_000, 1) + "\n" + event("shipped", 1_780_000_900_000, 2) + "\n"
    )
    second.write_text(event("open", 1_780_000_000_000, 3) + "\n")  # redelivery, older event

    pipeline.run(m, first, lake)
    stats = pipeline.run(m, second, lake)

    [row] = lake.rows("order")
    assert row["status"] == "shipped"
    assert stats.stale == 1 and stats.updated == 0
