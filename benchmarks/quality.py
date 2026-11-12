"""End-to-end run on the synthetic estate: every source into the lake, scored against the
generator's ground truth.

The Kafka topic is consumed in micro-batches of 500 events, as a streaming job would, so
late and redelivered events cross batch boundaries.

    python benchmarks/quality.py [--seed 7] [--json reports/quality.json]
"""

from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path

from uil import pipeline, synthetic
from uil.lake import Lake
from uil.mapping import all_mappings

MICRO_BATCH = 500


def _micro_batches(path: Path, out: Path) -> list[Path]:
    lines = [ln for f in sorted(path.glob("*.jsonl")) for ln in f.read_text().splitlines()]
    out.mkdir(parents=True, exist_ok=True)
    files = []
    for i in range(0, len(lines), MICRO_BATCH):
        f = out / f"batch_{i // MICRO_BATCH:03d}.jsonl"
        f.write_text("\n".join(lines[i : i + MICRO_BATCH]) + "\n")
        files.append(f)
    return files


def run(seed: int) -> dict:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        synthetic.build(root / "feeds", seed=seed)
        truth = json.loads((root / "feeds/truth.json").read_text())
        lake = Lake(root / "lake")
        per_source = {}
        for m in all_mappings():
            path = root / "feeds" / m.feed
            batches = _micro_batches(path, root / "mb") if m.reader["type"] == "kafka" else [path]
            agg: dict = {}
            for i, b in enumerate(batches):
                s = pipeline.run(m, b, lake, batch_id=f"{m.source}-{i}").as_dict()
                for k in (
                    "read",
                    "mapped",
                    "quarantined",
                    "collapsed",
                    "inserted",
                    "updated",
                    "stale",
                ):
                    agg[k] = agg.get(k, 0) + s[k]
            agg["batches"] = len(batches)
            per_source[m.source] = agg

        quarantined = lake.rows("quarantine")
        # score by native record id: an order's events share one injected defect
        id_field = {m.source: m.fields[0].source for m in all_mappings()}
        caught = {
            (r["source"].split(".")[0], json.loads(r["record"])[id_field[r["source"]]])
            for r in quarantined
        }
        label = {"vendor_file": "vendor"}
        injected = {(label.get(d["source"], d["source"]), d["id"]) for d in truth["defects"]}
        orders = {r["order_id"]: r for r in lake.rows("order")}
        expected = truth["orders"]
        wrong = [
            k
            for k, v in expected.items()
            if k not in orders
            or orders[k]["status"] != v["status"]
            or f"{orders[k]['amount']:.2f}" != v["amount"]
        ]
        extra = set(orders) - set(expected)
        customers = {r["customer_id"] for r in lake.rows("customer")}
        orphans = sum(1 for r in orders.values() if r["customer_id"] not in customers)
        return {
            "seed": seed,
            "sources": per_source,
            "records_read": sum(s["read"] for s in per_source.values()),
            "injected_defects": len(injected),
            "quarantined_records": len(quarantined),
            "defects_caught": len(injected & caught),
            "false_quarantines": len(caught - injected),
            "orders_expected": len(expected),
            "orders_wrong_or_missing": len(wrong),
            "orders_unexpected": len(extra),
            "order_state_accuracy_pct": round(100 * (1 - len(wrong) / len(expected)), 2),
            "orders_referencing_quarantined_customer": orphans,
            "canonical_tables": lake.query(
                "select 'customer', count(*) from customer union all "
                "select 'order', count(*) from \"order\""
            ),
        }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--json")
    a = ap.parse_args()
    r = run(a.seed)
    print(json.dumps(r, indent=2, default=str))
    if a.json:
        Path(a.json).parent.mkdir(parents=True, exist_ok=True)
        Path(a.json).write_text(json.dumps(r, indent=2, default=str) + "\n")


if __name__ == "__main__":
    main()
