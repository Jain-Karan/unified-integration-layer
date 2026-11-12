from benchmarks import loc, quality


def test_end_to_end_quality_on_synthetic_estate():
    r = quality.run(seed=3)
    assert r["defects_caught"] == r["injected_defects"] > 0
    assert r["false_quarantines"] == 0
    assert r["orders_wrong_or_missing"] == 0 and r["orders_unexpected"] == 0
    assert r["sources"]["kafka.orders"]["stale"] > 0  # micro-batches really do see late events


def test_loc_benchmark_counts_every_mapping():
    r = loc.measure()
    assert len(r["sources"]) == 6
    assert all(s["declarative"] < s["bespoke"] for s in r["sources"].values())
