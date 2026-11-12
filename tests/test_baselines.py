"""The bespoke per-source pipelines in `baselines/` and the declarative mappings are equivalent:
same canonical rows, same records rejected. This is what makes the code-size comparison fair."""

import importlib

import pytest

from uil.mapping import Mapping, all_mappings
from uil.readers import read

NAMES = [m.feed.split("/")[-1] for m in all_mappings()]


def _framework(m: Mapping, path):
    good, bad = [], []
    for rec in read(m.reader, path):
        try:
            good.append(m.map_record(rec))
        except Exception:  # noqa: BLE001 - the categories are tested elsewhere
            bad.append(rec)
    return good, bad


@pytest.mark.parametrize("name", NAMES)
def test_bespoke_pipeline_matches_declarative_mapping(name, feeds):
    m = Mapping.load(name)
    want_good, want_bad = _framework(m, feeds / m.feed)
    got_good, got_bad = importlib.import_module(f"baselines.{name}").rows(feeds / m.feed)
    assert len(want_bad) > 0, "feed should contain injected defects"
    assert got_good == want_good
    assert [r for r, _ in got_bad] == want_bad


@pytest.mark.parametrize("name", NAMES)
def test_bespoke_run_leaves_the_same_lake(name, feeds, tmp_path):
    from uil import pipeline
    from uil.lake import Lake

    m = Mapping.load(name)
    a, b = Lake(tmp_path / "framework"), Lake(tmp_path / "bespoke")
    pipeline.run(m, feeds / m.feed, a, batch_id="b1")
    importlib.import_module(f"baselines.{name}").run(feeds / m.feed, b, batch_id="b1")

    def snapshot(lake):
        drop = {"_ingested_at"}
        rows = [{k: v for k, v in r.items() if k not in drop} for r in lake.rows(m.entity)]
        return sorted(rows, key=lambda r: str(sorted(r.items())))

    assert snapshot(a) == snapshot(b)
    assert len(a.rows("quarantine")) == len(b.rows("quarantine"))
