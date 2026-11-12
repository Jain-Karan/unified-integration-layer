"""Command line entry point: generate synthetic feeds, ingest them, query the lake."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from . import pipeline, synthetic
from .lake import Lake
from .mapping import Mapping, all_mappings


def _cmd_generate(a: argparse.Namespace) -> int:
    synthetic.build(Path(a.out), seed=a.seed, scale=a.scale, defect_rate=a.defect_rate)
    print(f"synthetic feeds written to {a.out}")
    return 0


def _cmd_ingest(a: argparse.Namespace) -> int:
    lake = Lake(Path(a.lake))
    feeds = Path(a.feeds)
    maps = [Mapping.load(n) for n in a.mapping] if a.mapping else all_mappings()
    rc = 0
    for m in maps:
        path = feeds / m.feed
        stats = pipeline.run(m, path, lake, batch_id=a.batch_id)
        print(json.dumps(stats.as_dict()))
        if stats.quarantined and a.strict:
            rc = 2
    return rc


def _cmd_query(a: argparse.Namespace) -> int:
    lake = Lake(Path(a.lake))
    for row in lake.query(a.sql):
        print("\t".join("" if v is None else str(v) for v in row))
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="uil", description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("generate", help="write synthetic SAP/Salesforce/Kafka/flat-file feeds")
    g.add_argument("--out", default="data/feeds")
    g.add_argument("--seed", type=int, default=7)
    g.add_argument("--scale", type=int, default=1)
    g.add_argument("--defect-rate", type=float, default=0.03)
    g.set_defaults(fn=_cmd_generate)

    i = sub.add_parser("ingest", help="run mappings into the Iceberg lake")
    i.add_argument("--feeds", default="data/feeds")
    i.add_argument("--lake", default="data/lake")
    i.add_argument("--mapping", action="append", help="mapping name (repeatable); default all")
    i.add_argument("--batch-id", default="manual")
    i.add_argument("--strict", action="store_true", help="exit 2 if anything was quarantined")
    i.set_defaults(fn=_cmd_ingest)

    q = sub.add_parser("query", help="SQL over the canonical tables (DuckDB)")
    q.add_argument("sql")
    q.add_argument("--lake", default="data/lake")
    q.set_defaults(fn=_cmd_query)

    a = p.parse_args(argv)
    return a.fn(a)


if __name__ == "__main__":
    sys.exit(main())
