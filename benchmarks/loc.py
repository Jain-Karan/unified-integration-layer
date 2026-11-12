"""Code needed per source: bespoke pipeline vs declarative mapping.

Counts logical source lines: blank lines, comments and docstrings are excluded. Both sides
share the canonical models and the lake writer, so neither is counted. The framework's own code
(readers, transforms, mapping engine, pipeline) is counted once, in full, including the live API
paging the bespoke scripts don't have, so the totals are conservative for the framework.

    python benchmarks/loc.py [--json reports/loc.json]
"""

from __future__ import annotations

import argparse
import io
import json
import math
import tokenize
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FRAMEWORK = ["readers.py", "transforms.py", "mapping.py", "pipeline.py"]


def py_sloc(path: Path) -> int:
    src = path.read_text(encoding="utf-8")
    skip: set[int] = set()
    prev = tokenize.INDENT
    for tok in tokenize.generate_tokens(io.StringIO(src).readline):
        if tok.type == tokenize.STRING and prev in (
            tokenize.INDENT,
            tokenize.NEWLINE,
            tokenize.DEDENT,
        ):
            skip.update(range(tok.start[0], tok.end[0] + 1))  # docstring / bare string
        if tok.type not in (tokenize.NL, tokenize.COMMENT):
            prev = tok.type
    return sum(
        1
        for i, line in enumerate(src.splitlines(), 1)
        if line.strip() and not line.strip().startswith("#") and i not in skip
    )


def yaml_sloc(path: Path) -> int:
    return sum(
        1
        for line in path.read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    )


def measure() -> dict:
    sources = {}
    for y in sorted((ROOT / "src/uil/mappings").glob("*.yaml")):
        sources[y.stem] = {
            "bespoke": py_sloc(ROOT / "baselines" / f"{y.stem}.py"),
            "declarative": yaml_sloc(y),
        }
    framework = {f: py_sloc(ROOT / "src/uil" / f) for f in FRAMEWORK}
    n = len(sources)
    bespoke = sum(s["bespoke"] for s in sources.values())
    declarative = sum(s["declarative"] for s in sources.values())
    fw = sum(framework.values())
    per_src_saving = (bespoke - declarative) / n
    return {
        "sources": sources,
        "framework": framework,
        "per_source": {
            "bespoke_mean": round(bespoke / n, 1),
            "declarative_mean": round(declarative / n, 1),
            "reduction_pct": round(100 * (1 - declarative / bespoke), 1),
        },
        "total": {
            "sources": n,
            "bespoke": bespoke,
            "declarative_incl_framework": declarative + fw,
            "reduction_pct": round(100 * (1 - (declarative + fw) / bespoke), 1),
            "break_even_sources": math.ceil(fw / per_src_saving),
        },
    }


def _delta(reduction_pct: float) -> str:
    return f"{reduction_pct}% less" if reduction_pct >= 0 else f"{-reduction_pct}% more"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json")
    a = ap.parse_args()
    r = measure()
    print(f"{'source':28}{'bespoke':>9}{'yaml':>7}")
    for name, s in r["sources"].items():
        print(f"{name:28}{s['bespoke']:>9}{s['declarative']:>7}")
    p, t = r["per_source"], r["total"]
    print(
        f"\nper source: {p['bespoke_mean']} -> {p['declarative_mean']} lines "
        f"({p['reduction_pct']}% less)"
    )
    print(f"framework, counted once: {sum(r['framework'].values())} lines {r['framework']}")
    print(
        f"all {t['sources']} sources incl. framework: {t['bespoke']} -> "
        f"{t['declarative_incl_framework']} lines ({_delta(t['reduction_pct'])}); "
        f"break-even at {t['break_even_sources']} sources"
    )
    if a.json:
        Path(a.json).parent.mkdir(parents=True, exist_ok=True)
        Path(a.json).write_text(json.dumps(r, indent=2) + "\n")


if __name__ == "__main__":
    main()
