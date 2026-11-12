"""Protocol readers. One per *protocol*, not per source: every SAP feed uses `sap_odata`, every
Salesforce object uses `salesforce`, and so on. Each yields raw records as dicts.

Readers read from a local landing path (what an extract job or a connector drops into object
storage) and, for SAP and Salesforce, can also page a live endpoint.
"""

from __future__ import annotations

import csv
import io
import json
import urllib.request
from collections.abc import Iterator
from pathlib import Path


def _json_pages(path: Path) -> Iterator[dict]:
    files = sorted(path.glob("*.json")) if path.is_dir() else [path]
    for f in files:
        yield json.loads(f.read_text(encoding="utf-8"))


def _get_json(url: str, headers: dict[str, str]) -> dict:
    req = urllib.request.Request(url, headers={"Accept": "application/json", **headers})
    with urllib.request.urlopen(req, timeout=60) as r:  # noqa: S310 - URL comes from config
        return json.loads(r.read())


def read_sap_odata(
    path: Path | None = None, url: str | None = None, headers: dict | None = None, **_
) -> Iterator[dict]:  # noqa: ANN003
    """OData v2 JSON: `{"d": {"results": [...], "__next": "..."}}`. Drops `__metadata`."""
    pages = _json_pages(path) if path else None
    next_url = url
    while True:
        if pages is not None:
            page = next(pages, None)
            if page is None:
                return
        else:
            if not next_url:
                return
            page = _get_json(next_url, headers or {})
        d = page.get("d", page)
        for rec in d.get("results", []):
            yield {k: v for k, v in rec.items() if k != "__metadata"}
        if pages is None:
            next_url = d.get("__next")


def read_salesforce(
    path: Path | None = None,
    instance_url: str | None = None,
    soql: str | None = None,
    token: str | None = None,
    **_,
) -> Iterator[dict]:  # noqa: ANN003
    """REST query results: `{"records": [...], "done": false, "nextRecordsUrl": "..."}`."""
    if path:
        for page in _json_pages(path):
            for rec in page.get("records", []):
                yield _strip_attributes(rec)
        return
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{instance_url}/services/data/v60.0/query?q={urllib.request.quote(soql or '')}"
    while url:
        page = _get_json(url, headers)
        for rec in page.get("records", []):
            yield _strip_attributes(rec)
        url = f"{instance_url}{page['nextRecordsUrl']}" if not page.get("done") else None


def _strip_attributes(rec: dict) -> dict:
    return {
        k: (_strip_attributes(v) if isinstance(v, dict) else v)
        for k, v in rec.items()
        if k != "attributes"
    }


def read_kafka(
    path: Path | None = None,
    topic: str | None = None,
    bootstrap: str | None = None,
    group: str = "uil",
    **_,
) -> Iterator[dict]:  # noqa: ANN003
    """Kafka records with their position: the JSON value plus `_partition` and `_offset`.

    From a path: a JSONL capture of the topic (`{"partition", "offset", "key", "value"}` per
    line), which is what the tests and benchmark use. Live: confluent-kafka consumer.
    """
    if path:
        files = sorted(path.glob("*.jsonl")) if path.is_dir() else [path]
        for f in files:
            for line in f.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    m = json.loads(line)
                    yield {**m["value"], "_partition": m["partition"], "_offset": m["offset"]}
        return
    from confluent_kafka import Consumer  # optional dependency

    c = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    c.subscribe([topic])
    try:
        while (msg := c.poll(5.0)) is not None:
            if msg.error():
                raise RuntimeError(msg.error())
            yield {
                **json.loads(msg.value()),
                "_partition": msg.partition(),
                "_offset": msg.offset(),
            }
    finally:
        c.close()


def read_flatfile(
    path: Path, delimiter: str = ",", encoding: str = "utf-8", skip_rows: int = 0, **_
) -> Iterator[dict]:  # noqa: ANN003
    """Delimited vendor files. Vendors send Latin-1, semicolons and preamble lines; all three
    are configuration."""
    files = sorted(p for p in path.iterdir() if p.is_file()) if path.is_dir() else [path]
    for f in files:
        text = f.read_bytes().decode(encoding)
        lines = text.splitlines()[skip_rows:]
        yield from csv.DictReader(io.StringIO("\n".join(lines)), delimiter=delimiter)


READERS = {
    "sap_odata": read_sap_odata,
    "salesforce": read_salesforce,
    "kafka": read_kafka,
    "flatfile": read_flatfile,
}


def read(reader_cfg: dict, path: Path | None) -> Iterator[dict]:
    cfg = dict(reader_cfg)
    kind = cfg.pop("type")
    if kind not in READERS:
        raise ValueError(f"unknown reader {kind!r}")
    return READERS[kind](path=path, **cfg)
