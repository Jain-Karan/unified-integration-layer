"""Field transforms available to declarative mappings.

A mapping names transforms in YAML (`transform: [strip, upper, {prefix: "sap:"}]`). Each one is a
small, tested function here, so source-specific quirks (SAP `/Date(...)/` timestamps, German
decimal commas, 15-character Salesforce IDs) are handled once and reused by every source that
has them, instead of being re-implemented in each pipeline.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from datetime import UTC, date, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any
from zoneinfo import ZoneInfo


class TransformError(ValueError):
    """A value couldn't be transformed. The record goes to quarantine with this message."""


Transform = Callable[..., Any]
REGISTRY: dict[str, Transform] = {}


def transform(name: str) -> Callable[[Transform], Transform]:
    def deco(fn: Transform) -> Transform:
        REGISTRY[name] = fn
        return fn

    return deco


def _none_passthrough(fn: Transform) -> Transform:
    def wrapped(v: Any, *a: Any, **kw: Any) -> Any:
        return None if v is None else fn(v, *a, **kw)

    wrapped.__name__ = fn.__name__
    return wrapped


@transform("strip")
@_none_passthrough
def strip(v: Any) -> Any:
    return v.strip() if isinstance(v, str) else v


@transform("upper")
@_none_passthrough
def upper(v: str) -> str:
    return str(v).upper()


@transform("lower")
@_none_passthrough
def lower(v: str) -> str:
    return str(v).lower()


@transform("null_if_empty")
def null_if_empty(v: Any) -> Any:
    return None if v is None or (isinstance(v, str) and not v.strip()) else v


@transform("prefix")
@_none_passthrough
def prefix(v: Any, p: str) -> str:
    return f"{p}{v}"


@transform("map")
@_none_passthrough
def map_values(v: Any, table: dict, default: Any = "__error__") -> Any:
    key = v.strip() if isinstance(v, str) else v
    if key in table:
        return table[key]
    if default == "__error__":
        raise TransformError(f"no mapping for {v!r}")
    return default


_SAP_DATE = re.compile(r"^/Date\((-?\d+)([+-]\d{4})?\)/$")


@transform("sap_datetime")
@_none_passthrough
def sap_datetime(v: str) -> datetime:
    """SAP OData v2 JSON dates: `/Date(1609459200000)/`, milliseconds since the epoch in UTC.
    An optional `+0100` suffix is an offset *annotation*; the milliseconds are already UTC."""
    m = _SAP_DATE.match(str(v))
    if not m:
        raise TransformError(f"not an SAP OData date: {v!r}")
    return datetime.fromtimestamp(int(m.group(1)) / 1000, tz=UTC)


@transform("epoch_ms")
@_none_passthrough
def epoch_ms(v: Any) -> datetime:
    try:
        return datetime.fromtimestamp(int(v) / 1000, tz=UTC)
    except (ValueError, TypeError, OverflowError) as e:
        raise TransformError(f"bad epoch millis {v!r}") from e


@transform("datetime")
@_none_passthrough
def parse_datetime(v: Any, format: str | None = None, tz: str = "UTC") -> datetime:  # noqa: A002
    """ISO 8601 by default (a trailing Z is fine); otherwise strptime `format`. Naive values are
    interpreted in `tz` and converted to UTC."""
    if isinstance(v, datetime):
        dt = v
    else:
        s = str(v).strip()
        try:
            dt = datetime.strptime(s, format) if format else datetime.fromisoformat(s)
        except ValueError as e:
            raise TransformError(f"bad datetime {v!r}: {e}") from e
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo(tz))
    return dt.astimezone(UTC)


@transform("date")
@_none_passthrough
def parse_date(v: Any, format: str | None = None) -> date:  # noqa: A002
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    s = str(v).strip()
    try:
        return datetime.strptime(s, format).date() if format else date.fromisoformat(s[:10])
    except ValueError as e:
        raise TransformError(f"bad date {v!r}: {e}") from e


@transform("decimal")
@_none_passthrough
def to_decimal(v: Any, locale: str = "en", places: int = 2) -> Decimal:
    """`locale: de` reads `1.234,56`; `en` reads `1,234.56`. Rounds half-up to `places`."""
    if isinstance(v, (int, float, Decimal)):
        d = Decimal(str(v))
    else:
        s = str(v).strip().replace(" ", "")
        s = s.replace(".", "").replace(",", ".") if locale == "de" else s.replace(",", "")
        try:
            d = Decimal(s)
        except InvalidOperation as e:
            raise TransformError(f"bad decimal {v!r}") from e
    return d.quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP)


@transform("cents")
@_none_passthrough
def cents(v: Any) -> Decimal:
    try:
        return (Decimal(int(v)) / 100).quantize(Decimal("0.01"))
    except (ValueError, TypeError) as e:
        raise TransformError(f"bad cents value {v!r}") from e


_SF_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ012345"


@transform("sf_id18")
@_none_passthrough
def sf_id18(v: str) -> str:
    """Normalise a Salesforce ID to its case-insensitive 18-character form. Reports and exports
    often carry the 15-character case-sensitive form of the same record."""
    s = str(v).strip()
    if len(s) == 18:
        return s
    if len(s) != 15:
        raise TransformError(f"not a Salesforce ID: {v!r}")
    suffix = ""
    for i in range(3):
        chunk = s[i * 5 : i * 5 + 5]
        bits = sum(1 << j for j, ch in enumerate(chunk) if ch.isupper())
        suffix += _SF_ALPHABET[bits]
    return s + suffix


@transform("concat")
def concat(values: list[Any], sep: str = " ") -> str | None:
    parts = [str(x).strip() for x in values if x is not None and str(x).strip()]
    return sep.join(parts) or None


def apply(spec: list, value: Any) -> Any:
    """Apply a YAML transform list: items are names or one-key dicts of name -> args."""
    for step in spec or []:
        if isinstance(step, str):
            name, args = step, {}
        elif isinstance(step, dict) and len(step) == 1:
            name, args = next(iter(step.items()))
        else:
            raise ValueError(f"bad transform step {step!r}")
        fn = REGISTRY.get(name)
        if fn is None:
            raise ValueError(f"unknown transform {name!r}")
        value = fn(value, **args) if isinstance(args, dict) else fn(value, args)
    return value
