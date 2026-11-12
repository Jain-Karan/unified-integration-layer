from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from uil.transforms import TransformError, apply, sf_id18


@pytest.mark.parametrize(
    "id15,id18",
    [
        ("a0D30000001n7Pi", "a0D30000001n7PiEAI"),
        ("001A0000006Vm9r", "001A0000006Vm9rIAC"),
    ],
)
def test_sf_id18_matches_salesforce_casesafe_ids(id15, id18):
    assert sf_id18(id15) == id18
    assert sf_id18(id18) == id18  # idempotent on 18-char ids


def test_sf_id18_rejects_garbage():
    with pytest.raises(TransformError):
        sf_id18("001-short")


def test_sap_datetime_is_utc_and_ignores_offset_annotation():
    got = apply(["sap_datetime"], "/Date(1730419200000+0060)/")
    assert got == datetime(2024, 11, 1, tzinfo=UTC)


@pytest.mark.parametrize(
    "spec,raw,want",
    [
        ([{"decimal": {"locale": "de"}}], "1.234,565", Decimal("1234.57")),
        (["decimal"], "1234.565", Decimal("1234.57")),
        (["cents"], 12345, Decimal("123.45")),
    ],
)
def test_money(spec, raw, want):
    assert apply(spec, raw) == want


def test_naive_local_dates_are_converted_to_utc():
    spec = [{"datetime": {"format": "%d.%m.%Y", "tz": "Europe/Berlin"}}]
    assert apply(spec, "01.07.2026") == datetime(2026, 6, 30, 22, tzinfo=UTC)


def test_salesforce_timestamp_with_compact_offset():
    assert apply(["datetime"], "2026-10-01T12:00:00.000+0000") == datetime(
        2026, 10, 1, 12, tzinfo=UTC
    )


def test_epoch_ms_then_date():
    assert apply(["epoch_ms", "date"], 1730419200000) == date(2024, 11, 1)


def test_map_rejects_unknown_codes():
    with pytest.raises(TransformError):
        apply([{"map": {"table": {"A": "open"}}}], "Z")


def test_impossible_calendar_date_is_an_error():
    with pytest.raises(TransformError):
        apply([{"datetime": {"format": "%d.%m.%Y"}}], "31.02.2026")
