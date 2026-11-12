"""Every declarative mapping loads, targets only canonical fields, and maps a golden record."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from uil.mapping import ContractViolation, Mapping, all_mappings


def test_all_mappings_load():
    names = {m.source for m in all_mappings()}
    assert names == {
        "sap.customers",
        "sap.sales_orders",
        "salesforce.accounts",
        "salesforce.opportunities",
        "kafka.orders",
        "vendor.distributor_customers",
    }


def test_sap_order_golden_record():
    m = Mapping.load("sap_orders")
    row = m.map_record(
        {
            "SalesOrder": "5000001",
            "SoldToParty": "0001000002",
            "SalesOrderDate": "/Date(1730419200000)/",
            "TransactionCurrency": "eur",
            "TotalNetAmount": "1499.90",
            "OverallSDProcessStatus": "C",
            "LastChangeDateTime": "/Date(1730505600000+0000)/",
        }
    )
    assert row["order_id"] == "sap:5000001"
    assert row["customer_id"] == "sap:0001000002"
    assert row["currency"] == "EUR" and row["amount"] == Decimal("1499.90")
    assert row["status"] == "shipped"
    assert row["updated_at"] == datetime(2024, 11, 2, tzinfo=UTC)


def test_salesforce_15_and_18_char_account_ids_join_to_one_customer():
    m = Mapping.load("salesforce_opportunities")
    base = {
        "Id": "006A0000003kAbcIAE",
        "CloseDate": "2026-09-30",
        "CurrencyIsoCode": "USD",
        "Amount": 1200.0,
        "StageName": "Closed Won",
        "LastModifiedDate": "2026-10-01T12:00:00.000+0000",
    }
    a = m.map_record({**base, "AccountId": "001A0000006Vm9r"})
    b = m.map_record({**base, "AccountId": "001A0000006Vm9rIAC"})
    assert a["customer_id"] == b["customer_id"] == "sf:001A0000006Vm9rIAC"


def test_source_contract_rejects_missing_required_field():
    m = Mapping.load("sap_customers")
    with pytest.raises(ContractViolation):
        m.map_record({"Customer": "0001000002", "Country": "DE", "CreationDate": "/Date(0)/"})


def test_unknown_canonical_field_is_rejected_at_load(tmp_path):
    p = tmp_path / "bad.yaml"
    p.write_text(
        "source: x\nentity: customer\nreader: {type: flatfile}\n"
        "fields:\n  customer_id: {from: a}\n  vip_flag: {from: b}\n"
    )
    with pytest.raises(ValueError, match="vip_flag"):
        Mapping.load(p)
