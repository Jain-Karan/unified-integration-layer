"""The canonical model every source is mapped into, and its contract.

Two entities (customer, order) are enough to show the hard parts of enterprise integration:
cross-source identity, money in different formats, timestamps in different conventions, and
status vocabularies that never line up. The pydantic models are the canonical contract; the
Arrow schemas derived from them are the sink contract the Iceberg tables are created from.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum

import pyarrow as pa
from pydantic import BaseModel, ConfigDict, Field, field_validator


class OrderStatus(StrEnum):
    OPEN = "open"
    WON = "won"
    SHIPPED = "shipped"
    CANCELLED = "cancelled"
    CLOSED_LOST = "closed_lost"


class Lineage(BaseModel):
    """Where a canonical row came from. Stamped by the pipeline, not by mappings."""

    source: str
    batch_id: str
    ingested_at: datetime


# Officially assigned ISO 3166-1 alpha-2 codes (249). Two capital letters is not enough: vendor
# files send placeholders such as XX, and "UK"/"EU" are reserved, not assigned.
ISO_COUNTRIES = frozenset(
    """
    AD AE AF AG AI AL AM AO AQ AR AS AT AU AW AX AZ BA BB BD BE BF BG BH BI
    BJ BL BM BN BO BQ BR BS BT BV BW BY BZ CA CC CD CF CG CH CI CK CL CM CN
    CO CR CU CV CW CX CY CZ DE DJ DK DM DO DZ EC EE EG EH ER ES ET FI FJ FK
    FM FO FR GA GB GD GE GF GG GH GI GL GM GN GP GQ GR GS GT GU GW GY HK HM
    HN HR HT HU ID IE IL IM IN IO IQ IR IS IT JE JM JO JP KE KG KH KI KM KN
    KP KR KW KY KZ LA LB LC LI LK LR LS LT LU LV LY MA MC MD ME MF MG MH MK
    ML MM MN MO MP MQ MR MS MT MU MV MW MX MY MZ NA NC NE NF NG NI NL NO NP
    NR NU NZ OM PA PE PF PG PH PK PL PM PN PR PS PT PW PY QA RE RO RS RU RW
    SA SB SC SD SE SG SH SI SJ SK SL SM SN SO SR SS ST SV SX SY SZ TC TD TF
    TG TH TJ TK TL TM TN TO TR TT TV TW TZ UA UG UM US UY UZ VA VC VE VG VI
    VN VU WF WS YE YT ZA ZM ZW
""".split()  # noqa: SIM905 - readable 24-per-line table
)


class Customer(BaseModel):
    model_config = ConfigDict(extra="forbid")

    customer_id: str = Field(min_length=3)  # "<source system>:<native id>"
    name: str = Field(min_length=1)
    country: str = Field(pattern=r"^[A-Z]{2}$")  # ISO 3166-1 alpha-2, assigned codes only
    city: str | None = None
    email: str | None = Field(default=None, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    segment: str | None = None
    created_at: datetime
    updated_at: datetime

    @field_validator("created_at", "updated_at")
    @classmethod
    def _aware(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamps must be timezone-aware (UTC)")
        return v

    @field_validator("country")
    @classmethod
    def _assigned(cls, v: str) -> str:
        if v not in ISO_COUNTRIES:
            raise ValueError(f"{v!r} is not an assigned ISO 3166-1 alpha-2 code")
        return v


class Order(BaseModel):
    model_config = ConfigDict(extra="forbid")

    order_id: str = Field(min_length=3)
    customer_id: str = Field(min_length=3)
    order_date: date
    currency: str = Field(pattern=r"^[A-Z]{3}$")  # ISO 4217
    amount: Decimal = Field(max_digits=18, decimal_places=2, ge=0)
    status: OrderStatus
    updated_at: datetime

    @field_validator("updated_at")
    @classmethod
    def _aware(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamps must be timezone-aware (UTC)")
        return v


ENTITIES: dict[str, type[BaseModel]] = {"customer": Customer, "order": Order}
KEYS = {"customer": "customer_id", "order": "order_id"}

_TS = pa.timestamp("us", tz="UTC")
LINEAGE_FIELDS = [
    pa.field("_source", pa.string(), nullable=False),
    pa.field("_batch_id", pa.string(), nullable=False),
    pa.field("_ingested_at", _TS, nullable=False),
]

ARROW_SCHEMAS: dict[str, pa.Schema] = {
    "customer": pa.schema(
        [
            pa.field("customer_id", pa.string(), nullable=False),
            pa.field("name", pa.string(), nullable=False),
            pa.field("country", pa.string(), nullable=False),
            pa.field("city", pa.string()),
            pa.field("email", pa.string()),
            pa.field("segment", pa.string()),
            pa.field("created_at", _TS, nullable=False),
            pa.field("updated_at", _TS, nullable=False),
            *LINEAGE_FIELDS,
        ]
    ),
    "order": pa.schema(
        [
            pa.field("order_id", pa.string(), nullable=False),
            pa.field("customer_id", pa.string(), nullable=False),
            pa.field("order_date", pa.date32(), nullable=False),
            pa.field("currency", pa.string(), nullable=False),
            pa.field("amount", pa.decimal128(18, 2), nullable=False),
            pa.field("status", pa.string(), nullable=False),
            pa.field("updated_at", _TS, nullable=False),
            *LINEAGE_FIELDS,
        ]
    ),
}
