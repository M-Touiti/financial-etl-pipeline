"""Unit tests for the DataCleaner — no DB, no Kafka needed."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from app.domain.models import RawMarketEvent
from app.pipeline.transform.cleaner import DataCleaner, parse_decimal, parse_timestamp

# ── Helper ────────────────────────────────────────────────────────────────────


def make_event(**kwargs) -> RawMarketEvent:
    defaults = dict(
        event_id="EVT-001",
        symbol="AAPL",
        price="185.50",
        volume="1000000",
        currency="USD",
        market="NASDAQ",
        timestamp="2025-06-01T14:30:00Z",
        source="test",
    )
    defaults.update(kwargs)
    return RawMarketEvent(**defaults)


# ── parse_decimal ─────────────────────────────────────────────────────────────


class TestParseDecimal:
    def test_valid_float_string(self):
        assert parse_decimal("185.50") == Decimal("185.50")

    def test_valid_comma_decimal(self):
        assert parse_decimal("1.234,56") == Decimal("1234.56")

    def test_integer_string(self):
        assert parse_decimal("100") == Decimal("100")

    def test_none_returns_none(self):
        assert parse_decimal(None) is None

    def test_empty_string_returns_none(self):
        assert parse_decimal("") is None

    def test_nan_returns_none(self):
        assert parse_decimal("nan") is None

    def test_invalid_string_returns_none(self):
        assert parse_decimal("not-a-number") is None


# ── parse_timestamp ───────────────────────────────────────────────────────────


class TestParseTimestamp:
    def test_iso_utc_format(self):
        result = parse_timestamp("2025-06-01T14:30:00Z")
        assert result is not None
        assert result.year == 2025 and result.month == 6 and result.day == 1

    def test_iso_no_tz(self):
        result = parse_timestamp("2025-06-01T14:30:00")
        assert result is not None
        assert result.tzinfo == UTC

    def test_datetime_passthrough(self):
        dt = datetime(2025, 6, 1, 14, 30, tzinfo=UTC)
        assert parse_timestamp(dt) == dt

    def test_invalid_returns_none(self):
        assert parse_timestamp("not-a-date") is None

    def test_empty_returns_none(self):
        assert parse_timestamp("") is None


# ── DataCleaner ───────────────────────────────────────────────────────────────


class TestDataCleaner:
    @pytest.fixture
    def cleaner(self):
        return DataCleaner()

    def test_clean_valid_batch(self, cleaner):
        events = [make_event(event_id=f"EVT-{i:03d}") for i in range(5)]
        valid, invalid = cleaner.clean_batch(events)
        assert len(valid) == 5
        assert len(invalid) == 0

    def test_symbol_uppercased(self, cleaner):
        events = [make_event(symbol="  aapl  ")]
        valid, _ = cleaner.clean_batch(events)
        assert valid[0].symbol == "AAPL"

    def test_currency_uppercased_and_validated(self, cleaner):
        events = [make_event(currency="usd")]
        valid, _ = cleaner.clean_batch(events)
        assert valid[0].currency == "USD"

    def test_invalid_currency_rejected(self, cleaner):
        events = [make_event(currency="BITCOIN")]
        valid, invalid = cleaner.clean_batch(events)
        assert len(valid) == 0
        assert len(invalid) == 1
        assert "BITCOIN" in invalid[0][1]

    def test_negative_price_rejected(self, cleaner):
        events = [make_event(price="-10.00")]
        valid, invalid = cleaner.clean_batch(events)
        assert len(valid) == 0
        assert len(invalid) == 1

    def test_null_price_rejected(self, cleaner):
        events = [make_event(price=None)]
        valid, invalid = cleaner.clean_batch(events)
        assert len(valid) == 0
        assert len(invalid) == 1

    def test_mixed_batch(self, cleaner):
        events = [
            make_event(event_id="VALID-1"),
            make_event(event_id="INVALID-1", price=None),
            make_event(event_id="VALID-2"),
            make_event(event_id="INVALID-2", currency="XYZ"),
        ]
        valid, invalid = cleaner.clean_batch(events)
        assert len(valid) == 2
        assert len(invalid) == 2

    def test_empty_batch(self, cleaner):
        valid, invalid = cleaner.clean_batch([])
        assert valid == []
        assert invalid == []
