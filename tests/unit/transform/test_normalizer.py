"""Unit tests for DataNormalizer and DailyAggregator."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from app.domain.models import CleanMarketRecord
from app.pipeline.transform.aggregator import DailyAggregator
from app.pipeline.transform.normalizer import DataNormalizer


def make_clean_record(**kwargs) -> CleanMarketRecord:
    defaults = dict(
        event_id="EVT-001",
        symbol="AAPL",
        price=Decimal("185.50"),
        volume=1_000_000,
        currency="USD",
        market="NASDAQ",
        timestamp=datetime(2025, 6, 1, 14, 30, tzinfo=UTC),
        source="test",
    )
    defaults.update(kwargs)
    return CleanMarketRecord(**defaults)


# ── DataNormalizer ────────────────────────────────────────────────────────────


class TestDataNormalizer:
    @pytest.fixture
    def normalizer(self):
        return DataNormalizer()

    def test_eur_record_has_rate_one(self, normalizer):
        record = make_clean_record(currency="EUR", price=Decimal("100.00"))
        result = normalizer.normalize_batch([record])
        assert len(result) == 1
        assert result[0].fx_rate == Decimal("1.0")
        assert result[0].price_eur == Decimal("100.000000")

    def test_usd_converted_to_eur(self, normalizer):
        record = make_clean_record(currency="USD", price=Decimal("108.00"))
        result = normalizer.normalize_batch([record])
        # USD rate = 0.926 → 108 × 0.926 ≈ 100.008
        assert result[0].price_eur > Decimal("90")
        assert result[0].currency_original == "USD"

    def test_value_eur_computed(self, normalizer):
        record = make_clean_record(currency="EUR", price=Decimal("10.00"), volume=100)
        result = normalizer.normalize_batch([record])
        assert result[0].value_eur == Decimal("1000.0000")

    def test_date_and_hour_extracted(self, normalizer):
        record = make_clean_record(timestamp=datetime(2025, 6, 15, 9, 30, tzinfo=UTC))
        result = normalizer.normalize_batch([record])
        assert result[0].date == "2025-06-15"
        assert result[0].hour == 9

    def test_outlier_detection(self, normalizer):
        """A price 4 standard deviations from the mean should be an outlier."""
        base_price = Decimal("100.00")
        records = [
            make_clean_record(event_id=f"E{i}", price=base_price, volume=100) for i in range(9)
        ]
        # Add a clear outlier (price × 10 → z-score >> 3)
        records.append(make_clean_record(event_id="OUTLIER", price=base_price * 10, volume=100))

        result = normalizer.normalize_batch(records)
        outlier_results = [r for r in result if r.is_outlier]
        assert len(outlier_results) >= 1
        assert any(r.event_id == "OUTLIER" for r in outlier_results)

    def test_empty_batch(self, normalizer):
        assert normalizer.normalize_batch([]) == []


# ── DailyAggregator ───────────────────────────────────────────────────────────


class TestDailyAggregator:
    @pytest.fixture
    def aggregator(self):
        return DailyAggregator()

    @pytest.fixture
    def normalized_records(self):
        """5 AAPL trades on 2025-06-01."""
        from app.domain.models import NormalizedMarketRecord

        prices = [100, 105, 102, 108, 103]
        records = []
        for i, p in enumerate(prices):
            records.append(
                NormalizedMarketRecord(
                    event_id=f"E{i}",
                    symbol="AAPL",
                    price_original=Decimal(str(p)),
                    price_eur=Decimal(str(p)),
                    volume=1000,
                    value_eur=Decimal(str(p * 1000)),
                    currency_original="EUR",
                    fx_rate=Decimal("1"),
                    market="NASDAQ",
                    timestamp=datetime(2025, 6, 1, 14, i, tzinfo=UTC),
                    date="2025-06-01",
                    hour=14,
                    source="test",
                    is_outlier=False,
                )
            )
        return records

    def test_ohlcv_correct(self, aggregator, normalized_records):
        summaries = aggregator.aggregate(normalized_records)
        assert len(summaries) == 1
        s = summaries[0]
        assert s.symbol == "AAPL"
        assert s.date == "2025-06-01"
        assert float(s.open_price_eur) == 100  # first price
        assert float(s.close_price_eur) == 103  # last price
        assert float(s.high_price_eur) == 108
        assert float(s.low_price_eur) == 100
        assert s.total_volume == 5000
        assert s.trade_count == 5

    def test_vwap_correct(self, aggregator, normalized_records):
        summaries = aggregator.aggregate(normalized_records)
        s = summaries[0]
        # VWAP = total_value / total_volume = 518000 / 5000 = 103.6
        total_value = sum(p * 1000 for p in [100, 105, 102, 108, 103])
        expected_vwap = total_value / 5000
        assert abs(float(s.vwap_eur) - expected_vwap) < 0.01

    def test_multiple_symbols_produce_multiple_summaries(self, aggregator):
        from app.domain.models import NormalizedMarketRecord

        records = []
        for symbol in ["AAPL", "GOOG", "MSFT"]:
            records.append(
                NormalizedMarketRecord(
                    event_id=f"{symbol}-1",
                    symbol=symbol,
                    price_original=Decimal("100"),
                    price_eur=Decimal("100"),
                    volume=500,
                    value_eur=Decimal("50000"),
                    currency_original="EUR",
                    fx_rate=Decimal("1"),
                    market="NASDAQ",
                    timestamp=datetime(2025, 6, 1, tzinfo=UTC),
                    date="2025-06-01",
                    hour=9,
                    source="test",
                    is_outlier=False,
                )
            )
        summaries = aggregator.aggregate(records)
        assert len(summaries) == 3
        symbols = {s.symbol for s in summaries}
        assert symbols == {"AAPL", "GOOG", "MSFT"}

    def test_empty_batch(self, aggregator):
        assert aggregator.aggregate([]) == []
