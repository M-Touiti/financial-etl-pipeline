"""
Aggregator — Stage 4 of the ETL pipeline.

Computes OHLCV (Open, High, Low, Close, Volume) daily summaries
per symbol/market using Pandas groupby operations.

Also computes:
- VWAP (Volume-Weighted Average Price)
- Trade count
- Outlier count per group
"""

from decimal import Decimal

import pandas as pd

from app.core.logging import get_logger
from app.domain.models import DailySummary, NormalizedMarketRecord

logger = get_logger(__name__)


class DailyAggregator:
    """
    Aggregates normalized market records into OHLCV daily summaries.
    Uses Pandas groupby for efficient vectorized computation.
    """

    def aggregate(self, records: list[NormalizedMarketRecord]) -> list[DailySummary]:
        """
        Computes daily OHLCV summaries for each (symbol, market, date) group.
        Records must be sorted by timestamp for correct open/close computation.
        """
        if not records:
            return []

        df = pd.DataFrame(
            [
                {
                    "symbol": r.symbol,
                    "market": r.market,
                    "date": r.date,
                    "price_eur": float(r.price_eur),
                    "volume": r.volume,
                    "value_eur": float(r.value_eur),
                    "timestamp": r.timestamp,
                    "is_outlier": r.is_outlier,
                }
                for r in records
            ]
        )

        # Sort by timestamp for correct open/close
        df = df.sort_values("timestamp")

        summaries: list[DailySummary] = []

        for (symbol, market, date), group in df.groupby(["symbol", "market", "date"]):
            total_volume = int(group["volume"].sum())
            total_value = float(group["value_eur"].sum())

            # VWAP = sum(price × volume) / sum(volume)
            vwap = total_value / total_volume if total_volume > 0 else 0.0

            summaries.append(
                DailySummary(
                    symbol=str(symbol),
                    market=str(market),
                    date=str(date),
                    open_price_eur=Decimal(str(round(group["price_eur"].iloc[0], 6))),
                    high_price_eur=Decimal(str(round(group["price_eur"].max(), 6))),
                    low_price_eur=Decimal(str(round(group["price_eur"].min(), 6))),
                    close_price_eur=Decimal(str(round(group["price_eur"].iloc[-1], 6))),
                    total_volume=total_volume,
                    total_value_eur=Decimal(str(round(total_value, 4))),
                    vwap_eur=Decimal(str(round(vwap, 6))),
                    trade_count=len(group),
                    outlier_count=int(group["is_outlier"].sum()),
                )
            )

        logger.info("Aggregation complete", groups=len(summaries), total_records=len(records))
        return summaries
