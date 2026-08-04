"""
Normalizer — Stage 3 of the ETL pipeline.

Operations:
1. Convert all prices to EUR using FX rates
2. Compute value_eur = price_eur × volume
3. Extract date and hour for partitioning
4. Detect price outliers using z-score per symbol/day group
"""

from decimal import Decimal

import pandas as pd

from app.core.config import settings
from app.core.logging import get_logger
from app.domain.models import FX_RATES_TO_EUR, CleanMarketRecord, NormalizedMarketRecord

logger = get_logger(__name__)


class DataNormalizer:
    """
    Normalizes and enriches a batch of clean market records.
    Uses Pandas for vectorized operations — much faster than row-by-row.
    """

    def __init__(self, base_currency: str = settings.BASE_CURRENCY) -> None:
        self.base_currency = base_currency
        self.fx_rates = FX_RATES_TO_EUR

    def normalize_batch(self, records: list[CleanMarketRecord]) -> list[NormalizedMarketRecord]:
        """
        Normalizes a batch of clean records to the base currency (EUR).
        Detects outliers using z-score within each (symbol, date) group.
        """
        if not records:
            return []

        # ── Build DataFrame ────────────────────────────────────────────────────
        df = pd.DataFrame(
            [
                {
                    "event_id": r.event_id,
                    "symbol": r.symbol,
                    "price": float(r.price),
                    "volume": r.volume,
                    "currency": r.currency,
                    "market": r.market,
                    "timestamp": r.timestamp,
                    "source": r.source,
                }
                for r in records
            ]
        )

        # ── FX Normalization ───────────────────────────────────────────────────
        df["fx_rate"] = df["currency"].map(self.fx_rates).fillna(1.0)
        df["price_eur"] = df["price"] * df["fx_rate"]
        df["value_eur"] = df["price_eur"] * df["volume"]

        # ── Date/Hour extraction ───────────────────────────────────────────────
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df["date"] = df["timestamp"].dt.strftime("%Y-%m-%d")
        df["hour"] = df["timestamp"].dt.hour

        # ── Outlier detection (robust z-score per symbol/date group) ───────────
        # Uses median + MAD (median absolute deviation) rather than mean/std,
        # since mean/std are skewed by the very outliers they're meant to catch
        # (a single strong spike inflates std enough to mask its own z-score).
        df["is_outlier"] = False
        for (_symbol, _date), group in df.groupby(["symbol", "date"]):
            if len(group) < 3:  # need at least 3 points for meaningful comparison
                continue
            median = group["price_eur"].median()
            abs_dev = (group["price_eur"] - median).abs()
            mad = abs_dev.median()
            if mad == 0:
                # No spread in the majority of points — any deviation stands out.
                outlier_mask = abs_dev > 0
            else:
                # 0.6745 scales MAD to be comparable to a normal std deviation.
                modified_z_scores = 0.6745 * abs_dev / mad
                outlier_mask = modified_z_scores > settings.OUTLIER_ZSCORE_THRESHOLD
            df.loc[group.index, "is_outlier"] = outlier_mask

        outlier_count = df["is_outlier"].sum()
        if outlier_count > 0:
            logger.warning(
                "Outliers detected",
                count=int(outlier_count),
                symbols=df[df["is_outlier"]]["symbol"].unique().tolist(),
            )

        # ── Build NormalizedMarketRecord objects ───────────────────────────────
        normalized: list[NormalizedMarketRecord] = []
        for _, row in df.iterrows():
            normalized.append(
                NormalizedMarketRecord(
                    event_id=row["event_id"],
                    symbol=row["symbol"],
                    price_original=Decimal(str(round(row["price"], 6))),
                    price_eur=Decimal(str(round(row["price_eur"], 6))),
                    volume=int(row["volume"]),
                    value_eur=Decimal(str(round(row["value_eur"], 4))),
                    currency_original=row["currency"],
                    fx_rate=Decimal(str(row["fx_rate"])),
                    market=row["market"],
                    timestamp=row["timestamp"].to_pydatetime(),
                    date=row["date"],
                    hour=int(row["hour"]),
                    source=row["source"],
                    is_outlier=bool(row["is_outlier"]),
                )
            )

        logger.info("Normalization complete", records=len(normalized), outliers=int(outlier_count))
        return normalized
