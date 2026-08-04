"""
Cleaner — Stage 2 of the ETL pipeline.

Converts raw RawMarketEvent objects to validated CleanMarketRecord objects.
Uses Pandas for batch operations on the DataFrame and Pydantic for
per-record validation.

Cleaning operations:
- Strip whitespace from string fields
- Parse and coerce price/volume to correct types
- Parse timestamp to datetime (handles multiple formats)
- Validate required fields with Pydantic
- Separate valid records from invalid ones
"""

from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation

import pandas as pd
from pydantic import ValidationError as PydanticValidationError

from app.core.logging import get_logger
from app.domain.models import CleanMarketRecord, RawMarketEvent

logger = get_logger(__name__)

TIMESTAMP_FORMATS = [
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S%z",
]


def parse_timestamp(raw: str | datetime) -> datetime | None:
    """Tries multiple timestamp formats. Returns None if all fail."""
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=UTC)

    if not isinstance(raw, str) or not raw.strip():
        return None

    for fmt in TIMESTAMP_FORMATS:
        try:
            dt = datetime.strptime(raw.strip(), fmt)
            return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
        except ValueError:
            continue
    return None


def parse_decimal(raw) -> Decimal | None:
    """
    Safely parses any value to Decimal. Supports EU-style formatting
    (e.g. "1.234,56") where "." is the thousands separator and "," is
    the decimal separator.
    """
    try:
        if raw is None or str(raw).strip() in ("", "nan", "null", "None"):
            return None
        text = str(raw).strip()
        if "," in text:
            text = text.replace(".", "").replace(",", ".")
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def parse_int(raw) -> int | None:
    """Safely parses any value to int."""
    try:
        if raw is None or str(raw).strip() in ("", "nan", "null", "None"):
            return None
        return int(float(str(raw).strip()))
    except (ValueError, TypeError):
        return None


class DataCleaner:
    """
    Cleans a batch of RawMarketEvent objects using a Pandas DataFrame
    for vectorized operations, then validates each record with Pydantic.
    """

    def clean_batch(
        self, events: list[RawMarketEvent]
    ) -> tuple[list[CleanMarketRecord], list[tuple[RawMarketEvent, str]]]:
        """
        Cleans a batch of raw events.

        Returns:
            (valid_records, invalid_records)
            where invalid_records is a list of (event, error_message) tuples.
        """
        if not events:
            return [], []

        # ── Step 1: Build DataFrame for vectorized cleaning ───────────────────
        df = pd.DataFrame(
            [
                {
                    "event_id": e.event_id,
                    "symbol": e.symbol,
                    "price": e.price,
                    "volume": e.volume,
                    "currency": e.currency,
                    "market": e.market,
                    "timestamp": e.timestamp,
                    "source": e.source,
                    "raw_payload": e.raw_payload,
                }
                for e in events
            ]
        )

        # Vectorized string cleaning
        for col in ["symbol", "currency", "market", "source"]:
            df[col] = df[col].astype(str).str.strip().str.upper()

        # Parse price and volume
        df["price_parsed"] = df["price"].apply(parse_decimal)
        df["volume_parsed"] = df["volume"].apply(parse_int)
        df["timestamp_parsed"] = df["timestamp"].apply(parse_timestamp)

        # ── Step 2: Validate each row with Pydantic ───────────────────────────
        valid: list[CleanMarketRecord] = []
        invalid: list[tuple[RawMarketEvent, str]] = []

        events_by_id = {e.event_id: e for e in events}

        for _, row in df.iterrows():
            event_id = row["event_id"]
            raw_event = events_by_id.get(event_id)

            try:
                record = CleanMarketRecord(
                    event_id=event_id,
                    symbol=row["symbol"],
                    price=row["price_parsed"]
                    if row["price_parsed"] is not None
                    else Decimal("-1"),  # will fail Pydantic validation
                    volume=row["volume_parsed"] if row["volume_parsed"] is not None else -1,
                    currency=row["currency"],
                    market=row["market"],
                    timestamp=row["timestamp_parsed"] or datetime.min,
                    source=row["source"],
                )
                valid.append(record)

            except (PydanticValidationError, Exception) as e:
                error_msg = str(e)[:500]
                if raw_event:
                    invalid.append((raw_event, error_msg))
                logger.warning("Record failed cleaning", event_id=event_id, error=error_msg[:100])

        logger.info("Cleaning complete", valid=len(valid), invalid=len(invalid))
        return valid, invalid
