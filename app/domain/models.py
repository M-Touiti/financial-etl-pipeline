"""
Domain models for the ETL pipeline.

Each model represents a stage in the pipeline:
  RawMarketEvent → CleanMarketRecord → NormalizedMarketRecord → DailySummary
"""

from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator


class DataSource(str, Enum):
    KAFKA = "kafka"
    CSV = "csv"
    JSON = "json"
    API = "api"


class PipelineStatus(str, Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIAL = "partial"


class ValidationStatus(str, Enum):
    VALID = "valid"
    INVALID = "invalid"
    WARNING = "warning"


# ── Stage 1: Extract — raw event from Kafka / file ───────────────────────────


class RawMarketEvent(BaseModel):
    """Raw event as received from Kafka or file — no transformation yet."""

    event_id: str = Field(default_factory=lambda: str(uuid4()))
    symbol: str
    price: Any  # raw — may be string, float, or None
    volume: Any
    currency: str
    market: str
    timestamp: str | datetime
    source: str
    raw_payload: dict | None = None

    model_config = {"arbitrary_types_allowed": True}


# ── Stage 2: Validate + Clean ─────────────────────────────────────────────────

SUPPORTED_CURRENCIES = {"EUR", "USD", "GBP", "CHF", "JPY", "CAD", "AUD"}
KNOWN_MARKETS = {"NYSE", "NASDAQ", "LSE", "EURONEXT", "TSX", "ASX", "XETRA"}


class CleanMarketRecord(BaseModel):
    """Validated and cleaned market record — ready for normalization."""

    event_id: str
    symbol: str
    price: Decimal
    volume: int
    currency: str
    market: str
    timestamp: datetime
    source: str
    validation_warnings: list[str] = Field(default_factory=list)

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, v: str) -> str:
        return v.strip().upper()

    @field_validator("currency")
    @classmethod
    def validate_currency(cls, v: str) -> str:
        normalized = v.strip().upper()
        if normalized not in SUPPORTED_CURRENCIES:
            raise ValueError(f"Unsupported currency: {v}. Supported: {SUPPORTED_CURRENCIES}")
        return normalized

    @field_validator("price")
    @classmethod
    def validate_price(cls, v: Decimal) -> Decimal:
        if v <= 0:
            raise ValueError(f"Price must be positive, got: {v}")
        return v

    @field_validator("volume")
    @classmethod
    def validate_volume(cls, v: int) -> int:
        if v < 0:
            raise ValueError(f"Volume cannot be negative, got: {v}")
        return v


# ── Stage 3: Normalize / Enrich ───────────────────────────────────────────────

# Hardcoded FX rates (EUR base) — real system: call ECB API or Kafka FX feed
FX_RATES_TO_EUR: dict[str, float] = {
    "EUR": 1.0,
    "USD": 0.926,
    "GBP": 1.165,
    "CHF": 1.075,
    "JPY": 0.0062,
    "CAD": 0.685,
    "AUD": 0.610,
}


class NormalizedMarketRecord(BaseModel):
    """Market record normalized to EUR, enriched with derived metrics."""

    event_id: str
    symbol: str
    price_original: Decimal
    price_eur: Decimal
    volume: int
    value_eur: Decimal  # price_eur × volume
    currency_original: str
    fx_rate: Decimal
    market: str
    timestamp: datetime
    date: str  # yyyy-MM-dd — partitioning key
    hour: int  # 0–23 — for intraday analytics
    source: str
    is_outlier: bool = False  # flagged by z-score analysis
    processed_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


# ── Stage 4: Aggregate ────────────────────────────────────────────────────────


class DailySummary(BaseModel):
    """OHLCV daily summary per symbol/market — computed by aggregator."""

    symbol: str
    market: str
    date: str
    open_price_eur: Decimal
    high_price_eur: Decimal
    low_price_eur: Decimal
    close_price_eur: Decimal
    total_volume: int
    total_value_eur: Decimal
    vwap_eur: Decimal  # volume-weighted average price
    trade_count: int
    outlier_count: int
    computed_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


# ── Pipeline monitoring ───────────────────────────────────────────────────────


class PipelineRun(BaseModel):
    """Tracks one execution of the ETL pipeline."""

    run_id: str = Field(default_factory=lambda: str(uuid4()))
    source: DataSource
    source_detail: str  # topic name or file path
    status: PipelineStatus = PipelineStatus.RUNNING
    records_extracted: int = 0
    records_valid: int = 0
    records_invalid: int = 0
    records_loaded: int = 0
    records_outliers: int = 0
    error_message: str | None = None
    started_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    completed_at: datetime | None = None

    @property
    def duration_seconds(self) -> float | None:
        if self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None

    @property
    def success_rate(self) -> float:
        if self.records_extracted == 0:
            return 0.0
        return round(self.records_valid / self.records_extracted * 100, 1)


class ValidationError(BaseModel):
    """A record that failed validation — sent to DLT."""

    event_id: str
    raw_payload: dict
    error_message: str
    stage: str  # "clean", "normalize", "load"
    source: str
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
