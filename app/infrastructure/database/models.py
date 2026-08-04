import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class MarketRecordORM(Base):
    """Normalized market price records — one row per Kafka event."""

    __tablename__ = "market_records"

    event_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    price_original: Mapped[Decimal] = mapped_column(Numeric(19, 6), nullable=False)
    price_eur: Mapped[Decimal] = mapped_column(Numeric(19, 6), nullable=False)
    volume: Mapped[int] = mapped_column(BigInteger, nullable=False)
    value_eur: Mapped[Decimal] = mapped_column(Numeric(19, 4), nullable=False)
    currency_original: Mapped[str] = mapped_column(String(3), nullable=False)
    fx_rate: Mapped[Decimal] = mapped_column(Numeric(10, 6), nullable=False)
    market: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    date: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    hour: Mapped[int] = mapped_column(Integer, nullable=False)
    source: Mapped[str] = mapped_column(String(50), nullable=False)
    is_outlier: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class DailySummaryORM(Base):
    """OHLCV daily aggregates per symbol/market — computed by aggregator step."""

    __tablename__ = "daily_summaries"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    market: Mapped[str] = mapped_column(String(50), nullable=False)
    date: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    open_price_eur: Mapped[Decimal] = mapped_column(Numeric(19, 6), nullable=False)
    high_price_eur: Mapped[Decimal] = mapped_column(Numeric(19, 6), nullable=False)
    low_price_eur: Mapped[Decimal] = mapped_column(Numeric(19, 6), nullable=False)
    close_price_eur: Mapped[Decimal] = mapped_column(Numeric(19, 6), nullable=False)
    total_volume: Mapped[int] = mapped_column(BigInteger, nullable=False)
    total_value_eur: Mapped[Decimal] = mapped_column(Numeric(19, 4), nullable=False)
    vwap_eur: Mapped[Decimal] = mapped_column(Numeric(19, 6), nullable=False)
    trade_count: Mapped[int] = mapped_column(Integer, nullable=False)
    outlier_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        UniqueConstraint("symbol", "market", "date", name="uq_daily_summary_symbol_market_date"),
    )


class PipelineRunORM(Base):
    """Tracks each pipeline execution for monitoring and auditing."""

    __tablename__ = "pipeline_runs"

    run_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    source: Mapped[str] = mapped_column(String(20), nullable=False)
    source_detail: Mapped[str] = mapped_column(String(500), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="running", index=True)
    records_extracted: Mapped[int] = mapped_column(Integer, default=0)
    records_valid: Mapped[int] = mapped_column(Integer, default=0)
    records_invalid: Mapped[int] = mapped_column(Integer, default=0)
    records_loaded: Mapped[int] = mapped_column(Integer, default=0)
    records_outliers: Mapped[int] = mapped_column(Integer, default=0)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class ValidationErrorORM(Base):
    """Records that failed validation — stored for inspection and reprocessing."""

    __tablename__ = "validation_errors"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    event_id: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    run_id: Mapped[str | None] = mapped_column(String(100), nullable=True, index=True)
    raw_payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    error_message: Mapped[str] = mapped_column(Text, nullable=False)
    stage: Mapped[str] = mapped_column(String(50), nullable=False)
    source: Mapped[str] = mapped_column(String(50), nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
