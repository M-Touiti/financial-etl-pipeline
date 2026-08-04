"""
DB Writer — Load stage of the ETL pipeline.

Uses SQLAlchemy 2.0 async with PostgreSQL UPSERT (INSERT ... ON CONFLICT DO UPDATE)
for idempotent loads — safe to re-process the same records without duplicates.

Batch size is configurable (default: 1000 rows per INSERT statement).
"""

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.domain.models import DailySummary, NormalizedMarketRecord, PipelineRun
from app.infrastructure.database.models import (
    DailySummaryORM,
    MarketRecordORM,
    PipelineRunORM,
    ValidationErrorORM,
)

logger = get_logger(__name__)


class DatabaseWriter:
    """Handles all DB write operations for the ETL pipeline."""

    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    async def upsert_market_records(
        self, records: list[NormalizedMarketRecord], batch_size: int = 1000
    ) -> int:
        """
        Bulk upserts normalized market records using PostgreSQL ON CONFLICT DO NOTHING.
        event_id is the natural key — duplicate events are silently skipped.
        Returns the count of rows inserted.
        """
        if not records:
            return 0

        total_inserted = 0

        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            rows = [
                {
                    "event_id": r.event_id,
                    "symbol": r.symbol,
                    "price_original": r.price_original,
                    "price_eur": r.price_eur,
                    "volume": r.volume,
                    "value_eur": r.value_eur,
                    "currency_original": r.currency_original,
                    "fx_rate": r.fx_rate,
                    "market": r.market,
                    "timestamp": r.timestamp,
                    "date": r.date,
                    "hour": r.hour,
                    "source": r.source,
                    "is_outlier": r.is_outlier,
                    "processed_at": r.processed_at,
                }
                for r in batch
            ]

            stmt = pg_insert(MarketRecordORM).values(rows)
            stmt = stmt.on_conflict_do_nothing(index_elements=["event_id"])
            result = await self.db.execute(stmt)
            total_inserted += result.rowcount

        logger.info("Market records upserted", submitted=len(records), inserted=total_inserted)
        return total_inserted

    async def upsert_daily_summaries(self, summaries: list[DailySummary]) -> int:
        """
        Upserts daily OHLCV summaries.
        ON CONFLICT (symbol, market, date) → updates with latest values.
        """
        if not summaries:
            return 0

        rows = [
            {
                "symbol": s.symbol,
                "market": s.market,
                "date": s.date,
                "open_price_eur": s.open_price_eur,
                "high_price_eur": s.high_price_eur,
                "low_price_eur": s.low_price_eur,
                "close_price_eur": s.close_price_eur,
                "total_volume": s.total_volume,
                "total_value_eur": s.total_value_eur,
                "vwap_eur": s.vwap_eur,
                "trade_count": s.trade_count,
                "outlier_count": s.outlier_count,
                "computed_at": s.computed_at,
            }
            for s in summaries
        ]

        stmt = pg_insert(DailySummaryORM).values(rows)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_daily_summary_symbol_market_date",
            set_={
                "high_price_eur": stmt.excluded.high_price_eur,
                "low_price_eur": stmt.excluded.low_price_eur,
                "close_price_eur": stmt.excluded.close_price_eur,
                "total_volume": stmt.excluded.total_volume,
                "total_value_eur": stmt.excluded.total_value_eur,
                "vwap_eur": stmt.excluded.vwap_eur,
                "trade_count": stmt.excluded.trade_count,
                "outlier_count": stmt.excluded.outlier_count,
                "computed_at": stmt.excluded.computed_at,
            },
        )
        result = await self.db.execute(stmt)
        logger.info("Daily summaries upserted", count=len(summaries))
        return result.rowcount

    async def save_pipeline_run(self, run: PipelineRun) -> None:
        """Inserts or updates a pipeline run record."""
        stmt = pg_insert(PipelineRunORM).values(
            run_id=run.run_id,
            source=run.source.value,
            source_detail=run.source_detail,
            status=run.status.value,
            records_extracted=run.records_extracted,
            records_valid=run.records_valid,
            records_invalid=run.records_invalid,
            records_loaded=run.records_loaded,
            records_outliers=run.records_outliers,
            error_message=run.error_message,
            started_at=run.started_at,
            completed_at=run.completed_at,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["run_id"],
            set_={
                "status": stmt.excluded.status,
                "records_extracted": stmt.excluded.records_extracted,
                "records_valid": stmt.excluded.records_valid,
                "records_invalid": stmt.excluded.records_invalid,
                "records_loaded": stmt.excluded.records_loaded,
                "records_outliers": stmt.excluded.records_outliers,
                "error_message": stmt.excluded.error_message,
                "completed_at": stmt.excluded.completed_at,
            },
        )
        await self.db.execute(stmt)

    async def save_validation_errors(self, errors: list[dict], run_id: str) -> None:
        """Persists validation errors for inspection and reprocessing."""
        if not errors:
            return
        rows = [{**err, "run_id": run_id} for err in errors]
        await self.db.execute(pg_insert(ValidationErrorORM).values(rows).on_conflict_do_nothing())
        logger.info("Validation errors saved", count=len(errors))
