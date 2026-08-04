from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.infrastructure.database.models import (
    DailySummaryORM,
    MarketRecordORM,
    PipelineRunORM,
    ValidationErrorORM,
)
from app.infrastructure.database.session import get_db

router = APIRouter(prefix="/pipeline", tags=["Pipeline Monitoring"])

DB = Annotated[AsyncSession, Depends(get_db)]


@router.get("/runs")
async def list_pipeline_runs(
    db: DB,
    status: str | None = Query(None, description="Filter by status"),
    limit: int = Query(20, ge=1, le=100),
):
    """List recent pipeline run executions with their stats."""
    query = select(PipelineRunORM).order_by(desc(PipelineRunORM.started_at)).limit(limit)
    if status:
        query = query.where(PipelineRunORM.status == status)
    result = await db.execute(query)
    runs = result.scalars().all()

    return [
        {
            "run_id": r.run_id,
            "source": r.source,
            "source_detail": r.source_detail,
            "status": r.status,
            "records_extracted": r.records_extracted,
            "records_valid": r.records_valid,
            "records_invalid": r.records_invalid,
            "records_loaded": r.records_loaded,
            "records_outliers": r.records_outliers,
            "success_rate": round(r.records_valid / r.records_extracted * 100, 1)
            if r.records_extracted > 0
            else 0.0,
            "duration_seconds": (
                (r.completed_at - r.started_at).total_seconds() if r.completed_at else None
            ),
            "started_at": r.started_at,
            "completed_at": r.completed_at,
            "error_message": r.error_message,
        }
        for r in runs
    ]


@router.get("/runs/{run_id}")
async def get_pipeline_run(run_id: str, db: DB):
    """Get details and validation errors for a specific pipeline run."""
    result = await db.execute(select(PipelineRunORM).where(PipelineRunORM.run_id == run_id))
    run = result.scalar_one_or_none()
    if not run:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    errors = await db.execute(
        select(ValidationErrorORM)
        .where(ValidationErrorORM.run_id == run_id)
        .order_by(desc(ValidationErrorORM.occurred_at))
        .limit(50)
    )

    return {
        "run": {
            "run_id": run.run_id,
            "source": run.source,
            "source_detail": run.source_detail,
            "status": run.status,
            "records_extracted": run.records_extracted,
            "records_valid": run.records_valid,
            "records_invalid": run.records_invalid,
            "records_loaded": run.records_loaded,
            "records_outliers": run.records_outliers,
            "started_at": run.started_at,
            "completed_at": run.completed_at,
            "error_message": run.error_message,
        },
        "validation_errors": [
            {
                "event_id": e.event_id,
                "error_message": e.error_message,
                "stage": e.stage,
                "occurred_at": e.occurred_at,
            }
            for e in errors.scalars().all()
        ],
    }


@router.get("/metrics")
async def pipeline_metrics(db: DB):
    """
    Overall pipeline health metrics.
    Returns record counts, outlier rates, and top symbols.
    """
    total_records = (
        await db.execute(select(func.count()).select_from(MarketRecordORM))
    ).scalar_one()

    total_outliers = (
        await db.execute(select(func.count()).where(MarketRecordORM.is_outlier.is_(True)))
    ).scalar_one()

    total_errors = (
        await db.execute(select(func.count()).select_from(ValidationErrorORM))
    ).scalar_one()

    total_runs = (await db.execute(select(func.count()).select_from(PipelineRunORM))).scalar_one()

    # Top 5 symbols by trade count
    top_symbols = (
        await db.execute(
            select(MarketRecordORM.symbol, func.count().label("count"))
            .group_by(MarketRecordORM.symbol)
            .order_by(desc("count"))
            .limit(5)
        )
    ).all()

    return {
        "total_records_processed": total_records,
        "total_outliers_detected": total_outliers,
        "outlier_rate_pct": round(total_outliers / total_records * 100, 2) if total_records else 0,
        "total_validation_errors": total_errors,
        "total_pipeline_runs": total_runs,
        "top_symbols": [{"symbol": s, "count": c} for s, c in top_symbols],
    }


@router.get("/daily-summaries")
async def get_daily_summaries(
    db: DB,
    symbol: str | None = Query(None),
    date: str | None = Query(None, description="yyyy-MM-dd"),
    limit: int = Query(50, ge=1, le=200),
):
    """Query computed OHLCV daily summaries."""
    query = (
        select(DailySummaryORM)
        .order_by(desc(DailySummaryORM.date), DailySummaryORM.symbol)
        .limit(limit)
    )

    if symbol:
        query = query.where(DailySummaryORM.symbol == symbol.upper())
    if date:
        query = query.where(DailySummaryORM.date == date)

    result = await db.execute(query)
    summaries = result.scalars().all()

    return [
        {
            "symbol": s.symbol,
            "market": s.market,
            "date": s.date,
            "open_eur": float(s.open_price_eur),
            "high_eur": float(s.high_price_eur),
            "low_eur": float(s.low_price_eur),
            "close_eur": float(s.close_price_eur),
            "volume": s.total_volume,
            "vwap_eur": float(s.vwap_eur),
            "trade_count": s.trade_count,
            "outlier_count": s.outlier_count,
        }
        for s in summaries
    ]
