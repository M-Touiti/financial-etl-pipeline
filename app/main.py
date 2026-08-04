import asyncio
from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routers import pipeline as pipeline_router
from app.core.config import settings
from app.core.logging import configure_logging, get_logger
from app.pipeline.orchestrator import ETLPipeline

configure_logging(debug=settings.DEBUG)
logger = get_logger(__name__)

etl_pipeline = ETLPipeline()
scheduler = AsyncIOScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting ETL pipeline service", version=settings.APP_VERSION)

    # Start file-based scheduler
    if settings.SCHEDULER_ENABLED:
        scheduler.add_job(
            etl_pipeline.run_file_mode,
            trigger="interval",
            minutes=settings.FILE_SCAN_INTERVAL_MINUTES,
            id="file_etl",
            replace_existing=True,
        )
        scheduler.start()
        logger.info(
            "File scanner scheduler started", interval_minutes=settings.FILE_SCAN_INTERVAL_MINUTES
        )

    # Start Kafka consumer in background
    kafka_task = asyncio.create_task(
        etl_pipeline.run_kafka_mode(),
        name="kafka-etl-consumer",
    )

    yield

    # Shutdown
    kafka_task.cancel()
    try:
        await kafka_task
    except asyncio.CancelledError:
        pass

    if scheduler.running:
        scheduler.shutdown()

    logger.info("ETL pipeline service stopped")


def create_app() -> FastAPI:
    app = FastAPI(
        title=settings.APP_NAME,
        version=settings.APP_VERSION,
        description="""
## Financial Market Data ETL Pipeline

A production-grade Extract-Transform-Load pipeline for financial market data.

### Pipeline Flow
```
Kafka / CSV / JSON
       │
       ▼ Extract
Raw Market Events
       │
       ▼ Clean (Pandas + Pydantic)
Validated Records + DLT for invalid
       │
       ▼ Normalize
EUR conversion + outlier detection (z-score)
       │
       ▼ Load (PostgreSQL bulk upsert)
market_records + daily_summaries
```

### Monitoring
This API lets you observe pipeline health, inspect validation errors,
query OHLCV daily summaries, and review per-run statistics.
        """,
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["GET"],
        allow_headers=["*"],
    )

    app.include_router(pipeline_router.router, prefix="/api/v1")

    @app.get("/health", include_in_schema=False)
    async def health():
        return {"status": "ok", "version": settings.APP_VERSION}

    return app


app = create_app()
