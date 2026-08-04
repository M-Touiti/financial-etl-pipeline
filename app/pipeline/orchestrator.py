"""
Pipeline Orchestrator — coordinates Extract → Transform → Load.

Two execution modes:
1. Kafka mode: continuously consumes from topic in a loop
2. File mode: processes files from the input directory (triggered by scheduler)
"""

from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.logging import get_logger
from app.domain.models import DataSource, PipelineRun, PipelineStatus
from app.infrastructure.database.session import AsyncSessionLocal
from app.infrastructure.kafka.client import KafkaConsumerClient, KafkaProducerClient
from app.pipeline.extract.file_reader import FileExtractor
from app.pipeline.extract.kafka_consumer import KafkaExtractor
from app.pipeline.load.db_writer import DatabaseWriter
from app.pipeline.transform.aggregator import DailyAggregator
from app.pipeline.transform.cleaner import DataCleaner
from app.pipeline.transform.normalizer import DataNormalizer

logger = get_logger(__name__)


class ETLPipeline:
    """
    End-to-end ETL pipeline orchestrator.

    Pipeline flow:
    ┌────────────┐    ┌─────────────┐    ┌────────────┐    ┌──────────┐
    │  EXTRACT   │ →  │   CLEAN     │ →  │  NORMALIZE │ →  │   LOAD   │
    │ Kafka/File │    │ Pydantic +  │    │ FX rates + │    │ Postgres │
    │            │    │ Pandas      │    │ Outliers   │    │ upsert   │
    └────────────┘    └─────────────┘    └────────────┘    └──────────┘
                              │                                  │
                              ▼                                  ▼
                         DLT / DB                         DailySummary
                       (invalid recs)                     aggregation
    """

    def __init__(self) -> None:
        self.cleaner = DataCleaner()
        self.normalizer = DataNormalizer()
        self.aggregator = DailyAggregator()

    async def _process_batch(
        self,
        raw_messages: list[dict],
        run: PipelineRun,
        db: AsyncSession,
        dlt_producer: KafkaProducerClient | None = None,
    ) -> PipelineRun:
        """Processes one batch of raw messages through all pipeline stages."""
        writer = DatabaseWriter(db)

        # ── Extract ────────────────────────────────────────────────────────────
        extractor = KafkaExtractor(consumer=None)  # type: ignore
        raw_events, extraction_skipped = extractor.extract_batch(raw_messages)
        run.records_extracted += len(raw_events) + extraction_skipped

        # ── Clean ──────────────────────────────────────────────────────────────
        clean_records, invalid_records = self.cleaner.clean_batch(raw_events)
        run.records_valid += len(clean_records)
        run.records_invalid += len(invalid_records)

        # Handle invalid records → DLT + DB
        validation_errors = []
        for raw_event, error_msg in invalid_records:
            if dlt_producer:
                await dlt_producer.send_to_dlt(
                    record=raw_event.raw_payload or {},
                    error=error_msg,
                    stage="clean",
                )
            validation_errors.append(
                {
                    "event_id": raw_event.event_id,
                    "raw_payload": raw_event.raw_payload or {},
                    "error_message": error_msg,
                    "stage": "clean",
                    "source": raw_event.source,
                }
            )

        if validation_errors:
            await writer.save_validation_errors(validation_errors, run.run_id)

        # ── Normalize ──────────────────────────────────────────────────────────
        normalized = self.normalizer.normalize_batch(clean_records)
        run.records_outliers += sum(1 for r in normalized if r.is_outlier)

        # ── Load market records ────────────────────────────────────────────────
        inserted = await writer.upsert_market_records(normalized)
        run.records_loaded += inserted

        # ── Aggregate + load daily summaries ───────────────────────────────────
        summaries = self.aggregator.aggregate(normalized)
        await writer.upsert_daily_summaries(summaries)

        # Update pipeline run in DB
        await writer.save_pipeline_run(run)
        await db.commit()

        return run

    async def run_kafka_mode(self) -> None:
        """
        Continuously consumes from Kafka and processes batches.
        Runs indefinitely — managed by the app lifespan.
        """
        consumer_client = KafkaConsumerClient()
        dlt_producer = KafkaProducerClient()

        await consumer_client.start()
        await dlt_producer.start()

        logger.info("Pipeline started in Kafka mode")

        try:
            async for batch in consumer_client.consume_batch():
                run = PipelineRun(
                    source=DataSource.KAFKA,
                    source_detail=settings.KAFKA_TOPIC_MARKET_DATA,
                )
                async with AsyncSessionLocal() as db:
                    try:
                        run = await self._process_batch(batch, run, db, dlt_producer)
                        run.status = PipelineStatus.COMPLETED
                        run.completed_at = datetime.now(UTC)
                        writer = DatabaseWriter(db)
                        await writer.save_pipeline_run(run)
                        await db.commit()
                        logger.info(
                            "Batch processed",
                            run_id=run.run_id,
                            extracted=run.records_extracted,
                            loaded=run.records_loaded,
                        )
                    except Exception as e:
                        run.status = PipelineStatus.FAILED
                        run.error_message = str(e)[:1000]
                        run.completed_at = datetime.now(UTC)
                        logger.error("Batch failed", run_id=run.run_id, error=str(e))
        finally:
            await consumer_client.stop()
            await dlt_producer.stop()

    async def run_file_mode(self) -> list[PipelineRun]:
        """
        Processes all pending files in the input directory.
        Returns a list of PipelineRun results.
        """
        file_extractor = FileExtractor()
        pending = file_extractor.get_pending_files()

        if not pending:
            logger.info("No pending files to process")
            return []

        results: list[PipelineRun] = []

        for file_path in pending:
            run = PipelineRun(
                source=DataSource.CSV if file_path.suffix == ".csv" else DataSource.JSON,
                source_detail=file_path.name,
            )
            async with AsyncSessionLocal() as db:
                writer = DatabaseWriter(db)
                try:
                    raw_events, _ = file_extractor.extract_file(file_path)
                    raw_messages = [
                        {
                            "event_id": e.event_id,
                            "symbol": e.symbol,
                            "price": e.price,
                            "volume": e.volume,
                            "currency": e.currency,
                            "market": e.market,
                            "timestamp": e.timestamp,
                            "source": e.source,
                        }
                        for e in raw_events
                    ]
                    run = await self._process_batch(raw_messages, run, db)
                    run.status = PipelineStatus.COMPLETED
                    run.completed_at = datetime.now(UTC)
                    logger.info("File processed", file=file_path.name, loaded=run.records_loaded)
                except Exception as e:
                    run.status = PipelineStatus.FAILED
                    run.error_message = str(e)[:1000]
                    run.completed_at = datetime.now(UTC)
                    logger.error("File processing failed", file=file_path.name, error=str(e))
                finally:
                    await writer.save_pipeline_run(run)
                    await db.commit()

            results.append(run)

        return results
