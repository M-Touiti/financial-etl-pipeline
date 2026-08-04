import json
from collections.abc import AsyncGenerator

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaError

from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)


class KafkaConsumerClient:
    """
    Async Kafka consumer wrapper.

    Reads messages from the market-data-events topic in batches
    for efficient processing. Commits offsets only after successful
    batch processing (at-least-once delivery guarantee).
    """

    def __init__(self) -> None:
        self._consumer: AIOKafkaConsumer | None = None

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            settings.KAFKA_TOPIC_MARKET_DATA,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id=settings.KAFKA_CONSUMER_GROUP,
            auto_offset_reset=settings.KAFKA_AUTO_OFFSET_RESET,
            enable_auto_commit=False,  # manual commit after processing
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            max_poll_records=settings.KAFKA_MAX_POLL_RECORDS,
        )
        await self._consumer.start()
        logger.info(
            "Kafka consumer started",
            topic=settings.KAFKA_TOPIC_MARKET_DATA,
            group=settings.KAFKA_CONSUMER_GROUP,
        )

    async def stop(self) -> None:
        if self._consumer:
            await self._consumer.stop()
            logger.info("Kafka consumer stopped")

    async def consume_batch(self) -> AsyncGenerator[list[dict], None]:
        """
        Yields batches of raw messages.
        Commits offset only after the caller has successfully processed the batch.
        """
        if not self._consumer:
            raise RuntimeError("Consumer not started. Call start() first.")

        async for msg_batch in self._consumer:
            batch = [msg_batch.value]

            # Drain remaining messages up to max_poll_records
            try:
                records = await self._consumer.getmany(
                    timeout_ms=100,
                    max_records=settings.KAFKA_MAX_POLL_RECORDS - 1,
                )
                for _tp, messages in records.items():
                    batch.extend(m.value for m in messages)
            except KafkaError:
                pass  # proceed with partial batch

            yield batch
            await self._consumer.commit()
            logger.debug("Committed offsets", batch_size=len(batch))


class KafkaProducerClient:
    """
    Async Kafka producer for the Dead Letter Topic (DLT).
    Failed records that cannot be processed are sent here for inspection.
    """

    def __init__(self) -> None:
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",  # wait for all replicas
            retries=3,
        )
        await self._producer.start()
        logger.info("Kafka producer started", dlt_topic=settings.KAFKA_TOPIC_DLT)

    async def stop(self) -> None:
        if self._producer:
            await self._producer.stop()

    async def send_to_dlt(self, record: dict, error: str, stage: str) -> None:
        """Sends a failed record to the Dead Letter Topic."""
        if not self._producer:
            raise RuntimeError("Producer not started. Call start() first.")

        dlt_message = {
            "original_record": record,
            "error": error,
            "stage": stage,
            "topic": settings.KAFKA_TOPIC_MARKET_DATA,
        }
        await self._producer.send_and_wait(settings.KAFKA_TOPIC_DLT, value=dlt_message)
        logger.warning("Record sent to DLT", stage=stage, error=error[:100])
