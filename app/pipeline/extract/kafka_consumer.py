from app.core.logging import get_logger
from app.domain.models import RawMarketEvent
from app.infrastructure.kafka.client import KafkaConsumerClient

logger = get_logger(__name__)


class KafkaExtractor:
    """
    Extracts raw market events from a Kafka topic.

    Converts raw JSON payloads to RawMarketEvent domain objects.
    Invalid JSON or missing required fields are logged and skipped.
    """

    def __init__(self, consumer: KafkaConsumerClient) -> None:
        self.consumer = consumer

    def parse_raw_event(self, payload: dict) -> RawMarketEvent | None:
        """
        Parses a raw Kafka payload into a RawMarketEvent.
        Returns None if the payload is too malformed to even attempt cleaning.
        """
        try:
            return RawMarketEvent(
                event_id=str(payload.get("event_id", "")),
                symbol=str(payload.get("symbol", "")),
                price=payload.get("price"),
                volume=payload.get("volume"),
                currency=str(payload.get("currency", "")),
                market=str(payload.get("market", "")),
                timestamp=payload.get("timestamp", ""),
                source=str(payload.get("source", "kafka")),
                raw_payload=payload,
            )
        except Exception as e:
            logger.warning("Cannot parse raw payload", error=str(e), payload=str(payload)[:200])
            return None

    def extract_batch(self, raw_messages: list[dict]) -> tuple[list[RawMarketEvent], int]:
        """
        Converts a list of raw Kafka messages to RawMarketEvent objects.
        Returns (events, skipped_count).
        """
        events: list[RawMarketEvent] = []
        skipped = 0

        for msg in raw_messages:
            event = self.parse_raw_event(msg)
            if event:
                events.append(event)
            else:
                skipped += 1

        logger.info("Extraction complete", extracted=len(events), skipped=skipped)
        return events, skipped
