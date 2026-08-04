"""
Sample Kafka event producer — for testing and demo purposes.

Publishes synthetic market data events to the Kafka topic.
Usage: python scripts/produce_sample_events.py [--count 100] [--interval 0.1]
"""
import argparse
import asyncio
import json
import random
import uuid
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "market-data-events"

SYMBOLS = {
    "AAPL": ("USD", "NASDAQ"),
    "GOOG": ("USD", "NASDAQ"),
    "MSFT": ("USD", "NASDAQ"),
    "AMZN": ("USD", "NASDAQ"),
    "BNP.PA": ("EUR", "EURONEXT"),
    "LLOY.L": ("GBP", "LSE"),
    "NESN.SW": ("CHF", "XETRA"),
    "7203.T": ("JPY", "TSX"),
}

BASE_PRICES = {
    "AAPL": 185.0, "GOOG": 175.0, "MSFT": 420.0, "AMZN": 195.0,
    "BNP.PA": 62.0, "LLOY.L": 0.52, "NESN.SW": 104.0, "7203.T": 2850.0,
}


def generate_event(symbol: str) -> dict:
    currency, market = SYMBOLS[symbol]
    base_price = BASE_PRICES[symbol]
    price = round(base_price * (1 + random.uniform(-0.02, 0.02)), 4)
    volume = random.randint(100_000, 5_000_000)

    return {
        "event_id": str(uuid.uuid4()),
        "symbol": symbol,
        "price": price,
        "volume": volume,
        "currency": currency,
        "market": market,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": random.choice(["reuters", "bloomberg", "euronext"]),
    }


async def produce(count: int, interval: float) -> None:
    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()
    print(f"Publishing {count} events to {TOPIC}...")

    try:
        for i in range(count):
            symbol = random.choice(list(SYMBOLS.keys()))
            event = generate_event(symbol)
            await producer.send(TOPIC, value=event)

            if (i + 1) % 10 == 0:
                print(f"  Sent {i + 1}/{count} events")

            await asyncio.sleep(interval)

    finally:
        await producer.stop()
        print(f"Done. {count} events published to '{TOPIC}'.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Produce sample market events to Kafka")
    parser.add_argument("--count", type=int, default=50, help="Number of events to produce")
    parser.add_argument("--interval", type=float, default=0.05, help="Seconds between events")
    args = parser.parse_args()
    asyncio.run(produce(args.count, args.interval))
