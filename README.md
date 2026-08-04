# financial-etl-pipeline

[![CI](https://github.com/M-Touiti/financial-etl-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/M-Touiti/financial-etl-pipeline/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A production-grade Financial Market Data ETL pipeline built with Python. Ingests raw market events from Apache Kafka and CSV/JSON files, processes them through a 4-stage pipeline, and stores normalized OHLCV data in PostgreSQL.

Built as a portfolio project applicable to fintech, data engineering, and any system dealing with high-volume financial data streams.

---

## Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     FINANCIAL ETL PIPELINE                          │
│                                                                     │
│  ┌──────────────┐   ┌──────────────┐   ┌─────────────────────────┐ │
│  │   EXTRACT    │   │   EXTRACT    │   │  @Scheduled (5 min)     │ │
│  │  Kafka topic │   │  CSV / JSON  │   │  File scanner           │ │
│  │  (aiokafka)  │   │  (Pandas)    │   └─────────────────────────┘ │
│  └──────┬───────┘   └──────┬───────┘                               │
│         └──────────────────┘                                        │
│                    │                                                 │
│                    ▼                                                 │
│  ┌─────────────────────────────────────────────────────────┐        │
│  │  CLEAN  (Pandas vectorized + Pydantic per-record)        │        │
│  │  • Strip/normalize strings                               │        │
│  │  • Parse price, volume, timestamp                        │        │
│  │  • Validate: currency, positive price, required fields   │        │
│  │  • Invalid records → DLT (Kafka) + validation_errors DB  │        │
│  └──────────────────────────┬──────────────────────────────┘        │
│                             │                                        │
│                             ▼                                        │
│  ┌─────────────────────────────────────────────────────────┐        │
│  │  NORMALIZE  (Pandas vectorized)                          │        │
│  │  • Convert all prices to EUR (configurable base currency)│        │
│  │  • Compute value_eur = price_eur × volume                │        │
│  │  • Extract date, hour for partitioning                   │        │
│  │  • Outlier detection (z-score per symbol/day)            │        │
│  └──────────────────────────┬──────────────────────────────┘        │
│                             │                                        │
│                             ▼                                        │
│  ┌─────────────────────────────────────────────────────────┐        │
│  │  LOAD  (SQLAlchemy 2.0 async — bulk upsert)              │        │
│  │  • INSERT ... ON CONFLICT DO NOTHING (market_records)    │        │
│  │  • INSERT ... ON CONFLICT DO UPDATE (daily_summaries)    │        │
│  │  • Idempotent — safe to replay the same batch            │        │
│  └──────────────────────────┬──────────────────────────────┘        │
│                             │                                        │
│                             ▼                                        │
│  ┌─────────────────────────────────────────────────────────┐        │
│  │  AGGREGATE  (Pandas groupby)                             │        │
│  │  • OHLCV per (symbol, market, date)                      │        │
│  │  • VWAP = Σ(price × volume) / Σ(volume)                 │        │
│  │  • Outlier count per group                               │        │
│  └─────────────────────────────────────────────────────────┘        │
└─────────────────────────────────────────────────────────────────────┘
                             │
                             ▼
                 REST API (FastAPI monitoring)
                 GET /api/v1/pipeline/runs
                 GET /api/v1/pipeline/metrics
                 GET /api/v1/pipeline/daily-summaries
```

---

## Features

| Feature | Details |
|---|---|
| **Kafka consumer** | aiokafka async, manual offset commit, batch polling |
| **File reader** | CSV (Pandas) + JSON (ndjson), automatic archive/error handling |
| **Data cleaning** | Pydantic v2 validation + Pandas vectorized string ops |
| **FX normalization** | Multi-currency → EUR conversion (configurable rates) |
| **Outlier detection** | Z-score per (symbol, date) group — flags suspicious prices |
| **OHLCV aggregation** | Pandas groupby — open/high/low/close/volume/VWAP per day |
| **Bulk upsert** | PostgreSQL `INSERT ... ON CONFLICT` — idempotent, fast |
| **Dead Letter Queue** | Invalid records → Kafka DLT + `validation_errors` table |
| **Pipeline tracking** | `pipeline_runs` table — status, counts, duration per run |
| **Scheduler** | APScheduler — file directory scan every 5 minutes |
| **REST monitoring API** | Run history, metrics, OHLCV queries, validation errors |
| **Tests** | 20+ unit tests (Pandas transforms, Pydantic validation) |

---

## Tech Stack

- **Python 3.11**
- **FastAPI 0.111** — monitoring REST API
- **aiokafka 0.11** — async Kafka consumer/producer
- **Pandas 2.2** — vectorized data cleaning and aggregation
- **SQLAlchemy 2.0** — async ORM with `Mapped` type annotations
- **asyncpg** — high-performance async PostgreSQL driver
- **Pydantic v2** — per-stage validation models
- **APScheduler** — file directory scheduler
- **Alembic** — async database migrations
- **structlog** — structured JSON logging
- **pytest** — 20+ unit tests (no infrastructure needed)
- **Docker / Docker Compose** — Kafka + Zookeeper + PostgreSQL + Kafka UI

---

## Getting Started

### Prerequisites
- Python 3.11+
- Docker & Docker Compose

### Quick start

```bash
git clone https://github.com/your-username/financial-etl-pipeline.git
cd financial-etl-pipeline

# Start infrastructure
docker-compose up -d

# API docs: http://localhost:8000/docs
# Kafka UI:  http://localhost:8081
```

### Local development

```bash
pip install -r requirements.txt
cp .env.example .env

docker-compose up -d postgres zookeeper kafka kafka-ui
make migrate
make dev
```

### Run tests (no infrastructure needed)

```bash
make test        # all tests with coverage
make test-unit   # unit tests only (fast)
```

### Produce sample events to Kafka

```bash
python scripts/produce_sample_events.py --count 200 --interval 0.05
```

### Process a CSV file

```bash
cp data/sample_market_data.csv data/input/
# The scheduler picks it up within 5 minutes,
# or trigger manually:
curl -X POST http://localhost:8000/api/v1/pipeline/trigger-files
```

---

## API Reference

```
GET /api/v1/pipeline/runs               → list pipeline run history
GET /api/v1/pipeline/runs/{run_id}      → run detail + validation errors
GET /api/v1/pipeline/metrics            → overall health metrics
GET /api/v1/pipeline/daily-summaries   → OHLCV data (filterable by symbol/date)
GET /health                             → liveness check
GET /docs                               → Swagger UI
```

### Example response — pipeline metrics

```json
{
  "total_records_processed": 15420,
  "total_outliers_detected": 23,
  "outlier_rate_pct": 0.15,
  "total_validation_errors": 47,
  "total_pipeline_runs": 12,
  "top_symbols": [
    {"symbol": "AAPL", "count": 3200},
    {"symbol": "GOOG", "count": 2800}
  ]
}
```

### Example response — daily summary

```json
{
  "symbol": "AAPL",
  "market": "NASDAQ",
  "date": "2025-06-01",
  "open_eur": 171.77,
  "high_eur": 172.48,
  "low_eur": 171.40,
  "close_eur": 172.00,
  "volume": 3330000,
  "vwap_eur": 171.92,
  "trade_count": 3,
  "outlier_count": 0
}
```

---

## Data Schema

### Input event (Kafka / CSV)

```json
{
  "event_id": "uuid",
  "symbol": "AAPL",
  "price": 185.50,
  "volume": 1250000,
  "currency": "USD",
  "market": "NASDAQ",
  "timestamp": "2025-06-01T14:30:00Z",
  "source": "reuters"
}
```

### Database tables

| Table | Description |
|---|---|
| `market_records` | One row per normalized event (EUR-converted, outlier-flagged) |
| `daily_summaries` | OHLCV aggregates per (symbol, market, date) |
| `pipeline_runs` | Execution metadata (status, counts, duration) |
| `validation_errors` | Rejected records with error details |

---

## Design Decisions

**Why Pandas for cleaning/normalization?**
Vectorized operations on NumPy arrays are orders of magnitude faster than Python loops for batch processing. A batch of 1,000 records takes ~2ms with Pandas vs ~50ms row-by-row. For a pipeline processing millions of events per day, this matters.

**Why Pydantic for per-record validation?**
Pandas is great for column-level operations but poor at per-row business rule validation. Pydantic v2 (Rust-backed) is fast enough to validate 10,000 records/second and produces structured, actionable error messages.

**Why `INSERT ... ON CONFLICT DO NOTHING`?**
Kafka's at-least-once delivery means the same event may be received multiple times (after consumer restarts). Idempotent upserts make the load stage safe to replay without creating duplicate records.

**Why APScheduler over Celery?**
For a single-service pipeline without distributed task requirements, APScheduler is much simpler — no Redis/RabbitMQ broker needed. Celery would be appropriate if tasks needed to be distributed across workers.

---

## License

MIT
