# Kafka Load Tests

Load tests for `@message-queue-toolkit/kafka` measuring throughput, latency, and backlog under configurable load.

Two producer modes and two consumer modes can be combined:

- **CDC**: CockroachDB inserts → CDC changefeed → Kafka (realistic end-to-end)
- **Direct**: `AbstractKafkaPublisher` → Kafka (isolates Kafka consumer performance)
- **Single**: One message at a time (`batchProcessingEnabled: false`)
- **Batch**: Batched consumption via `KafkaMessageBatchStream` (`batchProcessingEnabled: true`)

## Prerequisites

- Docker & Docker Compose
- Node.js >= 22.14.0

## Quick Start

```bash
# Install dependencies
npm install

# Start infrastructure (Kafka + CockroachDB + CDC changefeed)
npm run docker:start

# Stop and clean up
npm run docker:stop
```

## Test Scripts

### CDC (CockroachDB → CDC → Kafka)

```bash
# Single-message consumer
npm run load:cdc:light          # 100 rows/sec, 30s
npm run load:cdc:medium         # 1000 rows/sec, 60s
npm run load:cdc:heavy          # 5000 rows/sec, 120s
npm run load:cdc -- --rate 500 --duration 45 --batch 50

# Batch consumer
npm run load:cdc:batch:light    # 100 rows/sec, 30s
npm run load:cdc:batch:medium   # 1000 rows/sec, 60s
npm run load:cdc:batch:heavy    # 5000 rows/sec, 120s
npm run load:cdc:batch -- --rate 500 --consumer-batch 100 --consumer-timeout 500
```

### Direct (Kafka publisher → Kafka)

```bash
# Single-message consumer
npm run load:direct:light       # 100 msgs/sec, 30s
npm run load:direct:medium      # 1000 msgs/sec, 60s
npm run load:direct:heavy       # 5000 msgs/sec, 120s
npm run load:direct -- --rate 500 --duration 45 --batch 50

# Batch consumer
npm run load:direct:batch:light   # 100 msgs/sec, 30s
npm run load:direct:batch:medium  # 1000 msgs/sec, 60s
npm run load:direct:batch:heavy   # 5000 msgs/sec, 120s
npm run load:direct:batch -- --rate 500 --consumer-batch 100 --consumer-timeout 500
```

## CLI Options

### Common

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--rate` | `-r` | 1000 | Target produce rate (rows or msgs per sec) |
| `--duration` | `-d` | 60 | Test duration (seconds) |
| `--batch` | `-b` | 100 | Producer batch size |

### Batch consumer only

| Flag | Default | Description |
|------|---------|-------------|
| `--consumer-batch` | 50 | Messages per consumer batch |
| `--consumer-timeout` | 200 | Batch flush timeout (ms) |

## Architecture

### CDC mode

```
Load Generator → CockroachDB (inserts) → CDC Changefeed → Kafka → Consumer → Metrics
```

1. **CockroachDB** tables (`events`, `orders`) with CDC changefeed targeting Kafka
2. **Load generator** inserts rows into CRDB at configurable rate with fire-and-forget concurrency
3. **CDC changefeed** publishes row changes to Kafka topics
4. **Consumer** (single or batch) processes messages and records metrics

### Direct mode

```
Load Generator → AbstractKafkaPublisher → Kafka → Consumer → Metrics
```

1. **Publisher** sends messages directly to Kafka topics (`direct-events`, `direct-orders`)
2. **Consumer** (single or batch) processes messages and records metrics

## Services

| Service | Port | Description |
|---------|------|-------------|
| Kafka | 9092 | Message broker (KRaft, 6 partitions) |
| CockroachDB | 26257 | SQL database |
| CockroachDB UI | 8181 | DB admin console |
| Kafka UI | 8080 | Topic browser |

## Latency Measurement

Each `events` row embeds `{"loadtest_ts": <epoch_ms>}` in its payload. The consumer extracts this timestamp and computes end-to-end latency (insert/publish → consume). Reported as avg, p50, p95, p99.
