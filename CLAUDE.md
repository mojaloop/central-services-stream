# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

`@mojaloop/central-services-stream` is a Kafka streaming library for Mojaloop services. It wraps `node-rdkafka` to provide Consumer and Producer classes with support for multiple consumption modes, OpenTelemetry tracing, and LIME protocol-based message formatting.

## Commands

```bash
# Run all tests (includes lint as pretest)
npm test

# Run a single test file
npx tapes 'test/unit/kafka/consumer.test.js' | tap-spec

# Lint
npm run lint
npm run lint:fix

# Coverage
npm run test:coverage
npm run test:coverage-check

# Dependency management
npm run dep:check      # Check for outdated dependencies
npm run audit:check    # Security audit
```

## Architecture

### Entry Point
`src/index.js` exports two modules:
- `Kafka` - Consumer, Producer, Protocol, and otel utilities
- `Util` - Utility wrappers for Consumer/Producer

### Consumer Modes (`src/kafka/consumer.js`)
The Consumer supports three processing modes via `CONSUMER_MODES`:
- **flow** (0) - One message at a time, as fast as possible
- **poll** (1) - Batch consumption at configurable poll frequency
- **recursive** (2) - Batch consumption with recursive calls (default)

Key configuration options:
- `sync: true` - Process messages in order via async queue
- `batchSize` - Messages per batch (poll/recursive modes)
- `messageAsJSON: true` - Auto-parse JSON messages
- `deserializeFn` - Custom deserialization function

### Producer (`src/kafka/producer.js`)
- Wraps node-rdkafka Producer/HighLevelProducer
- `options.sync: true` uses HighLevelProducer with delivery callbacks
- Built-in OpenTelemetry span creation for tracing
- Supports lag monitoring via `lagMonitor` config

### Protocol (`src/kafka/protocol/`)
Message format based on LIME protocol (limeprotocol.org). Messages include:
- `from`/`to` - Sender/receiver identifiers
- `id` - Message identifier
- `type` - MIME content type
- `content` - Message payload
- `metadata` - Context data

### OpenTelemetry Integration (`src/kafka/otel.js`)
- `startConsumerTracingSpan()` - Creates consumer spans from message headers
- Automatic span injection for producers
- Extracts `traceparent`, `tracestate`, `baggage` headers

### Health Checking (`src/kafka/shared.js`)
- `trackConnectionHealth()` - Monitors Kafka broker connection states
- Consumer provides `isHealthy()`, `isPollHealthy()`, `isEventStatsConnectionHealthy()`

## Peer Dependencies

Requires these Mojaloop packages as peer dependencies:
- `@mojaloop/central-services-error-handling`
- `@mojaloop/central-services-logger`

## Testing

Tests use `tapes` (tape wrapper) with Sinon for mocking. Test files are in `test/unit/` mirroring the src structure.

Mock Kafka clients via `test/unit/kafka/KafkaStub.js`.

## Librdkafka

The native `node-rdkafka` dependency builds librdkafka by default. To use a system-installed version:
```bash
export BUILD_LIBRDKAFKA=0
npm install
```
