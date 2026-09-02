# realtime-event-pipeline

Production-grade Kafka event streaming pipeline with typed schemas,
dead-letter queue handling, structured logging, and Prometheus monitoring.

## Architecture

```
┌──────────────┐    events     ┌──────────────┐   transformed   ┌──────────────┐
│   Producer   │──────────────►│    Kafka     │────────────────►│   Consumer   │
│              │               │              │                  │              │
│ Publishes    │               │  topics:     │                  │ Processes    │
│ typed events │               │  • events    │                  │ events with  │
│ (Pydantic)   │               │  • events.dlq│                  │ retry + DLQ  │
└──────────────┘               └──────────────┘                  └──────────────┘
                                                                        │
                                                               ┌────────▼────────┐
                                                               │   Prometheus    │
                                                               │   /metrics      │
                                                               │ consumed_total  │
                                                               │ dlq_total       │
                                                               │ latency_seconds │
                                                               └─────────────────┘
```

## Topics

| Topic | Purpose |
|---|---|
| `events` | Main event stream |
| `events.dlq` | Dead-letter queue — failed events with error metadata |

## Event types

All events are validated as Pydantic models before producing and after consuming.

## Quickstart

```bash
make install
make up       # start Kafka + Prometheus
make produce  # start producer (generates sample events)
make consume  # start consumer
```

Prometheus metrics: http://localhost:9090
