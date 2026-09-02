"""Consumer entry point — polls Kafka and processes events with DLQ routing."""

import logging
import signal
import sys

from confluent_kafka import Consumer, Producer

from event_pipeline.config import settings
from event_pipeline.consumer.consumer import EventConsumer
from event_pipeline.schemas import DLQEvent, serialise

logging.basicConfig(level=logging.INFO)

_running = True


def _handle_signal(signum: int, frame: object) -> None:
    global _running
    _running = False


def main() -> None:
    """Start consuming events with DLQ routing."""
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    kafka_consumer = Consumer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": settings.kafka_consumer_group,
        "auto.offset.reset": settings.kafka_auto_offset_reset,
        "enable.auto.commit": True,
    })
    dlq_producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})

    consumer = EventConsumer(
        kafka_consumer=kafka_consumer,
        topic=settings.kafka_topic,
        dlq_topic=settings.kafka_dlq_topic,
        max_retries=settings.max_retries,
    )

    kafka_consumer.subscribe([settings.kafka_topic])

    try:
        while _running:
            msg = kafka_consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logging.error("kafka error: %s", msg.error())
                continue

            result = consumer.process_message(msg)

            if result.should_dlq:
                dlq_event = DLQEvent(
                    original_topic=settings.kafka_topic,
                    original_payload=msg.value() or b"",
                    error=result.error or "unknown",
                    retry_count=0,
                )
                dlq_producer.produce(
                    topic=settings.kafka_dlq_topic,
                    value=serialise(dlq_event),
                )
                dlq_producer.poll(0)
    finally:
        kafka_consumer.close()
        dlq_producer.flush()


if __name__ == "__main__":
    main()
