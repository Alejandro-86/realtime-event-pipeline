"""Kafka event producer — serialises typed events and delivers to a topic."""

from typing import Any

import structlog

from event_pipeline.schemas import AnyEvent, serialise

logger = structlog.get_logger(__name__)


class EventProducer:
    """Produces typed events to a Kafka topic.

    Accepts any typed event, serialises it to JSON bytes, and delivers
    it to the configured topic.  Uses confluent-kafka's async delivery
    callbacks for error detection.

    Args:
        kafka_producer: Underlying confluent_kafka.Producer instance.
        topic: Target Kafka topic name.
    """

    def __init__(self, kafka_producer: Any, topic: str) -> None:
        self._producer = kafka_producer
        self._topic = topic

    def produce(self, event: AnyEvent) -> None:
        """Serialise and produce an event to Kafka.

        Args:
            event: Any typed event object.
        """
        payload = serialise(event)
        self._producer.produce(
            topic=self._topic,
            value=payload,
            on_delivery=self._on_delivery,
        )
        self._producer.poll(0)
        logger.info(
            "produced",
            event_type=event.event_type,
            event_id=event.event_id,
            topic=self._topic,
        )

    def close(self) -> None:
        """Flush all pending messages and close the producer."""
        self._producer.flush(timeout=30.0)
        logger.info("producer flushed and closed")

    @staticmethod
    def _on_delivery(err: Any, msg: Any) -> None:
        """Delivery report callback — logs errors."""
        if err:
            logger.error("delivery failed", error=str(err))
        else:
            logger.debug(
                "delivered",
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
            )
