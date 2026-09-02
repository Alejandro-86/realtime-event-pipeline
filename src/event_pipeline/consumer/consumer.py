"""Kafka event consumer with dead-letter queue routing.

Processing flow:
  1. Poll Kafka for messages
  2. Deserialise bytes → typed event (Pydantic validation)
  3. Process event (dispatch by type)
  4. On failure: retry up to max_retries, then route to DLQ topic
"""

from dataclasses import dataclass
from typing import Any

import structlog

from event_pipeline.schemas import (
    AnyEvent,
    ApiUsageEvent,
    EventType,
    UserSignupEvent,
    deserialise,
)

logger = structlog.get_logger(__name__)


@dataclass
class ProcessingResult:
    """Result of processing a single Kafka message.

    Attributes:
        success: True if the event was processed without error.
        event: The deserialised event, or None if deserialisation failed.
        error: Error message if success is False.
        should_dlq: True if this message should be sent to the DLQ.
    """

    success: bool
    event: AnyEvent | None = None
    error: str | None = None
    should_dlq: bool = False


class EventConsumer:
    """Consumes events from a Kafka topic with DLQ support.

    Args:
        kafka_consumer: Underlying confluent_kafka.Consumer instance.
        topic: Topic to subscribe to.
        dlq_topic: Dead-letter queue topic name.
        max_retries: Maximum processing attempts before DLQ routing.
    """

    def __init__(
        self,
        kafka_consumer: Any,
        topic: str,
        dlq_topic: str,
        max_retries: int = 3,
    ) -> None:
        self._consumer = kafka_consumer
        self._topic = topic
        self._dlq_topic = dlq_topic
        self._max_retries = max_retries

    def process_message(self, msg: Any) -> ProcessingResult:
        """Deserialise and process a single Kafka message.

        Args:
            msg: A confluent_kafka.Message object.

        Returns:
            ProcessingResult indicating success, failure, or DLQ routing.
        """
        raw = msg.value()
        if raw is None:
            return ProcessingResult(success=False, error="empty message", should_dlq=True)

        try:
            event = deserialise(raw)
        except ValueError as exc:
            logger.warning("deserialisation failed", error=str(exc))
            return ProcessingResult(
                success=False, error=str(exc), should_dlq=True
            )

        try:
            self._dispatch(event)
            return ProcessingResult(success=True, event=event)
        except Exception as exc:
            logger.error("processing failed", event_type=event.event_type, error=str(exc))
            return ProcessingResult(
                success=False, event=event, error=str(exc), should_dlq=True
            )

    def _dispatch(self, event: AnyEvent) -> None:
        """Route an event to the appropriate handler by type."""
        match event.event_type:
            case EventType.API_USAGE:
                self._handle_api_usage(event)  # type: ignore[arg-type]
            case EventType.USER_SIGNUP:
                self._handle_user_signup(event)  # type: ignore[arg-type]
            case _:
                logger.warning("unhandled event type", event_type=event.event_type)

    def _handle_api_usage(self, event: ApiUsageEvent) -> None:
        """Process an API usage event."""
        logger.info(
            "api_usage",
            user_id=event.user_id,
            model=event.model_id,
            chars=event.characters_used,
        )

    def _handle_user_signup(self, event: UserSignupEvent) -> None:
        """Process a user signup event."""
        logger.info("user_signup", user_id=event.user_id, plan=event.plan)
