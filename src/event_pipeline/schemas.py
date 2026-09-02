"""Pydantic event schemas for the streaming pipeline.

All events are serialised to JSON bytes before being produced to Kafka
and deserialised + validated after consuming.  Invalid payloads that
cannot be parsed are routed to the dead-letter queue.
"""

import json
import uuid
from datetime import datetime, timezone
from enum import StrEnum
from typing import Annotated, Union

from pydantic import BaseModel, EmailStr, field_validator, Field


class EventType(StrEnum):
    """Discriminator field for all event types."""

    API_USAGE   = "api_usage"
    USER_SIGNUP = "user_signup"
    DLQ         = "dlq"


class BaseEvent(BaseModel):
    """Common fields shared by all events.

    Args:
        event_id: Auto-generated UUID4 — unique event identifier.
        event_type: Discriminator field for deserialisation routing.
        timestamp: UTC timestamp set at event creation.
    """

    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: EventType
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )


class ApiUsageEvent(BaseEvent):
    """Records a single API usage call.

    Args:
        user_id: Identifier of the user who made the call.
        model_id: Model used for the request.
        characters_used: Number of characters consumed (non-negative).
        latency_ms: Request latency in milliseconds.
    """

    event_type: EventType = EventType.API_USAGE
    user_id: str
    model_id: str
    characters_used: int
    latency_ms: float = 0.0

    @field_validator("characters_used")
    @classmethod
    def non_negative(cls, v: int) -> int:
        """Character count cannot be negative."""
        if v < 0:
            raise ValueError("characters_used must be >= 0")
        return v


class UserSignupEvent(BaseEvent):
    """Records a new user registration.

    Args:
        user_id: New user's identifier.
        email: User's email address (validated format).
        plan: Subscription plan selected at signup.
    """

    event_type: EventType = EventType.USER_SIGNUP
    user_id: str
    email: EmailStr
    plan: str


class DLQEvent(BaseEvent):
    """Dead-letter queue envelope for failed events.

    Wraps the original raw payload alongside error context so failed
    events can be inspected and replayed.

    Args:
        original_topic: Topic the event was consumed from.
        original_payload: Raw bytes of the original message.
        error: Human-readable error description.
        retry_count: Number of processing attempts before DLQ routing.
    """

    event_type: EventType = EventType.DLQ
    original_topic: str
    original_payload: bytes
    error: str
    retry_count: int

    @field_validator("retry_count")
    @classmethod
    def non_negative(cls, v: int) -> int:
        """Retry count cannot be negative."""
        if v < 0:
            raise ValueError("retry_count must be >= 0")
        return v


AnyEvent = Union[ApiUsageEvent, UserSignupEvent, DLQEvent]

_TYPE_MAP: dict[str, type[AnyEvent]] = {
    EventType.API_USAGE:   ApiUsageEvent,
    EventType.USER_SIGNUP: UserSignupEvent,
    EventType.DLQ:         DLQEvent,
}


def serialise(event: AnyEvent) -> bytes:
    """Serialise an event to JSON bytes for Kafka production.

    Args:
        event: Any typed event object.

    Returns:
        UTF-8 encoded JSON bytes.
    """
    return event.model_dump_json().encode("utf-8")


def deserialise(data: bytes) -> AnyEvent:
    """Deserialise Kafka message bytes into a typed event.

    Uses the ``event_type`` discriminator to route to the correct schema.

    Args:
        data: Raw Kafka message value bytes.

    Returns:
        A typed event object.

    Raises:
        ValueError: If the bytes are not valid JSON or the event_type is unknown.
    """
    try:
        raw = json.loads(data.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError(f"failed to parse event bytes: {exc}") from exc

    event_type = raw.get("event_type")
    cls = _TYPE_MAP.get(event_type)
    if cls is None:
        raise ValueError(f"unknown event_type '{event_type}'")

    return cls(**raw)
