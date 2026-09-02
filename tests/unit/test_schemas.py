"""Unit tests for event schema models."""

import pytest
from event_pipeline.schemas import (
    EventType,
    ApiUsageEvent,
    UserSignupEvent,
    DLQEvent,
    deserialise,
    serialise,
)


class TestApiUsageEvent:
    def test_stores_core_fields(self) -> None:
        e = ApiUsageEvent(
            user_id="user-123",
            model_id="eleven_v3",
            characters_used=1500,
        )
        assert e.event_type == EventType.API_USAGE
        assert e.characters_used == 1500

    def test_characters_cannot_be_negative(self) -> None:
        with pytest.raises(ValueError):
            ApiUsageEvent(user_id="u", model_id="m", characters_used=-1)

    def test_event_id_auto_generated(self) -> None:
        e = ApiUsageEvent(user_id="u", model_id="m", characters_used=100)
        assert e.event_id != ""
        assert len(e.event_id) == 36  # UUID4

    def test_timestamp_auto_set(self) -> None:
        e = ApiUsageEvent(user_id="u", model_id="m", characters_used=100)
        assert e.timestamp is not None


class TestUserSignupEvent:
    def test_stores_email_and_plan(self) -> None:
        e = UserSignupEvent(user_id="user-456", email="alex@example.com", plan="starter")
        assert e.event_type == EventType.USER_SIGNUP
        assert e.plan == "starter"

    def test_email_validated(self) -> None:
        with pytest.raises(ValueError):
            UserSignupEvent(user_id="u", email="not-an-email", plan="free")


class TestDLQEvent:
    def test_wraps_original_payload(self) -> None:
        original = ApiUsageEvent(user_id="u", model_id="m", characters_used=100)
        dlq = DLQEvent(
            original_topic="events",
            original_payload=serialise(original),
            error="processing failed",
            retry_count=3,
        )
        assert dlq.original_topic == "events"
        assert dlq.retry_count == 3

    def test_retry_count_non_negative(self) -> None:
        with pytest.raises(ValueError):
            DLQEvent(original_topic="t", original_payload=b"x",
                     error="e", retry_count=-1)


class TestSerialisation:
    def test_round_trip_api_usage(self) -> None:
        original = ApiUsageEvent(user_id="u", model_id="m", characters_used=500)
        raw = serialise(original)
        recovered = deserialise(raw)
        assert recovered.event_id == original.event_id
        assert recovered.characters_used == 500  # type: ignore[union-attr]

    def test_round_trip_user_signup(self) -> None:
        original = UserSignupEvent(user_id="u", email="a@b.com", plan="free")
        raw = serialise(original)
        recovered = deserialise(raw)
        assert recovered.event_type == EventType.USER_SIGNUP

    def test_deserialise_raises_on_invalid_json(self) -> None:
        with pytest.raises(ValueError):
            deserialise(b"not-json")
