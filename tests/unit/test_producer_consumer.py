"""Unit tests for producer and consumer using in-memory stubs."""

import pytest
from event_pipeline.schemas import (
    ApiUsageEvent,
    UserSignupEvent,
    DLQEvent,
    EventType,
    serialise,
)
from event_pipeline.producer.producer import EventProducer
from event_pipeline.consumer.consumer import EventConsumer, ProcessingResult


# ─── Stub Kafka producer ──────────────────────────────────────────────────────

class StubKafkaProducer:
    """Records all produce calls without real Kafka."""

    def __init__(self) -> None:
        self.produced: list[tuple[str, bytes]] = []
        self.flushed = 0

    def produce(self, topic: str, value: bytes, on_delivery: object = None) -> None:
        self.produced.append((topic, value))

    def flush(self, timeout: float = 10.0) -> int:
        self.flushed += 1
        return 0

    def poll(self, timeout: float = 0) -> int:
        return 0


class TestEventProducer:
    def test_produce_sends_to_correct_topic(self) -> None:
        stub = StubKafkaProducer()
        producer = EventProducer(kafka_producer=stub, topic="events")
        event = ApiUsageEvent(user_id="u", model_id="m", characters_used=100)
        producer.produce(event)
        assert len(stub.produced) == 1
        assert stub.produced[0][0] == "events"

    def test_produce_serialises_event(self) -> None:
        stub = StubKafkaProducer()
        producer = EventProducer(kafka_producer=stub, topic="events")
        event = ApiUsageEvent(user_id="u", model_id="m", characters_used=500)
        producer.produce(event)
        raw = stub.produced[0][1]
        import json
        parsed = json.loads(raw)
        assert parsed["characters_used"] == 500

    def test_flush_called_on_close(self) -> None:
        stub = StubKafkaProducer()
        producer = EventProducer(kafka_producer=stub, topic="events")
        producer.close()
        assert stub.flushed >= 1

    def test_produce_multiple_events(self) -> None:
        stub = StubKafkaProducer()
        producer = EventProducer(kafka_producer=stub, topic="events")
        for i in range(5):
            producer.produce(ApiUsageEvent(user_id="u", model_id="m", characters_used=i))
        assert len(stub.produced) == 5


# ─── Stub message and consumer ───────────────────────────────────────────────

class StubMessage:
    """Mimics confluent_kafka.Message for testing."""

    def __init__(self, value: bytes, error: object = None) -> None:
        self._value = value
        self._error = error

    def value(self) -> bytes:
        return self._value

    def error(self) -> object:
        return self._error

    def topic(self) -> str:
        return "events"

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return 0


class TestEventConsumer:
    def test_process_valid_event_succeeds(self) -> None:
        event = ApiUsageEvent(user_id="u", model_id="m", characters_used=200)
        msg = StubMessage(serialise(event))
        consumer = EventConsumer(kafka_consumer=None, topic="events", dlq_topic="events.dlq")  # type: ignore[arg-type]
        result = consumer.process_message(msg)
        assert result.success is True
        assert result.event is not None
        assert result.event.event_type == EventType.API_USAGE

    def test_process_invalid_bytes_routes_to_dlq(self) -> None:
        msg = StubMessage(b"not-valid-json")
        consumer = EventConsumer(kafka_consumer=None, topic="events", dlq_topic="events.dlq")  # type: ignore[arg-type]
        result = consumer.process_message(msg)
        assert result.success is False
        assert result.should_dlq is True

    def test_process_user_signup_event(self) -> None:
        event = UserSignupEvent(user_id="u", email="a@b.com", plan="free")
        msg = StubMessage(serialise(event))
        consumer = EventConsumer(kafka_consumer=None, topic="events", dlq_topic="events.dlq")  # type: ignore[arg-type]
        result = consumer.process_message(msg)
        assert result.success is True
        assert result.event.event_type == EventType.USER_SIGNUP  # type: ignore[union-attr]

    def test_processing_result_stores_error(self) -> None:
        result = ProcessingResult(success=False, error="test error", should_dlq=True)
        assert result.error == "test error"
        assert result.should_dlq is True
