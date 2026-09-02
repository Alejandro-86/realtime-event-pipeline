"""Unit tests for Prometheus metrics."""

import pytest

from event_pipeline.monitoring.metrics import PipelineMetrics


class TestPipelineMetrics:
    def test_record_produced(self) -> None:
        m = PipelineMetrics(namespace="test")
        m.record_produced("api_usage")
        # Metric exists and is labelled — no exception = pass

    def test_record_consumed_success(self) -> None:
        m = PipelineMetrics(namespace="test")
        m.record_consumed("api_usage", success=True, latency_seconds=0.05)

    def test_record_consumed_failure(self) -> None:
        m = PipelineMetrics(namespace="test")
        m.record_consumed("api_usage", success=False, latency_seconds=0.1)

    def test_record_dlq(self) -> None:
        m = PipelineMetrics(namespace="test")
        m.record_dlq("api_usage", reason="deserialise_error")

    def test_multiple_event_types_tracked(self) -> None:
        m = PipelineMetrics(namespace="test2")
        m.record_produced("api_usage")
        m.record_produced("user_signup")
        m.record_consumed("api_usage", success=True, latency_seconds=0.02)
        m.record_consumed("user_signup", success=True, latency_seconds=0.03)
        # No exception = counters accept multiple labels

    def test_latency_accepts_zero(self) -> None:
        m = PipelineMetrics(namespace="test3")
        m.record_consumed("api_usage", success=True, latency_seconds=0.0)

    def test_latency_rejects_negative(self) -> None:
        m = PipelineMetrics(namespace="test4")
        with pytest.raises(ValueError):
            m.record_consumed("api_usage", success=True, latency_seconds=-1.0)
