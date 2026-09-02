"""Prometheus metrics for the event pipeline.

Exposes three metric families:
  - produced_total    — Counter, labelled by event_type
  - consumed_total    — Counter, labelled by event_type and status (success/failure)
  - processing_seconds — Histogram, labelled by event_type
  - dlq_total         — Counter, labelled by event_type and reason
"""

from prometheus_client import Counter, Histogram, CollectorRegistry


class PipelineMetrics:
    """Prometheus metrics collector for the event pipeline.

    Creates a fresh CollectorRegistry per instance so tests can instantiate
    multiple metrics objects without name collisions.

    Args:
        namespace: Metric name prefix (e.g. 'event_pipeline').
    """

    def __init__(self, namespace: str = "event_pipeline") -> None:
        self._registry = CollectorRegistry()

        self._produced = Counter(
            f"{namespace}_produced_total",
            "Total events produced to Kafka",
            labelnames=["event_type"],
            registry=self._registry,
        )
        self._consumed = Counter(
            f"{namespace}_consumed_total",
            "Total events consumed from Kafka",
            labelnames=["event_type", "status"],
            registry=self._registry,
        )
        self._latency = Histogram(
            f"{namespace}_processing_seconds",
            "Event processing latency in seconds",
            labelnames=["event_type"],
            buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
            registry=self._registry,
        )
        self._dlq = Counter(
            f"{namespace}_dlq_total",
            "Total events routed to the dead-letter queue",
            labelnames=["event_type", "reason"],
            registry=self._registry,
        )

    def record_produced(self, event_type: str) -> None:
        """Increment the produced counter for an event type.

        Args:
            event_type: The event_type discriminator string.
        """
        self._produced.labels(event_type=event_type).inc()

    def record_consumed(
        self,
        event_type: str,
        success: bool,
        latency_seconds: float,
    ) -> None:
        """Record a consumed event with its processing outcome.

        Args:
            event_type: The event_type discriminator string.
            success: True if processing succeeded.
            latency_seconds: Wall-clock processing time in seconds.

        Raises:
            ValueError: If latency_seconds is negative.
        """
        if latency_seconds < 0:
            raise ValueError("latency_seconds must be >= 0")

        status = "success" if success else "failure"
        self._consumed.labels(event_type=event_type, status=status).inc()
        self._latency.labels(event_type=event_type).observe(latency_seconds)

    def record_dlq(self, event_type: str, reason: str) -> None:
        """Increment the DLQ counter.

        Args:
            event_type: The event_type discriminator string.
            reason: Short description of why the event was DLQ'd.
        """
        self._dlq.labels(event_type=event_type, reason=reason).inc()
