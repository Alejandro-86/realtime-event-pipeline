"""Producer entry point — generates sample events for demonstration."""

import time
import random
import logging

from confluent_kafka import Producer

from event_pipeline.config import settings
from event_pipeline.producer.producer import EventProducer
from event_pipeline.schemas import ApiUsageEvent, UserSignupEvent

logging.basicConfig(level=logging.INFO)

MODELS = ["eleven_v3", "eleven_flash_v2_5", "eleven_multilingual_v2"]
PLANS  = ["free", "starter", "creator", "enterprise"]


def main() -> None:
    """Produce sample API usage and user signup events indefinitely."""
    kafka_producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
    producer = EventProducer(kafka_producer=kafka_producer, topic=settings.kafka_topic)

    try:
        counter = 0
        while True:
            if counter % 10 == 0:
                event = UserSignupEvent(
                    user_id=f"user-{counter}",
                    email=f"user{counter}@example.com",
                    plan=random.choice(PLANS),
                )
            else:
                event = ApiUsageEvent(
                    user_id=f"user-{random.randint(0, 100)}",
                    model_id=random.choice(MODELS),
                    characters_used=random.randint(100, 5000),
                    latency_ms=random.uniform(50, 500),
                )
            producer.produce(event)
            counter += 1
            time.sleep(0.1)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
