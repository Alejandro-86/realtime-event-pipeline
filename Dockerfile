FROM python:3.11-slim AS base
WORKDIR /app
COPY pyproject.toml .
RUN pip install --no-cache-dir -e .
COPY src/ src/

FROM base AS producer
CMD ["python", "-m", "event_pipeline.producer.run"]

FROM base AS consumer
CMD ["python", "-m", "event_pipeline.consumer.run"]
