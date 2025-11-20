# Kafka Order System (Assignment)

See assignment spec: order messages must be Avro serialized and system must support running average, retry logic, DLQ. :contentReference[oaicite:1]{index=1}

## Components
- Kafka + Zookeeper + Schema Registry (docker-compose)
- Avro producer (python) -> topic `orders`
- Avro consumer (python) -> computes running average; retries; sends to `dlq-orders`

## Run
1. docker-compose up -d (see docker/)
2. Run consumer
3. Run producer

## Files
- producer/order.avsc — Avro schema
- producer/producer.py — producer
- consumer/consumer.py — consumer + DLQ
