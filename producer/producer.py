"""
=============================================================================
KAFKA ORDER PRODUCER WITH AVRO SERIALIZATION
=============================================================================
Assignment: Kafka-based Order Processing System
Features:
  - Avro serialization with Schema Registry
  - Real-time order generation
  - Delivery confirmation callbacks
=============================================================================
"""

import time
import random
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# =============================================================================
# CONFIGURATION
# =============================================================================
KAFKA_BROKER = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC_NAME = "orders"

# Sample products for demo
PRODUCTS = [
    "Laptop",
    "Mouse",
    "Keyboard",
    "Monitor",
    "Headphones",
    "Webcam",
    "USB Cable",
    "SSD",
    "RAM",
    "Graphics Card",
]


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================
def load_avro_schema():
    """Load Avro schema from file"""
    with open("schemas/order.avsc", "r") as f:
        return f.read()


def delivery_report(err, msg):
    """Callback for message delivery confirmation"""
    if err is not None:
        print(f"    [DELIVERY FAILED] {err}")
    else:
        print(
            f"    [DELIVERED] Topic: {msg.topic()} | Partition: {msg.partition()} | Offset: {msg.offset()}"
        )


def order_to_dict(order, ctx):
    """Convert order to dictionary for Avro serialization"""
    return order


def print_banner():
    """Print startup banner"""
    print("\n" + "=" * 70)
    print(" KAFKA ORDER PRODUCER - AVRO SERIALIZATION DEMO")
    print("=" * 70)
    print(f" Kafka Broker:    {KAFKA_BROKER}")
    print(f" Schema Registry: {SCHEMA_REGISTRY_URL}")
    print(f" Topic:           {TOPIC_NAME}")
    print("=" * 70 + "\n")


# =============================================================================
# MAIN PRODUCER
# =============================================================================
def main():
    print_banner()

    # Load schema
    print("[1/3] Loading Avro schema...")
    schema_str = load_avro_schema()
    print("      Schema loaded successfully!\n")

    # Setup Schema Registry
    print("[2/3] Connecting to Schema Registry...")
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(schema_registry_client, schema_str, order_to_dict)
    print("      Schema Registry connected!\n")

    # Setup Producer
    print("[3/3] Initializing Kafka Producer...")
    producer = Producer(
        {"bootstrap.servers": KAFKA_BROKER, "client.id": "order-producer"}
    )
    print("      Producer ready!\n")

    print("=" * 70)
    print(" PRODUCING ORDERS (Press Ctrl+C to stop)")
    print("=" * 70)

    order_count = 0

    try:
        while True:
            order_count += 1

            # Generate random order
            order = {
                "orderId": f"ORD-{order_count:05d}",
                "product": random.choice(PRODUCTS),
                "price": round(random.uniform(10.0, 1000.0), 2),
                "timestamp": int(time.time() * 1000),
            }

            # Display order details
            print(f"\n[ORDER #{order_count}]")
            print(f"    Order ID:  {order['orderId']}")
            print(f"    Product:   {order['product']}")
            print(f"    Price:     ${order['price']:.2f}")

            # Serialize with Avro and produce
            serialized_order = avro_serializer(
                order, SerializationContext(TOPIC_NAME, MessageField.VALUE)
            )

            producer.produce(
                topic=TOPIC_NAME,
                value=serialized_order,
                key=order["orderId"].encode("utf-8"),
                on_delivery=delivery_report,
            )

            # Trigger delivery callbacks
            producer.poll(0)

            # Wait before next order (adjust for demo speed)
            time.sleep(2)

    except KeyboardInterrupt:
        print("\n\n" + "=" * 70)
        print(" SHUTTING DOWN PRODUCER")
        print("=" * 70)

    finally:
        print("\nFlushing pending messages...")
        producer.flush()
        print(f"Total orders produced: {order_count}")
        print("Producer shutdown complete!")
        print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
