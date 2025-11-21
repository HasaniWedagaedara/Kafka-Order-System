"""
=============================================================================
KAFKA ORDER CONSUMER WITH RETRY LOGIC & DEAD LETTER QUEUE
=============================================================================
Assignment: Kafka-based Order Processing System
Features:
  - Avro deserialization with Schema Registry
  - Real-time price aggregation (running average)
  - Retry logic for temporary failures (3 attempts)
  - Dead Letter Queue (DLQ) for permanent failures
=============================================================================
"""

import time
import random
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

# =============================================================================
# CONFIGURATION
# =============================================================================
KAFKA_BROKER = 'localhost:9092'
SCHEMA_REGISTRY_URL = 'http://localhost:8081'
TOPIC_NAME = 'orders'
DLQ_TOPIC = 'orders-dlq'
CONSUMER_GROUP = 'order-consumer-group'

# Retry settings
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds

# Simulated failure rate (for demo purposes - set to 0.2 for 20% failures)
FAILURE_RATE = 0.2

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================
def load_avro_schema():
    """Load Avro schema from file"""
    with open('schemas/order.avsc', 'r') as f:
        return f.read()

def print_banner():
    """Print startup banner"""
    print("\n" + "=" * 70)
    print(" KAFKA ORDER CONSUMER - RETRY & DLQ DEMO")
    print("=" * 70)
    print(f" Kafka Broker:    {KAFKA_BROKER}")
    print(f" Schema Registry: {SCHEMA_REGISTRY_URL}")
    print(f" Topic:           {TOPIC_NAME}")
    print(f" DLQ Topic:       {DLQ_TOPIC}")
    print(f" Consumer Group:  {CONSUMER_GROUP}")
    print(f" Max Retries:     {MAX_RETRIES}")
    print(f" Failure Rate:    {int(FAILURE_RATE * 100)}% (simulated for demo)")
    print("=" * 70 + "\n")

def print_stats(total_orders, total_price, running_average):
    """Print aggregation statistics"""
    print("\n    +------------------------------------------+")
    print("    |         REAL-TIME AGGREGATION            |")
    print("    +------------------------------------------+")
    print(f"    |  Total Orders:      {total_orders:<20}|")
    print(f"    |  Total Revenue:     ${total_price:<18.2f}|")
    print(f"    |  Running Average:   ${running_average:<18.2f}|")
    print("    +------------------------------------------+")

# =============================================================================
# ORDER PROCESSOR CLASS
# =============================================================================
class OrderProcessor:
    def __init__(self):
        # Aggregation state
        self.total_orders = 0
        self.total_price = 0.0
        self.running_average = 0.0
        self.failed_count = 0
        self.retry_count = 0
        
        # Load schema
        print("[1/4] Loading Avro schema...")
        schema_str = load_avro_schema()
        print("      Schema loaded!\n")
        
        # Setup Schema Registry
        print("[2/4] Connecting to Schema Registry...")
        self.schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
        self.avro_deserializer = AvroDeserializer(
            self.schema_registry_client,
            schema_str
        )
        print("      Schema Registry connected!\n")
        
        # Setup Consumer
        print("[3/4] Initializing Kafka Consumer...")
        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_BROKER,
            'group.id': CONSUMER_GROUP,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        })
        self.consumer.subscribe([TOPIC_NAME])
        print("      Consumer ready!\n")
        
        # Setup DLQ Producer
        print("[4/4] Initializing DLQ Producer...")
        self.dlq_producer = Producer({
            'bootstrap.servers': KAFKA_BROKER,
            'client.id': 'dlq-producer'
        })
        print("      DLQ Producer ready!\n")
    
    def process_order(self, order):
        """
        Process order with simulated failures for demo.
        In production, this would contain actual business logic.
        """
        # Simulate random failures for demo
        if random.random() < FAILURE_RATE:
            raise Exception("Simulated processing failure (database timeout)")
        
        # Update aggregation metrics
        self.total_orders += 1
        self.total_price += order['price']
        self.running_average = self.total_price / self.total_orders
        
        # Display success
        print("\n    [SUCCESS] Order processed successfully!")
        print(f"    Order ID:  {order['orderId']}")
        print(f"    Product:   {order['product']}")
        print(f"    Price:     ${order['price']:.2f}")
        
        # Show aggregation stats
        print_stats(self.total_orders, self.total_price, self.running_average)
    
    def send_to_dlq(self, message, error_reason):
        """Send failed message to Dead Letter Queue with error metadata"""
        try:
            self.dlq_producer.produce(
                topic=DLQ_TOPIC,
                key=message.key(),
                value=message.value(),
                headers=[
                    ('error_reason', error_reason.encode('utf-8')),
                    ('original_topic', TOPIC_NAME.encode('utf-8')),
                    ('failed_timestamp', str(int(time.time())).encode('utf-8')),
                    ('retry_attempts', str(MAX_RETRIES).encode('utf-8'))
                ]
            )
            self.dlq_producer.flush()
            self.failed_count += 1
            print(f"\n    [DLQ] Message sent to Dead Letter Queue")
            print(f"    Reason: {error_reason}")
            print(f"    DLQ Topic: {DLQ_TOPIC}")
        except Exception as e:
            print(f"\n    [ERROR] Failed to send to DLQ: {e}")
    
    def process_with_retry(self, msg):
        """Process message with retry logic"""
        # Deserialize Avro message
        order = self.avro_deserializer(
            msg.value(),
            SerializationContext(TOPIC_NAME, MessageField.VALUE)
        )
        
        print("\n" + "-" * 70)
        print(f" PROCESSING ORDER: {order['orderId']}")
        print("-" * 70)
        
        # Retry loop
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                print(f"\n    [ATTEMPT {attempt}/{MAX_RETRIES}] Processing...")
                self.process_order(order)
                
                # Success - commit offset
                self.consumer.commit(message=msg)
                print("\n    [COMMIT] Offset committed")
                return True
                
            except Exception as e:
                self.retry_count += 1
                print(f"    [FAILED] {str(e)}")
                
                if attempt < MAX_RETRIES:
                    print(f"    [RETRY] Waiting {RETRY_DELAY}s before retry...")
                    time.sleep(RETRY_DELAY)
                else:
                    print(f"\n    [EXHAUSTED] All {MAX_RETRIES} retries failed!")
                    self.send_to_dlq(msg, str(e))
                    self.consumer.commit(message=msg)
                    return False
    
    def run(self):
        """Main consumer loop"""
        print("=" * 70)
        print(" CONSUMING ORDERS (Press Ctrl+C to stop)")
        print("=" * 70)
        
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        raise KafkaException(msg.error())
                
                self.process_with_retry(msg)
                
        except KeyboardInterrupt:
            self.shutdown()
    
    def shutdown(self):
        """Graceful shutdown with final statistics"""
        print("\n\n" + "=" * 70)
        print(" SHUTTING DOWN CONSUMER")
        print("=" * 70)
        
        self.consumer.close()
        self.dlq_producer.flush()
        
        print("\n" + "=" * 70)
        print(" FINAL STATISTICS")
        print("=" * 70)
        print(f" Orders Processed:    {self.total_orders}")
        print(f" Total Revenue:       ${self.total_price:.2f}")
        print(f" Average Order Price: ${self.running_average:.2f}")
        print(f" Messages to DLQ:     {self.failed_count}")
        print(f" Total Retry Count:   {self.retry_count}")
        print("=" * 70)
        print(" Consumer shutdown complete!")
        print("=" * 70 + "\n")

# =============================================================================
# MAIN ENTRY POINT
# =============================================================================
if __name__ == '__main__':
    print_banner()
    processor = OrderProcessor()
    processor.run()