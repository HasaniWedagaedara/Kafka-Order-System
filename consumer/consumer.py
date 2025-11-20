import time
import random
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

# Configuration
KAFKA_BROKER = 'localhost:9092'
SCHEMA_REGISTRY_URL = 'http://localhost:8081'
TOPIC_NAME = 'orders'
DLQ_TOPIC = 'orders-dlq'
CONSUMER_GROUP = 'order-consumer-group'

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds

# Load Avro schema
with open('schemas/order.avsc', 'r') as f:
    schema_str = f.read()

class OrderProcessor:
    def __init__(self):
        self.total_orders = 0
        self.total_price = 0.0
        self.running_average = 0.0
        
        # Setup Schema Registry
        schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
        self.schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        
        # Setup Avro deserializer
        self.avro_deserializer = AvroDeserializer(
            self.schema_registry_client,
            schema_str
        )
        
        # Setup Consumer
        consumer_conf = {
            'bootstrap.servers': KAFKA_BROKER,
            'group.id': CONSUMER_GROUP,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        }
        self.consumer = Consumer(consumer_conf)
        
        # Setup DLQ Producer
        producer_conf = {
            'bootstrap.servers': KAFKA_BROKER,
            'client.id': 'dlq-producer'
        }
        self.dlq_producer = Producer(producer_conf)
        
        self.consumer.subscribe([TOPIC_NAME])
    
    def process_order(self, order):
        """Process order with simulated failures for testing retry logic"""
        
        # Simulate random failures (20% chance)
        if random.random() < 0.2:
            raise Exception("Simulated temporary processing failure")
        
        # Update aggregation
        self.total_orders += 1
        self.total_price += order['price']
        self.running_average = self.total_price / self.total_orders
        
        print(f"\n✅ Successfully processed order:")
        print(f"   Order ID:         {order['orderId']}")
        print(f"   Product:          {order['product']}")
        print(f"   Price:            ${order['price']:.2f}")
        print(f"   📊 AGGREGATION STATS:")
        print(f"      Total Orders:    {self.total_orders}")
        print(f"      Total Revenue:   ${self.total_price:.2f}")
        print(f"      Running Average: ${self.running_average:.2f}")
    
    def send_to_dlq(self, message, error_reason):
        """Send failed message to Dead Letter Queue"""
        try:
            self.dlq_producer.produce(
                topic=DLQ_TOPIC,
                key=message.key(),
                value=message.value(),
                headers=[
                    ('error_reason', error_reason.encode('utf-8')),
                    ('original_topic', TOPIC_NAME.encode('utf-8')),
                    ('failed_timestamp', str(int(time.time())).encode('utf-8'))
                ]
            )
            self.dlq_producer.flush()
            print(f"💀 Message sent to DLQ: {error_reason}")
        except Exception as e:
            print(f"❌ Failed to send to DLQ: {e}")
    
    def process_message_with_retry(self, msg):
        """Process message with retry logic"""
        
        # Deserialize order
        order = self.avro_deserializer(
            msg.value(),
            SerializationContext(TOPIC_NAME, MessageField.VALUE)
        )
        
        # Retry loop
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                print(f"\n🔄 Processing attempt {attempt}/{MAX_RETRIES} for Order: {order['orderId']}")
                self.process_order(order)
                
                # Success - commit offset
                self.consumer.commit(message=msg)
                print(f"✔️  Offset committed successfully")
                return True
                
            except Exception as e:
                print(f"⚠️  Attempt {attempt} failed: {str(e)}")
                
                if attempt < MAX_RETRIES:
                    print(f"⏳ Retrying in {RETRY_DELAY} seconds...")
                    time.sleep(RETRY_DELAY)
                else:
                    # All retries exhausted - send to DLQ
                    print(f"❌ All {MAX_RETRIES} retries exhausted!")
                    self.send_to_dlq(msg, str(e))
                    
                    # Commit offset even for failed messages to avoid reprocessing
                    self.consumer.commit(message=msg)
                    return False
    
    def run(self):
        """Main consumer loop"""
        print("🚀 Starting Order Consumer...")
        print(f"📥 Consuming from topic: {TOPIC_NAME}")
        print(f"👥 Consumer group: {CONSUMER_GROUP}")
        print(f"🔄 Max retries: {MAX_RETRIES}")
        print(f"💀 DLQ topic: {DLQ_TOPIC}")
        print("-" * 70)
        
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
                
                # Process message with retry logic
                self.process_message_with_retry(msg)
                
        except KeyboardInterrupt:
            print("\n\n⏹️  Consumer stopped by user")
        finally:
            print("\n🔄 Closing consumer...")
            self.consumer.close()
            self.dlq_producer.flush()
            print("✅ Consumer shutdown complete")
            print(f"\n📊 Final Statistics:")
            print(f"   Total Orders Processed: {self.total_orders}")
            print(f"   Total Revenue: ${self.total_price:.2f}")
            print(f"   Average Order Price: ${self.running_average:.2f}")

if __name__ == '__main__':
    processor = OrderProcessor()
    processor.run()