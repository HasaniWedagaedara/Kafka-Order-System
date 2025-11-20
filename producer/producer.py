import time
import random
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# Configuration
KAFKA_BROKER = 'localhost:9092'
SCHEMA_REGISTRY_URL = 'http://localhost:8081'
TOPIC_NAME = 'orders'

# Load Avro schema
with open('schemas/order.avsc', 'r') as f:
    schema_str = f.read()

# Products list
PRODUCTS = ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Headphones', 
            'Webcam', 'USB Cable', 'SSD', 'RAM', 'Graphics Card']

def delivery_report(err, msg):
    """Callback for message delivery reports"""
    if err is not None:
        print(f'❌ Message delivery failed: {err}')
    else:
        print(f'✅ Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

def order_to_dict(order, ctx):
    """Convert order object to dictionary for serialization"""
    return order

def main():
    # Setup Schema Registry client
    schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    
    # Setup Avro serializer
    avro_serializer = AvroSerializer(
        schema_registry_client,
        schema_str,
        order_to_dict
    )
    
    # Setup Producer
    producer_conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'client.id': 'order-producer'
    }
    producer = Producer(producer_conf)
    
    print("🚀 Starting Order Producer...")
    print("📊 Producing orders to Kafka topic:", TOPIC_NAME)
    print("-" * 60)
    
    order_count = 0
    try:
        while True:
            order_count += 1
            
            # Create random order
            order = {
                'orderId': f'ORD-{order_count:05d}',
                'product': random.choice(PRODUCTS),
                'price': round(random.uniform(10.0, 1000.0), 2),
                'timestamp': int(time.time() * 1000)
            }
            
            print(f"\n📦 Producing Order #{order_count}:")
            print(f"   Order ID: {order['orderId']}")
            print(f"   Product:  {order['product']}")
            print(f"   Price:    ${order['price']:.2f}")
            
            # Serialize and send
            serialized_order = avro_serializer(
                order,
                SerializationContext(TOPIC_NAME, MessageField.VALUE)
            )
            
            producer.produce(
                topic=TOPIC_NAME,
                value=serialized_order,
                key=order['orderId'].encode('utf-8'),
                on_delivery=delivery_report
            )
            
            # Trigger delivery reports
            producer.poll(0)
            
            # Wait before next order
            time.sleep(2)
            
    except KeyboardInterrupt:
        print("\n\n⏹️  Producer stopped by user")
    finally:
        print("\n🔄 Flushing remaining messages...")
        producer.flush()
        print("✅ Producer shutdown complete")

if __name__ == '__main__':
    main()