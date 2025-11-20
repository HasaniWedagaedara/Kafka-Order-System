# Kafka Order System

This project demonstrates a **Kafka-based order processing system** using Python, Avro serialization, and Confluent Kafka. It features a producer that generates orders and a consumer that processes them with retry logic and a Dead Letter Queue (DLQ) for failed messages.

---

## Features

- **Kafka Producer**  
  - Generates random orders with `orderId`, `product`, `price`, and `timestamp`.  
  - Serializes orders using **Avro** via Confluent Schema Registry.  
  - Publishes orders to the Kafka topic `orders`.  

- **Kafka Consumer**  
  - Consumes orders from the Kafka topic `orders`.  
  - Implements **retry logic** for temporary failures.  
  - Sends permanently failed messages to a **Dead Letter Queue (`orders-dlq`)**.  
  - Maintains **real-time aggregation**: total orders, total revenue, and running average price.  

- **Schema Management**  
  - Uses Avro schemas to enforce structure and data consistency.  
  - Schema stored in `schemas/order.avsc`.  

- **DLQ Handling**  
  - Messages that fail after maximum retries are sent to the `orders-dlq` topic for later inspection.

---

## Prerequisites

- Python 3.11  
- Kafka and Zookeeper running locally  
- Confluent Schema Registry running locally  

---

## Setup Instructions

1. **Clone the repository**

```bash
git clone https://github.com/HasaniWedagaedara/Kafka-Order-System.git
cd Kafka-Order-System
```
2. **Create a Python virtual environment**

```bash
py -3.11 -m venv venv
source venv/Scripts/activate
```

3. **Install dependencies**

```bash
pip install -r requirements.txt
```

3. **Run the System**

## Start the docker containers

```bash
docker-compose up -d
```

## Start the Producer

```bash
python producer/producer.py
```

## Start the Consumer

```bash
python consumer/consumer.py
```