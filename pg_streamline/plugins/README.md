# pg_streamline/plugins

This repository contains the necessary plugins for pg_streamline, which includes RabbitMQ's producer and consumer classes.

## Directory Structure

```
pg_streamline/plugins
├── __init__.py
└── rabbitmq
    ├── __init__.py
    └── producer.py
```

## Producer

The `RabbitMQProducer` class is used to produce messages and send them to a RabbitMQ broker.

### Usage

```python
from rabbitmq.producer import RabbitMQProducer

producer = RabbitMQProducer(rabbitmq_url="amqp://localhost", rabbitmq_exchange="table_exchange")
producer.perform_action("users", {"id": 1, "name": "John"})
```

## Consumer

The `RabbitMQConsumer` class is used to consume messages from a RabbitMQ broker.

### Usage

```python
from rabbitmq.consumer import RabbitMQConsumer

consumer = RabbitMQConsumer(
    rabbitmq_url="amqp://localhost",
    rabbitmq_exchange="table_exchange",
    routing_keys="key1,key2",
    queue="my_queue"
)
consumer.run_consumer()
```

## Coming Soon

### Kafka

Kafka support is coming soon. Stay tuned for updates!

## Installation

1. Clone this repository
2. Install the required packages
3. Run your producer or consumer

For more details, please refer to the inline documentation in each class.
