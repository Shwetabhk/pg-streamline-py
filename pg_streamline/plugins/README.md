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

producer = RabbitMQProducer(config_path='config.yml')
```

## Consumer

The `RabbitMQConsumer` class is used to consume messages from a RabbitMQ broker.

### Usage

```python
from rabbitmq.consumer import RabbitMQConsumer

consumer = RabbitMQConsumer(
    config_path='config.yml',
)
consumer.run_consumer()
```

## Config File

This is an example of a RabbitMQ config file:

```yaml
# Configuration for pg-streamline

# Database Configuration
database:
  name: dummy
  user: postgres
  password: ${DB_PASSWORD}
  host: localhost
  port: 5432
  connection_pool_size: 5
  replication_plugin: pgoutput
  replication_slot: pgtest

# RabbitMQ Configuration
rabbitmq:
  url: amqp://guest:guest@localhost:5672?heartbeat=0
  queue: pgtest
  exchange: pg-exchange
  routing_keys: 
    - public.pgbench_accounts
    - public.pgbench_branches
    - public.pgbench_history
    - public.pgbench_tellers
    - public.users

```

## Coming Soon

### Kafka

Kafka support is coming soon. Stay tuned for updates!

## Installation

1. Clone this repository
2. Install the required packages
3. Run your producer or consumer

For more details, please refer to the inline documentation in each class.
