# PG Streamline Examples

This folder contains example code for using the PG Streamline library. Each example demonstrates a different aspect or usage of the library.

## Table of Contents

1. [PGOutput Producer Example](#pgoutput-producer-example)
2. [Wal2JSON Producer Example](#wal2json-producer-example)
3. [RabbitMQ Consumer Example](#rabbitmq-consumer-example)
4. [RabbitMQ Producer Example](#rabbitmq-producer-example)

---

### PGOutput Producer Example

This example demonstrates how to use the 'Producer' class from PG Streamline to handle messages from a PostgreSQL database using the PGOutput logical decoding output plugin.

Snippet:
```
class PGOutputPGOutputProducer(Producer):
    def perform_action(self, table_name: str, data: dict):
        logging.info(f'Table name: {table_name}')
        logging.info(f'Data: {data}')
```

To run this example:
```
python pgoutput.py
```

---

### Wal2JSON Producer Example

This example shows how to use the 'Producer' class with the Wal2JSON logical decoding output plugin.

Snippet:
```
class PGWal2JSONPGOutputProducer(Producer):
    def perform_action(self, message_type: str, parsed_message: dict):
        logging.info(f'Message type: {message_type}')
```

To run this example:
```
python wal2json.py
```

---

### RabbitMQ Consumer Example

This example demonstrates how to extend the 'RabbitMQConsumer' to handle incoming messages.

Snippet:
```
class MyConsumer(RabbitMQConsumer):
    def perform_action(self, message_type: str, parsed_message: dict):
        logging.info(f'Performing action with message: {message_type}')
```

To run this example:
```
python -m examples.rabbitmq.consumer
```

---

### RabbitMQ Producer Example

This example shows how to use the 'RabbitMQProducer' to publish messages to a RabbitMQ broker.

Snippet:
```
class RabbitMQProducer(Producer):
    def perform_action(self, table_name: str, bytes_string: dict):
        logging.info(f'Table name: {table_name}')
```

To run this example:
```
python -m examples.rabbitmq.producer
```