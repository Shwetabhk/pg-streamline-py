# PostgreSQL Logical Replication Consumer

## Overview

The given Python script defines a `Consumer` class that is responsible for handling PostgreSQL logical replication events. The class uses the `psycopg2` library for database interactions and is capable of consuming messages for `INSERT`, `UPDATE`, and `DELETE` operations performed on the database tables. The code also makes use of a custom module named `pg_streamline` for parsing these messages.

## Prerequisites

- Python 3.x
- psycopg2 library
- Custom `pg_streamline` module for message parsing

## Class Attributes

### `params`

A dictionary containing the database connection parameters.

### `conn_pool`

A connection pool object from `psycopg2.pool.SimpleConnectionPool`.

## Class Methods

### `__init__(self, pool_size: int = 5, **kwargs) -> None`

Initializes the `Consumer` class. It sets up custom logging and initializes the database connection pool.

```python
def __init__(self, pool_size: int = 5, **kwargs) -> None:
    setup_custom_logging()
    self.params = {
        'dbname': kwargs.get('dbname'),
        'user': kwargs.get('user'),
        'password': kwargs.get('password'),
        'host': kwargs.get('host'),
        'port': kwargs.get('port')
    }
    self.conn_pool = psycopg2.pool.SimpleConnectionPool(1, pool_size, **self.params)
```

### `perform_termination(self) -> None`

This method should be overridden by subclasses to define what actions should be taken during the termination process.

```python
def perform_termination(self) -> None:
    raise NotImplementedError('You must implement the perform_termination method in your consumer class.')
```

### `__terminate(self, *args) -> None`

Handles the termination of the consumer process gracefully.

```python
def __terminate(self, *args) -> None:
    logging.info('Terminating consumer')
    self.conn_pool.closeall()
    self.perform_termination()
    sys.exit(0)
```

### `perform_action(self, message_type: str, parsed_message: dict) -> None`

This method should be overridden by subclasses to define what actions should be taken based on the message type and parsed message data.

```python
def perform_action(self, message_type: str, parsed_message: dict) -> None:
    raise NotImplementedError('You must implement the perform_action method in your consumer class.')
```

### `process_incoming_message(self, table_name: str, data: bytes) -> None`

Processes incoming messages and delegates them to the appropriate handler method based on the message type.

```python
def process_incoming_message(self, table_name: str, data: bytes) -> None:
    message_type = data[:1].decode('utf-8')
    # ... rest of the code
```
