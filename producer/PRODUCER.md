# Producer Class

## Overview

The `Producer` class is designed to handle PostgreSQL logical replication. It provides a Pythonic interface to set up and manage logical replication slots, connect to the PostgreSQL database, and process changes in the data.

## Features

- **Database Connection**: Establishes a connection to a PostgreSQL database.
- **Replication Slot Management**: Creates a new logical replication slot if it doesn't already exist.
- **Data Change Processing**: Processes changes in the data and logs them. It supports Insert, Update, and Delete operations.
- **Extensibility**: Provides a placeholder method `perform_action` for users to plug in their own actions based on the changes in the data.

## Methods

### Private Methods

#### `__new_connection(params: Dict[str, str]) -> psycopg2.extensions.connection`

This private static method creates a new database connection.

- **Parameters**: 
  - `params`: Database connection parameters.
  
- **Returns**: New database connection.

#### `__connect() -> None`

This private method connects to the PostgreSQL database using the parameters provided during the initialization of the `Producer` object.

#### `__create_replication_slot(slot_name: str) -> None`

This private method creates a new logical replication slot.

- **Parameters**: 
  - `slot_name`: Name of the replication slot to create.

#### `__process_changes(data: Any) -> None`

This private method processes the changes received from the logical replication.

- **Parameters**: 
  - `data`: The data payload from the replication stream.

### Public Methods

#### `__init__(dbname: str, user: str, password: str, host: str, port: str, replication_slot: str) -> None`

Initializes the `Producer` object and sets up the database connection and replication slot.

- **Parameters**: 
  - `dbname`: Name of the database.
  - `user`: Database user.
  - `password`: Database password.
  - `host`: Database host.
  - `port`: Database port.
  - `replication_slot`: Replication slot name.

#### `perform_action(message_type: str, parsed_message: dict)`

This is a placeholder for users to plug in their own actions based on the changes in the data.

- **Parameters**: 
  - `message_type`: The type of message (Insert, Update, Delete).
  - `parsed_message`: The parsed message as a dictionary.

#### `start_replication(publication_names: str, protocol_version: str) -> None`

Starts the logical replication.

- **Parameters**: 
  - `publication_names`: Comma-separated list of publication names.
  - `protocol_version`: Protocol version to use.

## Usage Example

```python
params = {
    'dbname': 'my_database',
    'user': 'my_user',
    'password': 'my_password',
    'host': 'localhost',
    'port': '5432',
    'replication_slot': 'my_replication_slot'
}

producer = Producer(**params)
producer.start_replication(['my_publication'], '2')
```

## Extending the Class

To extend the functionality, you can subclass `Producer` and override the `perform_action` method to implement your own logic based on the changes in the data.

```python
class MyProducer(Producer):
    def perform_action(self, message_type, parsed_message):
        # Your custom logic here
```

You can copy and paste this section into your `README.md` file.

## Logging

The `Producer` class uses Python's built-in logging module. By default, it logs debug messages to indicate the creation of replication slots, the types of messages received, and the parsed messages. You can configure the logging level and add handlers to log to different outputs like files or external services.

```python
import logging

logging.basicConfig(level=logging.INFO) 
```

## Dependencies

The `Producer` class depends on the following Python packages:

* *psycopg2*: For connecting to the PostgreSQL database and handling logical replication.

```bash
pip install requirements.txt
```
