# PostgreSQL Logical Replication Producer

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
  - [Initialization](#initialization)
  - [Starting Replication](#starting-replication)
  - [Performing Actions](#performing-actions)
- [Configuration](#configuration)
- [Logging](#logging)
- [Signal Handling](#signal-handling)
- [Contributing](#contributing)
- [License](#license)

## Overview

This Python-based producer handles logical replication for PostgreSQL databases. It supports both `pgoutput` and `wal2json` output plugins and provides a simple yet powerful API for extending its functionalities.

## Features

- Connection pooling for efficient database connections.
- Supports both `pgoutput` and `wal2json` output plugins.
- Asynchronous processing of database changes.
- Graceful shutdown and cleanup.
- Extensible for custom actions on database changes.

## Requirements

- Python 3.8+
- PostgreSQL 10+
- psycopg2

## Installation

```bash
pip install -r requirements.txt
```

## Usage

### Initialization

To initialize the producer, you need to provide the database connection parameters and optionally the output plugin and pool size.

```python
from producer import Producer

params = {
    'dbname': 'your_db',
    'user': 'your_user',
    'password': 'your_password',
    'host': 'your_host',
    'port': 'your_port',
    'replication_slot': 'your_replication_slot'
}

producer = Producer(pool_size=5, output_plugin='pgoutput', **params)
```

### Starting Replication

To start the replication process, you need to specify the publication names and the protocol version.

```python
producer.start_replication(publication_names=['your_publication'], protocol_version='your_protocol_version')
```

### Performing Actions

To perform custom actions on database changes, you can extend the 'Producer' class and override the 'perform_action' method.

```python
class CustomProducer(Producer):
    def perform_action(self, table_name, bytes_data):
        # Your custom logic here
```

### Termination

The producer listens for a 'SIGINT' (Ctrl+C) signal to gracefully terminate the replication process and close all database connections.

```bash
# To terminate the producer
Ctrl+C
```

### Logging

The producer uses Python's built-in logging to provide debug and operational information. You can configure the logging level and format as per your requirements.

### Error Handling

The producer is designed to handle common errors gracefully. For example, if a replication slot already exists, the producer will log a debug message and continue.

### Concurrency

The producer uses Python's 'ThreadPoolExecutor' for concurrent processing of database changes, ensuring efficient use of resources.

### Extending the Producer

You can extend the 'Producer' class to implement custom logic for handling database changes. To do so, create a subclass and override the 'perform_action' method.

Here's an example:

```python
import json
import logging
from pg_streamline import Producer  # Importing the Producer class from the producer module

# Setting up basic logging configuration
logging.basicConfig(level=logging.INFO)

class PGOutputPGOutputProducer(Producer):
    """
    PGOutputProducer class that extends the Producer class to handle specific types of messages.
    """

    def perform_action(self, table_name: str, data: dict):
        """
        Overriding the perform_action method to handle different types of messages.

        :param table_name: The name of the table where the change occurred.
        :param data: The data related to the change.
        """
        logging.info(f'Table name: {table_name}')
        logging.info(f'Data: {data}')

if __name__ == '__main__':
    # Database and replication parameters
    params = {
        'dbname': 'dummy',
        'user': 'postgres',
        'password': 'postgres',
        'host': 'localhost',
        'port': '5432',
        'replication_slot': 'pgtest'
    }

    # Creating an instance of PGOutputProducer with a pool size of 5
    producer = PGOutputPGOutputProducer(pool_size=5, **params)

    # Starting the replication process
    producer.start_replication(publication_names=['events'], protocol_version='4')
```

By extending the 'Producer' class, you can customize the behavior of the producer to suit your specific needs.
