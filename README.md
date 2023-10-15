# pg-streamline-py

## Streamlining PostgreSQL Logical Replication and Event Processing

pg-streamline is a Python library designed to simplify and streamline the process of logical replication and event processing with PostgreSQL. It provides modular components for producers, consumers, parsers, and plugins, making it highly extensible and customizable.

---

### Table of Contents

1. [Installation](#installation)
2. [Features](#features)
3. [Usage](#usage)
   - [Producer](#producer)
   - [Consumer](#consumer)
   - [Parser](#parser)
   - [Plugins](#plugins)
4. [Contributing](#contributing)
5. [License](#license)

---

### Installation

```bash
pip install pg-streamline
```

## Features

### Producer

- Handles PostgreSQL logical replication.
- Supports multiple output plugins like 'pgoutput' and 'wal2json'.
- Pooling support for better performance.

For more details, see the [Producer README](./pg_streamline/producer/README.md).

### Consumer

- Consumes and processes the events replicated by the producer.
- Extensible: Can be extended to perform custom actions when specific database changes occur.

For more details, see the [Consumer README](./pg_streamline/consumer/README.md).

### Parser

- Parses different types of logical events from PostgreSQL.
- Supports parsing of INSERT, UPDATE, and DELETE events.
- Makes it easier to understand and act upon the changes in the database.

For more details, see the [Parser README](./pg_streamline/parser/README.md).

### Plugins

- Extend the functionality of pgStreamline with various plugins.
- Currently supports RabbitMQ for message queuing.
- More plugins are in development.

For more details, see the [Plugins README](./pg_streamline/plugins/README.md).

## Usage

Please refer to the README files in each module's directory for specific usage instructions:

- [Producer Usage](./pg_streamline/producer/README.md)
- [Consumer Usage](./pg_streamline/consumer/README.md)
- [Parser Usage](./pg_streamline/parser/README.md)
- [Plugins Usage](./pg_streamline/plugins/README.md)

## Contributing

We welcome contributions! Please see the [Contributing Guidelines](CONTRIBUTING.md) for more details.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
