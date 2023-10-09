# pgoutput-parser

## Overview

`pgoutput-parser` is a Python-based PostgreSQL logical replication consumer. It listens to changes in the PostgreSQL database and processes INSERT, UPDATE, and DELETE events.

## Features

- Supports PostgreSQL logical replication
- Decodes INSERT, UPDATE, and DELETE messages
- Extensible for custom actions
- Built-in logging

## Directory Structure

```plaintext
pgoutput-parser/
├── README.md
├── main.py # This is a test file that can give you example code for extending Producer
├── pgoutput_events/
│   ├── __init__.py
│   ├── types/
│   │   ├── __init__.py
│   │   ├── base.py
│   │   ├── delete.py
│   │   ├── insert.py
│   │   └── update.py
│   └── utils.py
├── producer/
│   ├── PRODUCER.md
│   ├── __init__.py
│   ├── process.py
│   └── utils.py
└── requirements.txt
```

## Installation

1. Clone the repository:

    ```bash
    git clone https://github.com/shwetabhk/pgoutput-parser.git
    ```

2. Navigate to the project directory:

    ```bash
    cd pgoutput-parser
    ```

3. Install the required packages:

    ```bash
    pip install -r requirements.txt
    ```

## Usage

### Basic Usage

1. Update the `main.py` with your PostgreSQL database credentials and replication slot.

2. Run the `main.py` script:

    ```bash
    python main.py
    ```

### Extending the Producer

You can extend the `Producer` class to perform custom actions for each type of database event. See `PRODUCER.md` for more details.

## Logging

Logging is configured to output messages to the console. You can change the logging level by modifying the `logging.basicConfig` in your custom producer class.

## Dependencies

- Python 3.8+
- psycopg2
- PostgreSQL 11+

## Contributing

Please create an issue, fork the repo and create PR for any changes.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.
