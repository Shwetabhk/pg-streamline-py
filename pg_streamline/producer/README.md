# Consumer Class for PostgreSQL Logical Replication

The 'Consumer' class is designed to handle PostgreSQL logical replication by processing incoming replication messages and performing actions based on the message type. This class is intended to be subclassed, and specific actions should be implemented in the subclass.

## Attributes

- 'params' (Dict[str, str]): Connection parameters for the PostgreSQL database.
- 'conn_pool': Connection pool for managing database connections.

## Initialization

```python
def __init__(self, config_path: str = None) -> None:
    """
    Initialize the Consumer class.

    Args:
        config_path (str): The path to the configuration file.
    """
```

The '__init__' method initializes the 'Consumer' class. It takes an optional 'config_path' parameter to specify the path to the configuration file. The method performs the following steps:

1. Sets up custom logging for the consumer.
2. Parses the configuration file specified by 'config_path'.
3. Validates the configuration to ensure it contains the required database connection parameters.
4. Initializes the database connection parameters and creates a connection pool.
5. Registers a signal handler to handle graceful termination.

## Configuration Validation

```python
@staticmethod
def __validate_config(config: dict) -> None:
    """
    Validate the configuration file.

    Args:
        config (dict): The parsed configuration file.
    """
```

The '__validate_config' method validates the configuration file to ensure it contains the required keys for database configuration. It raises a 'ConnectionError' if any required key is missing.

## Termination

```python
def perform_termination(self) -> None:
    """
    Perform termination tasks. This method should be overridden by subclass.
    """
```

The 'perform_termination' method is meant to be overridden by a subclass and should implement any termination tasks specific to the consumer.

```python
def __terminate(self, *args) -> None:
    """
    Terminate the consumer process gracefully.
    """
```

The '__terminate' method is called when the consumer process is terminated gracefully. It closes all database connections and invokes the 'perform_termination' method before exiting the process.

## Message Processing

```
def perform_action(self, message_type: str, parsed_message: dict) -> None:
    """
    Perform an action based on the message type and parsed message.
    This method should be overridden by subclass.

    Args:
        message_type (str): The type of the message ('I', 'U', 'D').
        parsed_message (dict): The parsed message data.
    """
```

The 'perform_action' method should be overridden by a subclass to specify actions to be taken based on the message type and the parsed message data.

```python
def process_incoming_message(self, table_name: str, data: bytes) -> None:
    """
    Process incoming messages and delegate to the appropriate handler.

    Args:
        table_name (str): The name of the table the message is related to.
        data (bytes): The raw message data.
    """
```

The 'process_incoming_message' method processes incoming replication messages by determining their type and delegating them to the appropriate handler (insert, update, delete). It also calls the 'perform_action' method with the parsed message data.

## Example Usage

```python
# Create a subclass of Consumer and implement custom actions
class CustomConsumer(Consumer):
    def perform_action(self, message_type: str, parsed_message: dict) -> None:
        # Implement custom logic based on message type and parsed message
        pass

# Initialize the custom consumer
custom_consumer = CustomConsumer(config_path="config.yaml")

# Start the consumer to process replication messages
# custom_consumer.start()
```

To use the 'Consumer' class, you can create a subclass and implement custom logic for handling replication messages based on your application's requirements.