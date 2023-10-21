import json
import logging
import signal
import sys
from typing import Dict

import psycopg2

from pg_streamline import (
    InsertMessage,
    UpdateMessage,
    DeleteMessage
)

from pg_streamline.utils import (
    setup_custom_logging,
    parse_yaml_config
)


class Consumer:
    """
    Consumer class for handling PostgreSQL logical replication.

    Attributes:
        params (Dict[str, str]): Connection parameters for PostgreSQL database.
        conn_pool: Connection pool for database connections.
    """

    def __init__(self, config_path: str = None) -> None:
        """
        Initialize the Consumer class.

        Args:
            config_path (str): The path to the configuration file.
        """
        setup_custom_logging()

        config = parse_yaml_config(config_file_path=config_path)

        self.__validate_config(config)
        
        self.config = config

        self.params: Dict[str, str] = {
            'dbname': config['database']['name'],
            'user': config['database']['user'],
            'password': config['database']['password'],
            'host': config['database']['host'],
            'port': config['database']['port']
        }

        pool_size = config['database']['connection_pool_size']
        self.conn_pool = psycopg2.pool.SimpleConnectionPool(1, pool_size, **self.params)

        logging.info(f'Consumer initialized for database: {self.params.get("dbname")} on host: {self.params.get("host")}:{self.params.get("port")}')
        signal.signal(signal.SIGINT, self.__terminate)

    @staticmethod
    def __validate_config(config: dict) -> None:
        """
        Validate the configuration file.

        Args:
            config (dict): The parsed configuration file.
        """
        # Define required keys for database configuration
        required_db_keys = [
            'name', 'user', 'password', 'host', 'port', 'connection_pool_size'
        ]

        # Check if 'database' key exists in config
        if 'database' not in config:
            raise ConnectionError('Database configuration not found in config file.')

        database_config = config['database']

        # Validate required keys for database configuration
        for key in required_db_keys:
            if key not in database_config:
                raise ConnectionError(f'Database {key} not found in config file.')

    def perform_termination(self) -> None:
        """
        Perform termination tasks. This method should be overridden by subclass.
        """
        raise NotImplementedError('You must implement the perform_termination method in your consumer class.')

    def __terminate(self, *args) -> None:
        """
        Terminate the consumer process gracefully.
        """
        logging.info('Terminating consumer')
        self.conn_pool.closeall()

        self.perform_termination()

        # Exiting the process gracefully
        sys.exit(0)

    def perform_action(self, message_type: str, parsed_message: dict) -> None:
        """
        Perform an action based on the message type and parsed message.
        This method should be overridden by subclass.

        Args:
            message_type (str): The type of the message ('I', 'U', 'D').
            parsed_message (dict): The parsed message data.
        """
        logging.debug(f'Consumer - Parsed message: {parsed_message}')
        logging.debug(f'Consumer - Message type: {message_type}')

        raise NotImplementedError('You must implement the perform_action method in your consumer class.')

    def process_incoming_message(self, table_name: str, data: bytes) -> None:
        """
        Process incoming messages and delegate to the appropriate handler.

        Args:
            table_name (str): The name of the table the message is related to.
            data (bytes): The raw message data.
        """
        connection = self.conn_pool.getconn()
        cursor = connection.cursor()

        try:
            logging.debug(f'Incoming message: {data}')
            message_type = data[:1].decode('utf-8')
            parsed_message = {}

            if message_type == 'I':
                logging.info(f'INSERT Message, Message Type: {message_type} - {table_name}')
                parser = InsertMessage(data, cursor=cursor)
                parsed_message = parser.decode_insert_message()

            elif message_type == 'U':
                logging.info(f'UPDATE Message, Message Type: {message_type} - {table_name}')
                parser = UpdateMessage(data, cursor=cursor)
                parsed_message = parser.decode_update_message()

            elif message_type == 'D':
                logging.info(f'DELETE Message, Message Type: {message_type} - {table_name}')
                parser = DeleteMessage(data, cursor=cursor)
                parsed_message = parser.decode_delete_message()

            cursor.close()
            self.conn_pool.putconn(connection)

            if parsed_message:
                logging.debug(f'Message type: {message_type}, parsed message: {json.dumps(parsed_message, indent=4)}')
                self.perform_action(message_type, parsed_message)

            if message_type in ('I', 'U', 'D'):
                logging.info(f'Sending feedback, Message Type: {message_type} - {table_name}')
        except Exception as e:
            logging.exception(f'An error occurred: {e}')
            cursor.close()
            self.conn_pool.putconn(connection)
