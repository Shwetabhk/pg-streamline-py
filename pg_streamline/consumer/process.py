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

from pg_streamline.utils import setup_custom_logging


class Consumer:
    """
    Consumer class for handling PostgreSQL logical replication.

    Attributes:
        params (Dict[str, str]): Connection parameters for PostgreSQL database.
        conn_pool: Connection pool for database connections.
    """

    def __init__(self, pool_size: int = 5, **kwargs) -> None:
        """
        Initialize the Consumer class.

        Args:
            pool_size (int): The size of the connection pool.
            **kwargs: Database connection parameters.
        """
        setup_custom_logging()

        self.params: Dict[str, str] = {
            'dbname': kwargs.get('dbname'),
            'user': kwargs.get('user'),
            'password': kwargs.get('password'),
            'host': kwargs.get('host'),
            'port': kwargs.get('port')
        }

        self.conn_pool = psycopg2.pool.SimpleConnectionPool(1, pool_size, **self.params)

        logging.info(f'Consumer initialized for database: {kwargs.get("dbname")} on host: {kwargs.get("host")}:{kwargs.get("port")}')
        signal.signal(signal.SIGINT, self.__terminate)

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
        logging.debug(f'Incoming message: {data}')
        message_type = data[:1].decode('utf-8')
        connection = self.conn_pool.getconn()
        cursor = connection.cursor()

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
