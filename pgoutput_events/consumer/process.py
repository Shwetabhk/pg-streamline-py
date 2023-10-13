import json
import logging
from typing import Dict

import psycopg2

from pgoutput_events import (
    InsertMessage,
    UpdateMessage,
    DeleteMessage
)

from pgoutput_events.utils import setup_custom_logging


class Consumer:
    def __init__(self, pool_size: int = 5, **kwargs) -> None:
        setup_custom_logging()

        self.params: Dict[str, str] = {
            'dbname': kwargs.get('dbname'),
            'user': kwargs.get('user'),
            'password': kwargs.get('password'),
            'host': kwargs.get('host'),
            'port': kwargs.get('port')
        }

        self.connection = psycopg2.connect(**self.params)

        logging.info(f'Consumer initialized for database: {kwargs.get("dbname")} on host: {kwargs.get("host")}:{kwargs.get("port")}')

    def perform_action(self, message_type: str, parsed_message: dict) -> None:
        logging.debug(f'Consumer - Parsed message: {parsed_message}')
        logging.debug(f'Consumer - Message type: {message_type}')

        raise NotImplementedError('You must implement the perform_action method in your consumer class.')

    def process_incoming_message(self, table_name, data):
        logging.debug(f'Incoming message: {data}')
        message_type = data[:1].decode('utf-8')
        connection = self.connection
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
        self.connection.close()

        if parsed_message:
            logging.debug(f'Message type: {message_type}, parsed message: {json.dumps(parsed_message, indent=4)}')
            self.perform_action(message_type, parsed_message)

        if message_type in ('I', 'U', 'D'):
            logging.info(f'Sending feedback, Message Type: {message_type} - {table_name}')
