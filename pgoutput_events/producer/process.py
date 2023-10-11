import json
import logging
from typing import Optional, Dict, Any
from concurrent.futures import ThreadPoolExecutor

import psycopg2
from psycopg2.extras import LogicalReplicationConnection
from psycopg2 import pool

from pgoutput_events.utils import (
    setup_custom_logging,
    Utils as parser_utils
)


class Producer:
    """Class to handle PostgreSQL logical replication."""

    def __init__(self, pool_size: int = 5, **kwargs) -> None:
        setup_custom_logging()

        self.params: Dict[str, str] = {
            'dbname': kwargs.get('dbname'),
            'user': kwargs.get('user'),
            'password': kwargs.get('password'),
            'host': kwargs.get('host'),
            'port': kwargs.get('port'),
            'connection_factory': LogicalReplicationConnection
        }
        self.replication_slot: str = kwargs.get('replication_slot')
        self.conn_pool = pool.SimpleConnectionPool(1, pool_size, **self.params)
        self.conn: Optional[psycopg2.extensions.connection] = None
        self.cur: Optional[psycopg2.extensions.cursor] = None

        self.__connect()
        self.__create_replication_slot(self.replication_slot)

        logging.info(f'Producer initialized for database: {kwargs.get("dbname")} on host: {kwargs.get("host")}:{kwargs.get("port")}')
        logging.info(f'Using replication slot: {self.replication_slot}')

    def __connect(self) -> None:
        """Connect to the PostgreSQL database."""
        self.conn = self.conn_pool.getconn()
        self.cur = self.conn.cursor()
    
    @staticmethod
    def __get_table_name(relation_id: int, cur: Optional[psycopg2.extensions.cursor]) -> str:
        """Get the table name from the relation ID."""
        cur.execute(
            f"SELECT schemaname, relname FROM pg_stat_user_tables WHERE relid = {relation_id};"
        )
        schema_name, table_name = cur.fetchone()
        return f'{schema_name}.{table_name}'

    def __create_replication_slot(self, slot_name: str) -> None:
        try:
            self.cur.execute(
                f"SELECT pg_create_logical_replication_slot('{slot_name}', 'pgoutput');"
            )
            logging.debug('Replication slot created')
        except psycopg2.errors.DuplicateObject:
            logging.debug('Replication slot already exists')

    def __process_changes(self, data: Any) -> None:
        with ThreadPoolExecutor() as executor:
            executor.submit(self.__process_single_change, data)

    def __process_single_change(self, data: Any) -> None:
        connection = self.conn_pool.getconn()
        cursor = connection.cursor()
        logging.info(f'Processing change: {data.payload}')
        relation_id = parser_utils.convert_bytes_to_int(data.payload[1:5])

        table_name = self.__get_table_name(relation_id, cursor)

        logging.info(f'Change occurred on table: {table_name}')

        self.cur.send_feedback(flush_lsn=data.data_start)

    def perform_action(self, message_type: str, parsed_message: dict):
        logging.debug(f'Message type: {message_type}')
        logging.debug(f'Parsed message: {json.dumps(parsed_message, indent=4)}')

        raise NotImplementedError('This method should be overridden by subclass')

    def start_replication(self, publication_names: str, protocol_version: str) -> None:
        self.cur.close()
        self.cur = self.conn.cursor()
        self.cur.start_replication(slot_name=self.replication_slot, decode=False, options={
            'publication_names': ','.join(publication_names),
            'proto_version': protocol_version
        })
        logging.info(f'Starting replication with publications: {publication_names} and protocol version: {protocol_version}')
        self.cur.consume_stream(self.__process_changes)
