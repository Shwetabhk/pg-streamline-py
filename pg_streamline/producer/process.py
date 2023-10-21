import signal
import logging
import sys
from typing import Optional, Dict, Any
from concurrent.futures import ThreadPoolExecutor

import psycopg2
from psycopg2.extras import LogicalReplicationConnection
from psycopg2 import pool, OperationalError

from pg_streamline.utils import (
    setup_custom_logging,
    Utils as parser_utils,
    parse_yaml_config
)


logger = logging.getLogger(__name__)


class Producer:
    """
    Producer Class for handling PostgreSQL logical replication.

    Attributes:
        params (Dict[str, str]): Connection parameters for PostgreSQL database.
        replication_slot (str): The name of the replication slot to use.
        conn_pool: Connection pool for database connections.
        replication_cursor: Cursor for logical replication.
        output_plugin (str): The output plugin to use ('pgoutput' or 'wal2json').
    """

    def __init__(self, config_path: str = None) -> None:
        """
        Initialize the Producer class.

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
            'port': config['database']['port'],
            'connection_factory': LogicalReplicationConnection
        }
        self.replication_slot: str = config['database']['replication_slot']
        self.output_plugin = config['database']['replication_plugin']
        pool_size = config['database']['connection_pool_size']
        self.conn_pool = pool.SimpleConnectionPool(1, pool_size, **self.params)

        connection = self.conn_pool.getconn()
        self.replication_cursor = connection.cursor()

        self.__create_replication_slot(self.replication_slot)

        logger.info(f'Producer initialized for database: {self.params.get("dbname")} on host: {self.params.get("host")}:{self.params.get("port")}')
        logger.info(f'Using replication slot: {self.replication_slot}')
        logger.info(f'Using output plugin: {self.output_plugin}')

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
            'name', 'user', 'password', 'host', 'port', 
            'connection_pool_size', 'replication_plugin', 'replication_slot'
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
        Perform termination of the replication process.
        """
        raise NotImplementedError('You must implement the perform_termination method in your producer class.')

    def __terminate(self, *args):
        """
        Terminate the replication process.

        Args:
            signum (int): Signal number.
            frame (frame): Current stack frame.
        """
        logger.info('Terminating replication process')

        self.replication_cursor.close()
        self.conn_pool.closeall()

        logger.info('Replication process terminated')

        self.perform_termination()
        # Exiting the process gracefully
        sys.exit(0)

    def __create_replication_slot(self, slot_name: str) -> None:
        """
        Create a new logical replication slot.

        Args:
            slot_name (str): The name of the replication slot to create.
        """
        try:
            connection = self.conn_pool.getconn()
            cursor = connection.cursor()
            cursor.execute(
                "select pg_create_logical_replication_slot(%s, %s);",
                (slot_name, self.output_plugin)
            )
            logger.debug('Replication slot created')
            self.__close_connection(cursor, connection)
        except psycopg2.errors.DuplicateObject:
            logger.debug('Replication slot already exists')
        except OperationalError:
            logger.exception("Operational error during initialization.")
            raise OperationalError("Operational error during initialization.")
    
    def __close_connection(self, cursor, connection):
        """
        Close the connection and cursor.

        Args:
            cursor (psycopg2.extensions.cursor): Database cursor.
            connection (psycopg2.extensions.connection): Database connection.
        """
        cursor.close()
        self.conn_pool.putconn(connection)

    def __get_table_name(self, relation_id: int, cur: Optional[psycopg2.extensions.cursor]) -> str:
        """
        Get the table name using the relation ID.

        Args:
            relation_id (int): The relation ID of the table.
            cur (Optional[psycopg2.extensions.cursor]): Database cursor.

        Returns:
            str: Full table name including schema.
        """
        try:
            cur.execute(
                "select schemaname, relname from pg_stat_user_tables where relid = %s;",
                (relation_id,)
            )
            schema_name, table_name = cur.fetchone()
            return f'{schema_name}.{table_name}'
        except Exception:
            logger.exception("Failed to get table name.")
            raise
    
    def send_feedback(self, flush_lsn: int) -> None:
        """
        Send feedback to the PostgreSQL server.

        Args:
            flush_lsn (int): The LSN to send feedback for.
        """
        self.replication_cursor.send_feedback(flush_lsn=flush_lsn)

    def __process_wal2json_change(self, data: Any) -> None:
        """
        Process a single change event for plugin wal2json.

        Args:
            data (Any): The incoming data to process.
        """
        try:
            logger.info(f'Change occurred at LSN: {data.data_start}')
            self.perform_action('wal2json', data.payload)
            self.send_feedback(flush_lsn=data.data_start)
            logger.info(f'Change processed at LSN: {data.data_start}')
        except Exception:
            logger.exception("Failed to process change.")
            self.send_feedback(flush_lsn=data.data_start)
            raise Exception("Failed to process change.")

    def __process_pgoutput_change(self, data: Any) -> None:
        """
        Process a single change event for plugin pgoutput.

        Args:
            data (Any): The incoming data to process.
        """
        connection = self.conn_pool.getconn()
        cursor = connection.cursor()

        try:
            message_type = data.payload[:1].decode('utf-8')
            relation_id = parser_utils.convert_bytes_to_int(data.payload[1:5])
            if message_type in ['I', 'U', 'D']:
                operation_type = 'INSERT' if message_type == 'I' else 'UPDATE' if message_type == 'U' else 'DELETE'
                table_name = self.__get_table_name(relation_id, cursor)

                logger.info(f'{operation_type} Change occurred on table: {table_name}')
                logger.info(f'{operation_type} Change occurred at LSN: {data.data_start}')

                self.perform_action(table_name, data.payload)

                logger.info(f'{operation_type} Change processed on table: {table_name}')
                logger.info(f'{operation_type} Change processed at LSN: {data.data_start}')

            self.send_feedback(flush_lsn=data.data_start)
            self.__close_connection(cursor, connection)
        except Exception:
            logger.exception("Failed to process change.")
            self.send_feedback(flush_lsn=data.data_start)
            self.__close_connection(cursor, connection)
            raise Exception("Failed to process change.")

    def __process_changes(self, data: Any) -> None:
        """
        Process a single change event.

        Args:
            data (Any): The incoming data to process.
        """
        with ThreadPoolExecutor() as executor:
            if self.output_plugin == 'pgoutput':
                executor.submit(self.__process_pgoutput_change, data)
            elif self.output_plugin == 'wal2json':
                executor.submit(self.__process_wal2json_change, data)

    def perform_action(self, table_name: str, bytes_message: dict):
        """
        Perform an action based on the table name and parsed message.

        Args:
            table_name (str): The name of the table.
            bytes_message (dict): The parsed message.

        Raises:
            NotImplementedError: This method should be overridden by subclass.
        """
        logger.debug(f'Table name: {table_name}')
        logger.debug(f'Byte Data: {bytes_message}')

        raise NotImplementedError('This method should be overridden by subclass')

    def start_replication(self, publication_names: list, protocol_version: str) -> None:
        """
        Start the logical replication process.

        Args:
            publication_names (str): The names of the publications to replicate.
            protocol_version (str): The protocol version to use.
        """
        options = {}

        if self.output_plugin == 'pgoutput':
            options = {
                'proto_version': protocol_version,
                'publication_names': ','.join(publication_names)
            }
            logger.info(f'Starting replication with publications: {publication_names} and protocol version: {protocol_version}')

        self.replication_cursor.start_replication(slot_name=self.replication_slot, decode=False, options=options)
        self.replication_cursor.consume_stream(self.__process_changes)
