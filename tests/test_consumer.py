from unittest import mock
import psycopg2

import pytest

from pg_streamline import Consumer
from tests.conftest import ExtendedConsumer


# Test process_incoming_message method for insert payload
def test_insert_process_incoming_message(extended_consumer_instance: ExtendedConsumer, insert_payload, mocked_schema):
    with mock.patch('psycopg2.pool.SimpleConnectionPool') as MockConnectionPool:
        extended_consumer_instance.conn_pool = MockConnectionPool.return_value
        extended_consumer_instance.conn_pool.getconn.return_value = mock.MagicMock()
        extended_consumer_instance.conn_pool.getconn.return_value.cursor.return_value.fetchall.return_value = mocked_schema

        extended_consumer_instance.process_incoming_message('public.users', insert_payload.payload)

        assert extended_consumer_instance.name == 'Test is successful'

        with mock.patch('pg_streamline.parser.insert.InsertMessage.decode_insert_message') as mock_decode_insert_message:
            with mock.patch('logging.exception') as mock_logger:
                mock_decode_insert_message.side_effect = Exception('Error connecting to database.')

                extended_consumer_instance.process_incoming_message('public.users', insert_payload.payload)

            mock_logger.assert_called_once()


# Test process_incoming_message method for update payload
def test_update_process_incoming_message(extended_consumer_instance: ExtendedConsumer, update_payload, mocked_schema):
    with mock.patch('psycopg2.pool.SimpleConnectionPool') as MockConnectionPool:
        extended_consumer_instance.conn_pool = MockConnectionPool.return_value
        extended_consumer_instance.conn_pool.getconn.return_value = mock.MagicMock()
        extended_consumer_instance.conn_pool.getconn.return_value.cursor.return_value.fetchall.return_value = mocked_schema

        extended_consumer_instance.process_incoming_message('public.users', update_payload.payload)

        assert extended_consumer_instance.name == 'Test is successful'


# Test process_incoming_message method for delete payload
def test_delete_process_incoming_message(extended_consumer_instance: ExtendedConsumer, delete_payload, mocked_schema):
    with mock.patch('psycopg2.pool.SimpleConnectionPool') as MockConnectionPool:
        extended_consumer_instance.conn_pool = MockConnectionPool.return_value
        extended_consumer_instance.conn_pool.getconn.return_value = mock.MagicMock()
        extended_consumer_instance.conn_pool.getconn.return_value.cursor.return_value.fetchall.return_value = mocked_schema

        extended_consumer_instance.process_incoming_message('public.users', delete_payload.payload)

        assert extended_consumer_instance.name == 'Test is successful'


# Test simple consumer instance with perform_action method not implemented
def extended_consumer_instance(consumer_instance: Consumer):

    with pytest.raises(NotImplementedError) as excinfo:
        consumer_instance.perform_action('I', {})

    assert 'You must implement the perform_action method in your consumer class.' in str(excinfo.value)


# Test __create_replication_slot method
def test_terminate(extended_consumer_instance: ExtendedConsumer):
    mock_cursor = mock.MagicMock()
    mock_cursor.execute = mock.MagicMock()

    mock_conn = mock.MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with mock.patch('psycopg2.connect', return_value=mock_conn):
        extended_consumer_instance.conn = mock_conn
        extended_consumer_instance.cur = mock_cursor
        mock_cursor.execute.side_effect = psycopg2.errors.DuplicateObject

        with mock.patch('pg_streamline.producer.process.logger.debug'):
            with (mock.patch('sys.exit')) as mock_exit:
                extended_consumer_instance._Consumer__terminate(1, 2)

            mock_exit.assert_called_once()


# Test perform_action method
def test_perform_action(consumer_instance: Consumer):
    with pytest.raises(NotImplementedError) as excinfo:
        consumer_instance.perform_action(
            message_type='I',
            parsed_message={}
        )
    assert 'You must implement the perform_action method in your consumer class.' in str(excinfo.value)


# Test perform_termination method
def test_perform_termination(consumer_instance: Consumer):
    with pytest.raises(NotImplementedError) as excinfo:
        consumer_instance.perform_termination()
    assert 'You must implement the perform_termination method in your consumer class' in str(excinfo.value)


# Test __validate_config method
def test_validate_config(consumer_instance: Consumer):
    config = {}

    with pytest.raises(ConnectionError) as excinfo:
        consumer_instance._Consumer__validate_config(config)

    assert 'Database configuration not found in config file.' in str(excinfo.value)

    config = {'database': {}}

    with pytest.raises(ConnectionError) as excinfo:
        consumer_instance._Consumer__validate_config(config)

    assert 'Database name not found in config file.' in str(excinfo.value)
