import psycopg2
import pytest
from unittest import mock

from .conftest import PGOutputProducer, Wal2jsonProducer
from pgoutput_events import Producer


# Test initialization of Producer
def test_producer_init():
    with mock.patch('psycopg2.connect'):
        params = {
            'dbname': 'dummy',
            'user': 'postgres',
            'password': 'postgres',
            'host': 'localhost',
            'port': '5432',
            'replication_slot': 'pgtest'
        }
        producer = Producer(pool_size=5, **params)
        assert producer.replication_cursor is not None
        assert producer.replication_slot == 'pgtest'


# Test start_replication method
def test_start_replication(pgo_producer_instance: PGOutputProducer):
    mock_cursor = mock.MagicMock()
    mock_cursor.start_replication = mock.MagicMock()
    mock_cursor.consume_stream = mock.MagicMock()

    mock_conn = mock.MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with mock.patch('psycopg2.connect', return_value=mock_conn):
        pgo_producer_instance.conn = mock_conn
        pgo_producer_instance.replication_cursor = mock_cursor

        pgo_producer_instance.start_replication(publication_names=['events'], protocol_version='4')

    mock_cursor.start_replication.assert_called_once()


# Test perform_action method
def test_perform_action(producer_instance: Producer, update_payload):
    with pytest.raises(NotImplementedError) as excinfo:
        producer_instance.perform_action(
            table_name='public.test',
            bytes_message=update_payload
        )
    assert 'This method should be overridden by subclass' in str(excinfo.value)


# Test __process_pgoutput_change method
def test_process_pgoutput_change(pgo_producer_instance: PGOutputProducer, insert_payload):
    mock_cursor = mock.MagicMock()
    mock_cursor.execute = mock.MagicMock()

    mock_conn = mock.MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with mock.patch('psycopg2.connect', return_value=mock_conn):
        pgo_producer_instance.conn = mock_conn
        pgo_producer_instance.cur = mock_cursor
        mock_cursor.fetchone.return_value = ('public', 'users')

        with mock.patch('pgoutput_events.producer.process.logger.info') as mock_logging:
            pgo_producer_instance._Producer__process_pgoutput_change(insert_payload)

        # assert that mocker is called
        mock_logging.assert_called()

        # Test raise Exception
        mock_cursor.fetchone.return_value = None
        with pytest.raises(Exception) as excinfo:
            pgo_producer_instance._Producer__process_pgoutput_change(insert_payload)
        
        assert 'Failed to process change.' in str(excinfo.value)


def test_process_wal2json_change(wal2json_producer_instance: Wal2jsonProducer, insert_payload):
    with mock.patch('pgoutput_events.producer.process.logger.info') as mock_logging:
        wal2json_producer_instance._Producer__process_wal2json_change(insert_payload)

        assert mock_logging.call_count == 2

        mock_logging.side_effect = Exception('Failed to process change.')

        with pytest.raises(Exception) as excinfo:
            wal2json_producer_instance._Producer__process_wal2json_change(insert_payload)
        
        assert 'Failed to process change.' in str(excinfo.value)


# test __process_changes method
def test_process_changes(pgo_producer_instance: PGOutputProducer, wal2json_producer_instance: Wal2jsonProducer):
    with mock.patch('concurrent.futures.ThreadPoolExecutor.submit') as mock_executor:
        pgo_producer_instance._Producer__process_changes('test')
    mock_executor.assert_called_once()

    with mock.patch('concurrent.futures.ThreadPoolExecutor.submit') as mock_executor:
        wal2json_producer_instance._Producer__process_changes('test')
    mock_executor.assert_called_once()


# Test __create_replication_slot method
def test_create_replication_slot(pgo_producer_instance: PGOutputProducer):
    mock_cursor = mock.MagicMock()
    mock_cursor.execute = mock.MagicMock()

    mock_conn = mock.MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with mock.patch('psycopg2.connect', return_value=mock_conn):
        pgo_producer_instance.conn = mock_conn
        pgo_producer_instance.cur = mock_cursor
        mock_cursor.execute.side_effect = psycopg2.errors.DuplicateObject

        with mock.patch('pgoutput_events.producer.process.logger.debug') as mock_logging:
            pgo_producer_instance._Producer__create_replication_slot('pgtest')

        mock_logging.assert_called_once_with('Replication slot already exists')
    
    # Test OperationError exception
    with mock.patch('psycopg2.connect', return_value=mock_conn):
        pgo_producer_instance.conn = mock_conn
        pgo_producer_instance.cur = mock_cursor
        mock_cursor.execute.side_effect = psycopg2.errors.OperationalError
       
        with pytest.raises(psycopg2.errors.OperationalError) as excinfo:
            pgo_producer_instance._Producer__create_replication_slot('pgtest')

        assert 'Operational error during initialization.' in str(excinfo.value)


# Test __create_replication_slot method
def test_terminate(pgo_producer_instance: PGOutputProducer):
    mock_cursor = mock.MagicMock()
    mock_cursor.execute = mock.MagicMock()

    mock_conn = mock.MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with mock.patch('psycopg2.connect', return_value=mock_conn):
        pgo_producer_instance.conn = mock_conn
        pgo_producer_instance.cur = mock_cursor
        mock_cursor.execute.side_effect = psycopg2.errors.DuplicateObject

        with mock.patch('pgoutput_events.producer.process.logger.debug') as mock_logging:
            with (mock.patch('sys.exit')) as mock_exit:
                pgo_producer_instance._Producer__terminate(1, 2)

            mock_exit.assert_called_once()


# Test __process_single_change method for insert payload
@pytest.mark.skip('Need to fix this test')
def test_insert_process_single_change(pgo_producer_instance: Producer, insert_payload, mocked_schema):
    with mock.patch('psycopg2.connect') as mock_conn:
        mock_con = mock_conn.return_value
        mock_cur = mock_con.cursor.return_value
        mock_cur.fetchall.return_value = mocked_schema

        pgo_producer_instance._Producer__process_single_change(insert_payload)

        assert pgo_producer_instance.name == 'Test is successful'


# Test __process_single_change method for update payload
@pytest.mark.skip('Need to fix this test')
def test_update_process_single_change(producer_instance: Producer, update_payload, mocked_schema):
    with mock.patch('psycopg2.connect') as mock_conn:
        mock_con = mock_conn.return_value
        mock_cur = mock_con.cursor.return_value
        mock_cur.fetchall.return_value = mocked_schema

        with pytest.raises(NotImplementedError) as excinfo:
            producer_instance._Producer__process_single_change(update_payload)

        assert 'This method should be overridden by subclass' in str(excinfo.value)


# Test __process_single_change method for delete payload
@pytest.mark.skip('Need to fix this test')
def test_delete_process_single_change(producer_instance: Producer, delete_payload, mocked_schema):
    with mock.patch('psycopg2.connect') as mock_conn:
        mock_con = mock_conn.return_value
        mock_cur = mock_con.cursor.return_value
        mock_cur.fetchall.return_value = mocked_schema

        with pytest.raises(NotImplementedError) as excinfo:
            producer_instance._Producer__process_single_change(delete_payload)

        assert 'This method should be overridden by subclass' in str(excinfo.value)
