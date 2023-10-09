from unittest import mock

import pytest

from .conftest import EventProducer
from producer import Producer


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
        assert producer.conn is not None
        assert producer.cur is not None
        assert producer.replication_slot == 'pgtest'

def test_start_replication(event_producer_instance: EventProducer):
    mock_cursor = mock.MagicMock()
    mock_cursor.start_replication = mock.MagicMock()
    mock_cursor.consume_stream = mock.MagicMock()

    mock_conn = mock.MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with mock.patch('psycopg2.connect', return_value=mock_conn):
        event_producer_instance.conn = mock_conn  # Set the mock connection to your test_producer instance
        event_producer_instance.cur = mock_cursor  # Set the mock cursor to your test_producer instance
        event_producer_instance.start_replication(publication_names=['events'], protocol_version='4')

    mock_cursor.start_replication.assert_called_once()

def test_perform_action(producer_instance: Producer):
    with pytest.raises(NotImplementedError) as excinfo:
        producer_instance.perform_action(
            message_type='I',
            parsed_message={'message_type': 'I', 'relation_id': 1, 'new': {'id': 1, 'name': 'test'}}
        )
    assert 'This method should be overridden by subclass' in str(excinfo.value)
