from unittest import mock

import pytest

from pgoutput_events import Consumer
from tests.conftest import TestConsumer


# Test process_incoming_message method for insert payload
def test_insert_process_incoming_message(test_consumer_instance: TestConsumer, insert_payload, mocked_schema):
    with mock.patch('psycopg2.connect') as mocker:
        mock_conn = mocker.return_value

        mock_cur = mock_conn.cursor.return_value

        mock_cur.fetchall.return_value = mocked_schema

        test_consumer_instance.connection = mock_conn

        test_consumer_instance.process_incoming_message('public.users', insert_payload.payload)

        assert test_consumer_instance.name == 'Test is successful'


# Test process_incoming_message method for update payload
def test_update_process_incoming_message(test_consumer_instance: TestConsumer, update_payload, mocked_schema):
    with mock.patch('psycopg2.connect') as mocker:
        mock_conn = mocker.return_value

        mock_cur = mock_conn.cursor.return_value

        mock_cur.fetchall.return_value = mocked_schema

        test_consumer_instance.connection = mock_conn

        test_consumer_instance.process_incoming_message('public.users', update_payload.payload)

        assert test_consumer_instance.name == 'Test is successful'


# Test process_incoming_message method for delete payload
def test_delete_process_incoming_message(test_consumer_instance: TestConsumer, delete_payload, mocked_schema):
    with mock.patch('psycopg2.connect') as mocker:
        mock_conn = mocker.return_value

        mock_cur = mock_conn.cursor.return_value

        mock_cur.fetchall.return_value = mocked_schema

        test_consumer_instance.connection = mock_conn

        test_consumer_instance.process_incoming_message('public.users', delete_payload.payload)

        assert test_consumer_instance.name == 'Test is successful'


# Test simple consumer instance with perform_action method not implemented
def test_consumer_instance(consumer_instance: Consumer):

    with pytest.raises(NotImplementedError) as excinfo:
        consumer_instance.perform_action('I', {})

    assert 'You must implement the perform_action method in your consumer class.' in str(excinfo.value)
