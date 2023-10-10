import pytest
from unittest import mock

from pgoutput_events import (
    InsertMessage,
    UpdateMessage,
    DeleteMessage
)
from pgoutput_events.types.base import BaseMessage


# Test InsertMessage decoding
def test_insert(insert_payload, insert_response, mocked_schema):
    with mock.patch('psycopg2.connect') as mock_conn:
        mock_con = mock_conn.return_value
        mock_cur = mock_con.cursor.return_value
        mock_cur.fetchall.return_value = mocked_schema

        parser = InsertMessage(insert_payload.payload, cursor=mock_cur)
        parsed_message = parser.decode_insert_message()

        assert parsed_message == insert_response


# Test UpdateMessage decoding
def test_update(update_payload, update_response, mocked_schema):
    with mock.patch('psycopg2.connect') as mock_conn:
        mock_con = mock_conn.return_value
        mock_cur = mock_con.cursor.return_value
        mock_cur.fetchall.return_value = mocked_schema

        parser = UpdateMessage(update_payload.payload, cursor=mock_cur)
        parsed_message = parser.decode_update_message()

        assert parsed_message == update_response


# Test DeleteMessage decoding
def test_delete(delete_payload, delete_response, mocked_schema):
    with mock.patch('psycopg2.connect') as mock_conn:
        mock_con = mock_conn.return_value
        mock_cur = mock_con.cursor.return_value
        mock_cur.fetchall.return_value = mocked_schema

        parser = DeleteMessage(delete_payload.payload, cursor=mock_cur)
        parsed_message = parser.decode_delete_message()

        assert parsed_message == delete_response


# Test BaseMessage for NotImplementedError
def test_base_not_implemented_methods():
    with mock.patch('psycopg2.connect') as mock_conn:
        mock_con = mock_conn.return_value
        mock_cur = mock_con.cursor.return_value

        parser = BaseMessage(b'123', cursor=mock_cur)

        with pytest.raises(NotImplementedError) as excinfo:
            parser.decode_insert_message()
        assert 'This method should be overridden by subclass' in str(excinfo.value)

        with pytest.raises(NotImplementedError) as excinfo:
            parser.decode_update_message()
        assert 'This method should be overridden by subclass' in str(excinfo.value)

        with pytest.raises(NotImplementedError) as excinfo:
            parser.decode_delete_message()
        assert 'This method should be overridden by subclass' in str(excinfo.value)
