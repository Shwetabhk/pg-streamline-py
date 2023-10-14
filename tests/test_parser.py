import pytest
from unittest import mock

from pg_streamline import (
    InsertMessage,
    UpdateMessage,
    DeleteMessage
)
from pg_streamline.parser.base import BaseMessage


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


# Test BaseMessage for decode_tuple for 'u' and 'n' types
def test_decode_tuple_null_and_unchanged():
     with mock.patch('psycopg2.connect') as mock_conn:
        mock_con = mock_conn.return_value
        mock_cur = mock_con.cursor.return_value

        base_message_instance = BaseMessage(b'123', cursor=mock_cur)
        # Replace with actual class instantiation if needed
        base_message_instance.schema = {'columns': [{'name': 'col1'}, {'name': 'col2'}]}

        # Mock the methods
        base_message_instance.read_int16 = mock.MagicMock(return_value=2)
        base_message_instance.read_string = mock.MagicMock(side_effect=['n', 'u'])
        base_message_instance.read_int32 = mock.MagicMock()  # This won't be called

        # Call the method
        result = base_message_instance.decode_tuple()

        # Assertions
        assert result == {'col1': None, 'col2': None}
