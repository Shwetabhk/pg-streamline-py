import logging

import pytest
from unittest.mock import patch

from pgoutput_events import Producer



# Custom Producer class that implements the perform_action method
class EventProducer(Producer):
    def perform_action(self, message_type: str, parsed_message: dict):
        logging.debug(f'EventProducer - Parsed message: {parsed_message}')
        logging.debug(f'EventProducer - Message type: {message_type}')
        self.name = 'Test is successful'


# Fixture for creating an instance of EventProducer
@pytest.fixture
def event_producer_instance():
    with patch('psycopg2.connect'):
        params = {
            'dbname': 'test_db',
            'user': 'test_user',
            'password': 'test_password',
            'host': 'localhost',
            'port': '5432',
            'replication_slot': 'test_slot'
        }
        return EventProducer(pool_size=5, **params)

# Fixture for creating an instance of Producer
@pytest.fixture
def producer_instance():
    with patch('psycopg2.connect'):
        params = {
            'dbname': 'test_db',
            'user': 'test_user',
            'password': 'test_password',
            'host': 'localhost',
            'port': '5432',
            'replication_slot': 'test_slot'
        }
        return Producer(pool_size=5, **params)

# Fixture for mocking the schema
@pytest.fixture
def mocked_schema():
    return [
        ('id', 'uuid'),
        ('full_name', 'text'),
        ('email', 'text'),
        ('password', 'text'),
        ('is_verified', 'boolean'),
        ('created_at', 'timestamp'),
        ('updated_at', 'timestamp')
    ]

# Class to hold payload data
class OutputData:
    payload = None
    data_start = 124122

# Fixture for insert payload
@pytest.fixture
def insert_payload():
    data = OutputData()
    data.payload = b'I\x00\x00@9N\x00\x07t\x00\x00\x00$2ea2efd6-f0f1-4091-bce2-40dcdb8d2c5et\x00\x00\x00\x06Zapzapt\x00\x00\x00\x16johnboss2002@dummy.comt\x00\x00\x00\x11great_pass_authort\x00\x00\x00\x01tt\x00\x00\x00\x1a2023-10-09 13:13:47.929773t\x00\x00\x00\x1a2023-10-09 13:13:47.929773'
    return data

# Fixture for expected insert response
@pytest.fixture
def insert_response():
    return {
        'message_type': 'I',
        'relation_id': 16441,
        'new': {
            'id': '2ea2efd6-f0f1-4091-bce2-40dcdb8d2c5e',
            'full_name': 'Zapzap',
            'email': 'johnboss2002@dummy.com',
            'password': 'great_pass_author',
            'is_verified': 't',
            'created_at': '2023-10-09 13:13:47.929773',
            'updated_at': '2023-10-09 13:13:47.929773'
        }
    }

# Fixture for update payload
@pytest.fixture
def update_payload():
    data = OutputData()
    data.payload = b'U\x00\x00@9O\x00\x07t\x00\x00\x00$2ea2efd6-f0f1-4091-bce2-40dcdb8d2c5et\x00\x00\x00\x06Zapzapt\x00\x00\x00\x16johnboss2002@dummy.comt\x00\x00\x00\x11great_pass_authort\x00\x00\x00\x01tt\x00\x00\x00\x1a2023-10-09 13:13:47.929773t\x00\x00\x00\x1a2023-10-09 13:13:47.929773N\x00\x07t\x00\x00\x00$2ea2efd6-f0f1-4091-bce2-40dcdb8d2c5et\x00\x00\x00\x06Zapzapt\x00\x00\x00\x0bssx@xyz.comt\x00\x00\x00\x11great_pass_authort\x00\x00\x00\x01tt\x00\x00\x00\x1a2023-10-09 13:13:47.929773t\x00\x00\x00\x1a2023-10-09 13:13:47.929773'
    return data

# Fixture for expected update response
@pytest.fixture
def update_response():
    return  {
        'message_type': 'U',
        'relation_id': 16441,
        'old': {
            'id': '2ea2efd6-f0f1-4091-bce2-40dcdb8d2c5e',
            'full_name': 'Zapzap',
            'email': 'johnboss2002@dummy.com',
            'password': 'great_pass_author',
            'is_verified': 't',
            'created_at': '2023-10-09 13:13:47.929773',
            'updated_at': '2023-10-09 13:13:47.929773'
        },
        'new': {
            'id': '2ea2efd6-f0f1-4091-bce2-40dcdb8d2c5e',
            'full_name': 'Zapzap',
            'email': 'ssx@xyz.com',
            'password': 'great_pass_author',
            'is_verified': 't',
            'created_at': '2023-10-09 13:13:47.929773',
            'updated_at': '2023-10-09 13:13:47.929773'
        },
        'diff': {
            'email': {
                'old_value': 'johnboss2002@dummy.com',
                'new_value': 'ssx@xyz.com'
            }
        }
    }

# Fixture for delete payload
@pytest.fixture
def delete_payload():
    data = OutputData()
    data.payload = b'D\x00\x00@9O\x00\x07t\x00\x00\x00$2ea2efd6-f0f1-4091-bce2-40dcdb8d2c5et\x00\x00\x00\x06Zapzapt\x00\x00\x00\x0bssx@xyz.comt\x00\x00\x00\x11great_pass_authort\x00\x00\x00\x01tt\x00\x00\x00\x1a2023-10-09 13:13:47.929773t\x00\x00\x00\x1a2023-10-09 13:13:47.929773'
    return data

# Fixture for expected delete response
@pytest.fixture
def delete_response():
    return {
        'message_type': 'D',
        'relation_id': 16441,
        'old': {
            'id': '2ea2efd6-f0f1-4091-bce2-40dcdb8d2c5e',
            'full_name': 'Zapzap',
            'email': 'ssx@xyz.com',
            'password': 'great_pass_author',
            'is_verified': 't',
            'created_at': '2023-10-09 13:13:47.929773',
            'updated_at': '2023-10-09 13:13:47.929773'
        }
    }