from producer.process import Producer
import logging

class EventProducer(Producer):
    def perform_action(self, message_type: str, parsed_message: dict):
        logging.debug(f'EventProducer - Message type: {message_type}')
        logging.debug(f'EventProducer - Parsed message: {parsed_message}')


import pytest
from unittest.mock import patch


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
