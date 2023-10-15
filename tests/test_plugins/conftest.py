import pytest
from unittest import mock
from pg_streamline.plugins.rabbitmq import RabbitMQProducer, RabbitMQConsumer


# Fixture for creating an instance of RabbitMQProducer
@pytest.fixture
def rabbitmq_producer_instance():
    with mock.patch('pika.BlockingConnection'):
        with mock.patch('psycopg2.connect'):
            params = {
                'dbname': 'test_db',
                'user': 'test_user',
                'password': 'test_password',
                'host': 'localhost',
                'port': '5432',
                'replication_slot': 'test_slot'
            }
            return RabbitMQProducer(rabbitmq_url='amqp://localhost', rabbitmq_exchange='pg-exchage', pool_size=5, **params)


# Fixture for creating an instance of RabbitMQConsumer
@pytest.fixture
def rabbitmq_consumer_instance():
    with mock.patch('pika.BlockingConnection'):
        with mock.patch('psycopg2.connect'):
            params = {
                'dbname': 'test_db',
                'user': 'test_user',
                'password': 'test_password',
                'host': 'localhost',
                'port': '5432',
                'replication_slot': 'test_slot'
            }
            return RabbitMQConsumer(
                rabbitmq_url='amqp://localhost',
                rabbitmq_exchange='pg-exchange',
                routing_keys='table1,table2',
                queue='test_queue',
                pool_size=5,
                **params
            )
