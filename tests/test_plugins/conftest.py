import pytest
from unittest import mock
from pg_streamline.plugins.rabbitmq import RabbitMQProducer, RabbitMQConsumer


# Fixture for creating an instance of RabbitMQProducer
@pytest.fixture
def rabbitmq_producer_instance():
    with mock.patch('pika.BlockingConnection'):
        with mock.patch('psycopg2.connect'):
            return RabbitMQProducer()


# Fixture for creating an instance of RabbitMQConsumer
@pytest.fixture
def rabbitmq_consumer_instance():
    with mock.patch('pika.BlockingConnection'):
        with mock.patch('psycopg2.connect'):
            return RabbitMQConsumer()
