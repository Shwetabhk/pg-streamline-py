from unittest import mock

import pytest
from pg_streamline.plugins.rabbitmq import RabbitMQProducer, RabbitMQConsumer

# Test Producer class
def test_producer_perform_action(rabbitmq_producer_instance: RabbitMQProducer):
    mock_connection = rabbitmq_producer_instance.connection
    mock_channel = mock_connection.channel.return_value
    mock_basic_publish = mock_channel.basic_publish

    rabbitmq_producer_instance.perform_action('test_table', b'test_data')
    
    mock_basic_publish.assert_called_once()


def test_producer_perform_termination(rabbitmq_producer_instance: RabbitMQProducer):
    mock_connection = rabbitmq_producer_instance.connection
    mock_channel = mock_connection.channel.return_value
    mock_channel.close = mock.MagicMock()
    mock_connection.close = mock.MagicMock()

    rabbitmq_producer_instance.perform_termination()

    mock_channel.close.assert_called_once()
    mock_connection.close.assert_called_once()


def test_producer_validate_config(rabbitmq_producer_instance: RabbitMQProducer):
    rabbitmq_producer_instance.config = {}

    with pytest.raises(ConnectionError) as excinfo:
        rabbitmq_producer_instance._RabbitMQProducer__validate_config()

    assert 'rabbitmq is missing from the configuration file.' in str(excinfo.value)

    rabbitmq_producer_instance.config = {'rabbitmq': {}}

    with pytest.raises(ConnectionError) as excinfo:
        rabbitmq_producer_instance._RabbitMQProducer__validate_config()

    assert 'url is missing from the configuration file.' in str(excinfo.value)


# Test Consumer class

def test_consumer_run_consumer(rabbitmq_consumer_instance: RabbitMQConsumer):
    mock_connection = rabbitmq_consumer_instance.connection
    mock_channel = mock_connection.channel.return_value
    mock_basic_consume = mock_channel.basic_consume
    mock_basic_consume.return_value = 'test_consumer'

    rabbitmq_consumer_instance.run_consumer()

    mock_basic_consume.assert_called_once()


def test_consumer_perform_action(rabbitmq_consumer_instance: RabbitMQConsumer):
    with mock.patch('logging.Logger.info') as mock_logger:
        rabbitmq_consumer_instance.perform_action('I', {'test': 'test'})

    mock_logger.assert_called()


def test_consumer_perform_termination(rabbitmq_consumer_instance: RabbitMQConsumer):
    mock_connection = rabbitmq_consumer_instance.connection
    mock_channel = mock_connection.channel.return_value
    mock_channel.close = mock.MagicMock()
    mock_connection.close = mock.MagicMock()

    rabbitmq_consumer_instance.perform_termination()

    mock_channel.close.assert_called_once()
    mock_connection.close.assert_called_once()


def test_consumer_callback(rabbitmq_consumer_instance):
    mock_method = mock.MagicMock()
    mock_method.routing_key = 'test_routing_key'
    mock_method.delivery_tag = 'some_tag'
    mock_body = b'test_body'

    mock_channel = mock.MagicMock()
    mock_channel.basic_ack = mock.MagicMock()
    mock_channel.basic_reject = mock.MagicMock()

    with mock.patch.object(rabbitmq_consumer_instance, 'process_incoming_message', autospec=True) as mock_process_incoming_message:
        rabbitmq_consumer_instance.callback(mock_channel, mock_method, None, mock_body)

        mock_process_incoming_message.assert_called_once_with('test_routing_key', mock_body)

        mock_channel.basic_ack.assert_called_once_with(delivery_tag='some_tag')

        mock_channel.basic_reject.assert_not_called()
    
        mock_process_incoming_message.side_effect = Exception()

        rabbitmq_consumer_instance.callback(mock_channel, mock_method, None, mock_body)

        mock_channel.basic_reject.assert_called_once_with(delivery_tag='some_tag', requeue=True)


def test_consumer_validate_config(rabbitmq_consumer_instance):
    rabbitmq_consumer_instance.config = {}

    with pytest.raises(ConnectionError) as excinfo:
        rabbitmq_consumer_instance._RabbitMQConsumer__validate_config()

    assert 'rabbitmq is missing from the configuration file.' in str(excinfo.value)

    rabbitmq_consumer_instance.config = {'rabbitmq': {}}

    with pytest.raises(ConnectionError) as excinfo:
        rabbitmq_consumer_instance._RabbitMQConsumer__validate_config()

    assert 'url is missing from the configuration file.' in str(excinfo.value)
