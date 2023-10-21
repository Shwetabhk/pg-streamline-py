import logging
import pika
from pg_streamline import Producer


# Initialize logging
logging.basicConfig(level=logging.INFO)


class RabbitMQProducer(Producer):
    """
    RabbitMQProducer Class

    This class extends the base Producer class from the pg_streamline package.
    It initializes a RabbitMQ producer that publishes messages to a specific exchange.

    Attributes:
        rabbitmq_url (str): The URL for the RabbitMQ broker.
    """

    def __init__(self, config_path: str = None):
        """
        Initialize the RabbitMQProducer.

        Args:
            config_path (str): The path to the configuration file.
        """
        super().__init__(config_path=config_path)

        self.__validate_config()

        self.connection = pika.BlockingConnection(pika.URLParameters(self.config['rabbitmq']['url']))
        self.channel = self.connection.channel()
        self.rabbitmq_exchange = self.config['rabbitmq']['exchange']

        # Declare a topic exchange
        self.channel.exchange_declare(exchange=self.rabbitmq_exchange, exchange_type='topic', durable=True)

    def __validate_config(self):
        """
        Validate the configuration file.
        """
        # Define required keys for RabbitMQ configuration
        required_rabbitmq_keys = ['url', 'exchange']

        # Check if 'rabbitmq' key exists in config
        if 'rabbitmq' not in self.config:
            raise ConnectionError('rabbitmq is missing from the configuration file.')

        rabbitmq_config = self.config['rabbitmq']

        # Validate required keys for RabbitMQ configuration
        for key in required_rabbitmq_keys:
            if key not in rabbitmq_config:
                raise ConnectionError(f'{key} is missing from the configuration file.')

    def perform_action(self, table_name: str, bytes_string: dict):
        """
        Publish a message to the RabbitMQ exchange.

        Args:
            table_name (str): The table name that the message pertains to.
            bytes_string (dict): The message content.
        """
        logging.info(f'Table name: {table_name}, Bytes String: {bytes_string}')

        self.channel.basic_publish(
            exchange=self.rabbitmq_exchange,
            routing_key=table_name,
            body=bytes_string,
            properties=pika.BasicProperties(delivery_mode=2)
        )

    def perform_termination(self):
        """
        Close the RabbitMQ connection.

        This method is called to gracefully close the RabbitMQ connection and channel.
        """
        logging.info('Closing connection to RabbitMQ')
        self.channel.close()
        self.connection.close()
