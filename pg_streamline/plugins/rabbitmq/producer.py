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

    def __init__(self, rabbitmq_url: str, rabbitmq_exchange: str, *args, **kwargs):
        """
        Initialize the RabbitMQProducer.

        Args:
            rabbitmq_url (str): The URL for the RabbitMQ broker.
        """
        super().__init__(*args, **kwargs)
        self.connection = pika.BlockingConnection(pika.URLParameters(rabbitmq_url))
        self.channel = self.connection.channel()

        # Declare a direct exchange
        self.channel.exchange_declare(exchange=rabbitmq_exchange, exchange_type='topic', durable=True)

    def perform_action(self, table_name: str, bytes_string: dict):
        """
        Publish a message to the RabbitMQ exchange.

        Args:
            table_name (str): The table name that the message pertains to.
            bytes_string (dict): The message content.
        """
        logging.info(f'Table name: {table_name}, Bytes String: {bytes_string}')

        self.channel.basic_publish(
            exchange='table_exchange',
            routing_key=table_name,
            body=bytes_string
        )

    def perform_termination(self):
        """
        Close the RabbitMQ connection.

        This method is called to gracefully close the RabbitMQ connection and channel.
        """
        logging.info('Closing connection to RabbitMQ')
        self.channel.close()
        self.connection.close()
