import logging
import json
import pika
from pg_streamline import Consumer


# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RabbitMQConsumer(Consumer):
    """
    RabbitMQConsumer Class

    This class extends the base Consumer class from the pg_streamline package.
    It initializes a RabbitMQ consumer that listens to a specific queue and processes incoming messages.

    Attributes:
        rabbitmq_url (str): The URL for the RabbitMQ broker.
        routing_keys (str): Comma-separated list of routing keys to bind to the queue.
        queue (str): The name of the RabbitMQ queue to consume messages from.
        exchange (str): The name of the RabbitMQ exchange to bind to.
    """

    def __init__(self, rabbitmq_url: str, rabbitmq_exchange: str, routing_keys: str, queue: str, *args, **kwargs):
        """
        Initialize the RabbitMQConsumer.

        Args:
            rabbitmq_url (str): The URL for the RabbitMQ broker.
            routing_keys (str): Comma-separated list of routing keys to bind to the queue.
            queue (str): The name of the RabbitMQ queue to consume messages from.
            exchange (str): The name of the RabbitMQ exchange to bind to.
        """
        super().__init__(*args, **kwargs)
        self.connection = pika.BlockingConnection(pika.URLParameters(rabbitmq_url))
        self.channel = self.connection.channel()

        # Declare the queue
        self.channel.queue_declare(queue=queue, durable=True, exclusive=False, auto_delete=False)

        # Declare the exchange and bind the queue to it with specified routing keys
        self.channel.exchange_declare(exchange=rabbitmq_exchange, exchange_type='topic', durable=True)
        for routing_key in routing_keys.split(','):
            self.channel.queue_bind(exchange=rabbitmq_exchange, queue=queue, routing_key=routing_key.strip())

    def callback(self, channel, method, properties, body):
        """
        Callback function to process incoming messages.

        Args:
            channel: The channel object.
            method: The method frame.
            properties: The properties.
            body: The message body.
        """
        try:
            self.process_incoming_message(method.routing_key, body)
            channel.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledge message
        except Exception as e:
            logger.error(f"An error occurred: {e}")
            channel.basic_reject(delivery_tag=method.delivery_tag, requeue=True)  # Reject message

    def perform_action(self, message_type: str, parsed_message: dict):
        """
        Perform action based on the incoming message.

        Args:
            message_type (str): The type of the message (Insert, Update, Delete).
            parsed_message (dict): The parsed message content.
        """
        logger.info(f'Performing action with message: {message_type}')
        logger.info(json.dumps(parsed_message, indent=4))

    def perform_termination(self):
        """
        Close the RabbitMQ connection.

        This method is called to gracefully close the RabbitMQ connection and channel.
        """
        logger.info('Closing connection to RabbitMQ')
        self.channel.close()
        self.connection.close()

    def run_consumer(self):
        """
        Start the RabbitMQ consumer.

        This method starts consuming messages from the RabbitMQ queue and calls the callback function
        for each incoming message.
        """
        self.channel.basic_consume(queue='pgtest', on_message_callback=self.callback, auto_ack=False)
        logger.info('RabbitMQConsumer is running...')
        self.channel.start_consuming()
