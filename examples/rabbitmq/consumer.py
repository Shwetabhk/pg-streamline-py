import json
import logging
import os
from pg_streamline.plugins.rabbitmq import RabbitMQConsumer


class MyConsumer(RabbitMQConsumer):
    """
    MyConsumer Class

    This class extends the RabbitMQConsumer to override the perform_action method.
    You can plug in your own logic here to handle incoming messages.

    Methods:
        perform_action(message_type: str, parsed_message: dict): Overrides the perform_action
            method from RabbitMQConsumer to handle incoming messages.
    """

    def perform_action(self, message_type: str, parsed_message: dict):
        """
        Perform Action on Incoming Message

        This method is called whenever a new message arrives. It logs the message type
        and the parsed message.

        Args:
            message_type (str): The type of the incoming message. Could be 'I' for Insert,
                'U' for Update, or 'D' for Delete.
            parsed_message (dict): The parsed message as a dictionary.

        """
        logging.info(f'Performing action with message: {message_type}')
        logging.info(json.dumps(parsed_message, indent=4))


if __name__ == '__main__':
    """
    Main Execution Point

    This block initializes the consumer with database and RabbitMQ parameters,
    and then starts the consumer.
    """
    os.environ.setdefault('DB_PASSWORD', 'postgres')

    # Create an instance of MyConsumer with a pool size of 5
    consumer = MyConsumer('pg-streamline-config.yaml')

    # Start the consumer
    consumer.run_consumer()
