import logging
import json
import pika
from pg_streamline import Consumer


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RabbitMQConsumerPlugin(Consumer):
    def __init__(self, rabbitmq_url: str, routing_keys: str, queue: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.connection = pika.BlockingConnection(pika.URLParameters(rabbitmq_url))
        self.channel = self.connection.channel()

        # Declare the queue
        self.channel.queue_declare(queue=queue, durable=True)

        # Declare the exchange and bind the queue to it with specified routing keys
        self.channel.exchange_declare(exchange='table_exchange', exchange_type='direct')
        for routing_key in routing_keys.split(','):
            self.channel.queue_bind(exchange='table_exchange', queue='pgtest', routing_key=routing_key.strip())

    def callback(self, ch, method, properties, body):
        self.process_incoming_message(method.routing_key, body)

    def perform_action(self, message_type: str, parsed_message: dict):
        logger.info(f'Performing action with message: {message_type}')
        logger.info(json.dumps(parsed_message, indent=4))

    def run_consumer(self):
        self.channel.basic_consume(queue='pgtest', on_message_callback=self.callback, auto_ack=True)
        logger.info('RabbitMQConsumerPlugin is running...')
        self.channel.start_consuming()


if __name__ == '__main__':
    # Database and consumer parameters
    db_params = {
        'dbname': 'dummy',
        'user': 'postgres',
        'password': 'postgres',
        'host': 'localhost',
        'port': '5432'
    }

    # RabbitMQ URL
    rabbitmq_url = 'amqp://localhost'

    # Routing keys
    routing_keys = 'public.pgbench_accounts,public.pgbench_branches,public.pgbench_history,public.pgbench_tellers'

    # Create an instance of RabbitMQConsumerPlugin with a pool size of 5
    consumer = RabbitMQConsumerPlugin(
        rabbitmq_url=rabbitmq_url,
        routing_keys=routing_keys,
        queue='pgtest',
        pool_size=5,
        **db_params
    )

    # Start the consumer
    consumer.run_consumer()
