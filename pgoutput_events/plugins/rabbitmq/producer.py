import logging
import pika
from pgoutput_events import Producer


logging.basicConfig(level=logging.INFO)


class RabbitMQProducerPlugin(Producer):
    def __init__(self, rabbitmq_url: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.connection = pika.BlockingConnection(pika.URLParameters(rabbitmq_url))
        self.channel = self.connection.channel()

        # Declare a direct exchange
        self.channel.exchange_declare(exchange='table_exchange', exchange_type='direct')

    def perform_action(self, table_name: str, bytes_string: dict):
        logging.info(f'Table name: {table_name}, Bytes String: {bytes_string}')

        self.channel.basic_publish(
            exchange='table_exchange',
            routing_key=table_name,
            body=bytes_string
        )

    def perform_termination(self):
        logging.info('Closing connection to RabbitMQ')
        self.channel.close()
        self.connection.close()

if __name__ == '__main__':
    # Database and replication parameters
    db_params = {
        'dbname': 'dummy',
        'user': 'postgres',
        'password': 'postgres',
        'host': 'localhost',
        'port': '5432',
        'replication_slot': 'pgtest'
    }

    rabbitmq_url = 'amqp://localhost'

    producer = RabbitMQProducerPlugin(rabbitmq_url=rabbitmq_url, pool_size=5, **db_params)
    producer.start_replication(publication_names=['events'], protocol_version='4')
