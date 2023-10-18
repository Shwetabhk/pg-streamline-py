import os
from pg_streamline.plugins.rabbitmq import RabbitMQProducer

if __name__ == '__main__':
    """
    Main Execution Point

    This script initializes a RabbitMQ producer with database and RabbitMQ parameters,
    and then starts the replication process.
    """
    os.environ.setdefault('DB_PASSWORD', 'postgres')

    producer = RabbitMQProducer('pg-streamline-config.yaml')

    # Start Replication
    # -----------------
    # Start the replication process with the specified publication names and protocol version.
    # The producer will start listening for changes in the database and publish them to RabbitMQ.
    producer.start_replication(publication_names=['events'], protocol_version='4')
