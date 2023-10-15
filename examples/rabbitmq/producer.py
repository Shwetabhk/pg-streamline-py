from pg_streamline.plugins.rabbitmq import RabbitMQProducer

if __name__ == '__main__':
    """
    Main Execution Point

    This script initializes a RabbitMQ producer with database and RabbitMQ parameters,
    and then starts the replication process.
    """

    # Database and replication parameters
    # -----------------------------------
    # dbname: The name of the database to connect to.
    # user: The username to connect with.
    # password: The password for the user.
    # host: The hostname of the machine where the database is running.
    # port: The port number on which the database is listening.
    # replication_slot: The name of the replication slot to use.
    db_params = {
        'dbname': 'dummy',
        'user': 'postgres',
        'password': 'postgres',
        'host': 'localhost',
        'port': '5432',
        'replication_slot': 'pgtest'
    }

    # RabbitMQ URL
    # ------------
    # The URL to connect to the RabbitMQ broker.
    rabbitmq_url = 'amqp://localhost'

    # Initialize RabbitMQ Producer
    # ----------------------------
    # Create an instance of RabbitMQProducer with a pool size of 5.
    # The producer will connect to the RabbitMQ broker and the PostgreSQL database
    # with the provided parameters.
    producer = RabbitMQProducer(rabbitmq_url=rabbitmq_url, rabbitmq_exchange='pg-exchage', pool_size=5, **db_params)

    # Start Replication
    # -----------------
    # Start the replication process with the specified publication names and protocol version.
    # The producer will start listening for changes in the database and publish them to RabbitMQ.
    producer.start_replication(publication_names=['events'], protocol_version='4')
