import json
import logging
from pg_streamline import Producer  # Importing the Producer class from the producer module


# Setting up basic logging configuration
logging.basicConfig(level=logging.INFO)


class PGOutputPGOutputProducer(Producer):
    """
    PGOutputProducer class that extends the Producer class to handle specific types of messages.
    """

    def perform_action(self, table_name: str, data: dict):
        """
        Overriding the perform_action method to handle different types of messages.

        :param message_type: The type of message ('I', 'U', or 'D').
        :param data: The data to process.
        """
        logging.info(f'Table name: {table_name}')
        logging.info(f'Data: {data}')

if __name__ == '__main__':
    # Database and replication parameters
    params = {
        'dbname': 'dummy',
        'user': 'postgres',
        'password': 'postgres',
        'host': 'localhost',
        'port': '5432',
        'replication_slot': 'pgtest'
    }

    # Creating an instance of PGOutputProducer with a pool size of 5
    producer = PGOutputPGOutputProducer(pool_size=5, **params)

    # Starting the replication process
    producer.start_replication(publication_names=['events'], protocol_version='4')
