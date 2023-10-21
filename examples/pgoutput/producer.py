import json
import logging
import os
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
    os.environ.setdefault('DB_PASSWORD', 'postgres')

    config_file_path = 'examples/pgoutput/config.yaml'

    # Creating an instance of PGOutputProducer with a pool size of 5
    producer = PGOutputPGOutputProducer(config_path=config_file_path)

    # Starting the replication process
    producer.start_replication(publication_names=['events'], protocol_version='4')
