import json
import logging
from pgoutput_events import Producer  # Importing the Producer class from the producer module


# Setting up basic logging configuration
logging.basicConfig(level=logging.INFO)


class EventProducer(Producer):
    """
    EventProducer class that extends the Producer class to handle specific types of messages.
    """

    def perform_action(self, message_type: str, parsed_message: dict):
        """
        Overriding the perform_action method to handle different types of messages.

        :param message_type: The type of message ('I', 'U', or 'D').
        :param parsed_message: The parsed message as a dictionary.
        """
        logging.debug(f'Message type: {message_type}')
        
        if message_type == 'I':
            logging.info(f'INSERT Message: {json.dumps(parsed_message, indent=4)}')
        
        elif message_type == 'U':
            logging.info(f'UPDATE Message: {json.dumps(parsed_message, indent=4)}')
        
        elif message_type == 'D':
            logging.info(f'DELETE Message: {json.dumps(parsed_message, indent=4)}')


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

    # Creating an instance of EventProducer with a pool size of 5
    producer = EventProducer(pool_size=5, **params)

    # Starting the replication process
    producer.start_replication(publication_names=['events'], protocol_version='4')
