import logging
from typing import Union


class Utils:
    @staticmethod
    def convert_bytes_to_int(in_bytes: bytes) -> int:
        return int.from_bytes(in_bytes, byteorder='big', signed=True)

    @staticmethod
    def convert_bytes_to_utf8(in_bytes: Union[bytes, bytearray]) -> str:
        return in_bytes.decode('utf-8')


def setup_custom_logging():
    """
    Configures the root logger's handlers to use a custom logging format.

    This function iterates through all handlers of the root logger and sets their
    formatter to use a custom format string. The format string includes the timestamp,
    logger name, log level, and the log message.

    Format:
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    Example:
        '2021-10-08 12:34:56,789 - root - INFO - This is an info message'

    Note:
        This function assumes that the root logger has already been configured with handlers.
    """
    format_str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(format_str)

    for handler in logging.root.handlers:
        handler.setFormatter(formatter)
