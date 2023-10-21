import logging
import os
import re
from typing import Union

import yaml


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


def parse_yaml_config(config_file_path: str) -> dict:
    """
    Parse YAML Configuration File

    This function parses a YAML configuration file and returns a dictionary with the parsed values.

    Args:
        file_path (str): The path to the YAML configuration file.

    Returns:
        dict: A dictionary with the parsed values.
    """
    if not config_file_path:
        config_file_path = 'pg-streamline-config.yaml'
    
    if not os.path.exists(config_file_path):
        raise FileNotFoundError(f'Configuration file {config_file_path} does not exist.')

    def replace_env_var(match):
        """
        Replace Environment Variable
        This function replaces the environment variable with its value.

        Args:
            match (re.Match): The regex match object.
        """
        var_name = match.group(1)
        return os.environ.get(var_name, '')

    with open(config_file_path, 'r') as f:
        content = re.sub(r'\${(\w+)}', replace_env_var, f.read())
        config = yaml.safe_load(content)

    return config
