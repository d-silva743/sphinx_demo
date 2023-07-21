"""
This is a workflow test.
"""

from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import sproc, col, min, max, avg
from snowflake.snowpark import functions as F
from snowflake.snowpark.functions import concat, lit, col, lag, lead,lower,trim,upper
from snowflake.snowpark.window import Window
from snowflake.snowpark import types as T
import datetime as dt
import time
from snowflake.snowpark.types import StringType
from pathlib import Path
import configparser
import os

# To import modules from another directory into the current directory
import sys
root_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, root_dir.__str__())


def init_config():

    """
    This function does nothing.

    Args:
        None

    Return:
        None
    """

    config = configparser.ConfigParser()

    parentFolderPath = Path(__file__).parent.parent.parent
    config_file_path = os.path.join(parentFolderPath, 'common/utils', 'config.ini')

    config.read(config_file_path)

    return config

def hello_world():
    """
    This function is empty.

    Args:
        None
    
    Return:
        None
    """

    print("Hello")

def add(x, y):

    """
    Add two numbers together

    Args:
        Float or Int

    Return:
        Float or Int
    """

    return x + y


def add(x, y):

    """
    Substract two numbers together

    Args:
        Float or Int

    Return:
        Float or Int
    """

    return x - y