import logging
from pprint import pprint
from os import environ

import requests

from kafka import *


log_level = environ.get('LOG_LEVEL', 'INFO')
logging.basicConfig(level=log_level)
