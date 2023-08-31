import os
import logging
from pathlib import Path

LOGLEVEL_KEY = 'LOGLEVEL'
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    level=os.environ.get(LOGLEVEL_KEY, 'INFO').upper(),
    datefmt='%Y-%m-%d %H:%M:%S',
)

ROOT_DIR = Path(__file__).parent.parent


def dummy(a):
    return a + 1
