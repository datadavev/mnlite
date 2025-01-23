import os
import logging
from datetime import datetime

from opersist.cli import LOG_DATE_FORMAT, LOG_FORMAT
from mnlite.mnode import DEFAULT_NODE_CONFIG

DEFAULT_JSON = DEFAULT_NODE_CONFIG

__version__ = 'v0.0.1'

LOG_FORMAT = "%(asctime)s %(funcName)s:%(levelname)s: %(message)s" # overrides import

FN_DATE = datetime.now().strftime('%Y-%m-%d')
HM_DATE = datetime.now().strftime('%Y-%m-%d-%H%M')
YM_DATE = datetime.now().strftime('%Y-%m')
LOG_DIR = '/var/log/mnlite/'
LOG_NAME = 'mnonboard-%s.log' % (FN_DATE)
LOG_LOC = os.path.join(LOG_DIR, LOG_NAME)

HARVEST_LOG_NAME = '-crawl.log'

def start_logging():
    """
    Initialize logger.

    :returns: The logger to use
    :rtype: logging.Logger
    """
    logger = logging.getLogger('mnonboard')
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt=LOG_FORMAT, datefmt=LOG_DATE_FORMAT)
    s = logging.StreamHandler()
    s.setLevel(logging.INFO)
    s.setFormatter(formatter)
    # this initializes logging to file
    f = logging.FileHandler(LOG_LOC)
    f.setLevel(logging.DEBUG)
    f.setFormatter(formatter)
    # warnings also go to file
    # initialize logging
    logger.addHandler(s) # stream
    logger.addHandler(f) # file
    logger.info('----- mnonboard %s start -----' % __version__)
    return logger

L = start_logging()

# absolute path of current file
CUR_PATH_ABS = os.path.dirname(os.path.abspath(__file__))

# relative path from root of mnlite dir to nodes directory
NODE_PATH_REL = 'instance/nodes/'

def default_json(fx='Unspecified'):
    """
    A function that spits out a dict to be used in onboarding.

    :returns: A dict of values to be used in member node creation
    :rtype: dict
    """
    L.info('%s function loading default json template.' % (fx))
    return DEFAULT_JSON
