import os
import logging
from logging.handlers import RotatingFileHandler
import datetime
from app_const import LOG_PATH

# Get environment variables
DEBUG_LEVEL = os.getenv('MYSQL_STREAMER_DEBUG_LEVEL')
if DEBUG_LEVEL not in ('ERROR','INFO','DEBUG'):
    DEBUG_LEVEL = 'INFO'

LOG_PREFIX = os.getenv('MYSQL_STREAMER_LOG_PREFIX')
if not LOG_PREFIX:
    LOG_PREFIX = 'app_log_'

LOG_MAX_BYTES = 0
STREAMER_LOG_MAX_BYTES = os.getenv('MYSQL_STREAMER_LOG_MAX_BYTES')
if not STREAMER_LOG_MAX_BYTES:
    LOG_MAX_BYTES = 100*1024*1024
else:
    LOG_MAX_BYTES = int(STREAMER_LOG_MAX_BYTES)

ROTATE_COUNT = 0
STREAMER_LOG_ROTATE_COUNT = os.getenv('MYSQL_STREAMER_LOG_ROTATE_COUNT')
if not ROTATE_COUNT:
    ROTATE_COUNT = 10
else:
    ROTATE_COUNT = int(STREAMER_LOG_ROTATE_COUNT)

LOG_FILENAME = '{0}/{1}{2}.log'.format(LOG_PATH, LOG_PREFIX, datetime.date.today().strftime('%d%m%Y'))

handlers = [RotatingFileHandler(filename=LOG_FILENAME, mode='a', maxBytes=LOG_MAX_BYTES, backupCount=ROTATE_COUNT)]
logging.basicConfig(handlers=handlers,
                    format='%(asctime)s.%(msecs)03d | %(levelname)-8s | %(lineno)04d | %(message)s',
                    datefmt='%d-%m-%Y %H:%M:%S',
                    level=DEBUG_LEVEL
                   );