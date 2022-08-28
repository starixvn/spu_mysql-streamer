import os
import time
from datetime import datetime, date
from decimal import Decimal
from configparser import ConfigParser
from app_const import CONF_PATH

DEFAUL_CONF_PATH='{0}/config.ini'.format(CONF_PATH)

def util_get_config(section, conf_file=DEFAUL_CONF_PATH):
    if not conf_file or not os.path.isfile(conf_file):
        return None

    config = ConfigParser()
    config.read(conf_file)
    if section:
        return config[section]
    else:
        return None

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        serial = obj.isoformat()
        return serial
    if isinstance(obj, Decimal):
    	return float(obj)
    else:
    	print ("Type '{0}' for '{1}' not serializable".format(obj.__class__, obj))
    	return None

# Optional per-message delivery callback (triggered by poll() or flush())
# when a message has been successfully delivered or permanently
# failed delivery (after retries).
def delivery_callback(err, msg):
    if err:
        sys.stderr.write('%% Message failed delivery: %s\n' % err)
    else:
        sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                            (msg.topic(), msg.partition(), msg.offset()))