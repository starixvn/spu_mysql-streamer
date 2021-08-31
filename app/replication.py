import sys
from datetime import datetime
import json
from multiprocessing import shared_memory
from app_logging import logging as log
from app_const import CONF_PATH

def datetime_handler(x):
    if isinstance(x, datetime):
        return x.isoformat()
    raise TypeError("Unknown type")


class MessageEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, BuildMessage):
            return obj.__dict__

        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)

class BuildMessage(object):
	def __init__(self, schema, table, op=None):
		self.rows = []
		self.schema = schema
		self.table = table
		self.op = op
	

class BuildPayload(object):
	def __init__(self, evt_type, gtid=None, message:BuildMessage=None, position=None, timestamp:int=0, binlog=None):
		self.evt_type = evt_type
		self.gtid = gtid
		self.message = message
		self.binlog = binlog
		self.position = position
		self.timestamp = timestamp

	@property
	def data(self):
		if self.message:
			return self.message.rows
		return []

class StreamReplicator(object):
	last_commit_time = None
	last_commit_gtid = None
	binlog = None
	position = None
	hearbeat_interval = 30

	def __init__(self, options, **kwargs):
		self.options = options
		self.auto_position = None
		self.auto_path = '{0}/auto-gtid.txt'.format(CONF_PATH)
		self.cts_shm = shared_memory.ShareableList(name='cts_shm')

	def get_auto_position(self, repl_conf=None, mode=None):
		txt_gtid:str = None
		if 'auto_position' in repl_conf and mode=='up':
			return repl_conf['auto_position']
		
		if self.auto_position:
			txt_gtid = self.auto_position

		else:
			auto_file = open(self.auto_path,'r')
			txt_gtid = auto_file.readline()
			auto_file.close()

		if txt_gtid:
			patterns = txt_gtid.split('\t')[0].split(':')
			txt_gtid = '{uuid}:1-{pos}'.format(uuid=patterns[0], pos=patterns[1])

		return txt_gtid

	def set_heartbeat_interval(self, slave_heartbeat_conf):
		if slave_heartbeat_conf:
			self.hearbeat_interval = slave_heartbeat_conf

	def set_gtid_executed(self, gtid, position=None):
		if gtid:
			self.last_commit_gtid = gtid
		if position:
			self.position = position

	def get_gtid_executed(self):
		return self.cts_shm

	def get_master_config(self):
		if self.options:
			return dict(self.options)

		return None

	def set_heartbeat(self, binlog, position):
		if binlog:
			self.binlog = binlog
			self.position = position

	def commit_auto_position(self, timestamp:int, position:int):
		if position:
			self.position = position

		if not self.last_commit_gtid:
			txt_gtid = self.get_gtid_executed()
			if txt_gtid:
				self.last_commit_gtid = txt_gtid[0]

		if self.last_commit_gtid:
			self.auto_position = self.last_commit_gtid
			self.last_commit_time = datetime.fromtimestamp(timestamp)
			try:
				self.cts_shm[0] = self.last_commit_gtid
				self.cts_shm[1] = self.last_commit_time.strftime('%Y-%m-%d %H:%M:%S')
				self.cts_shm[2] = self.position

			except Exception as ex:
				log.exception('%% Unhandle exception (%s write GTID failed): try again\n' %str(ex))
			
			
