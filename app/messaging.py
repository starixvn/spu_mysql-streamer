from datetime import datetime
from pymysqlreplication.event import (
	StopEvent,
	GtidEvent,
	HeartbeatLogEvent,
	RotateEvent
)
from pymysqlreplication.row_event import (
	BINLOG,
	RowsEvent,
	WriteRowsEvent,
	DeleteRowsEvent,
	UpdateRowsEvent
)

from replication import (
	StreamReplicator,
	BuildPayload,
	BuildMessage
)

def build_message(binlog_evt):
	
	if isinstance(binlog_evt, (RotateEvent)):
		return BuildPayload(evt_type=BINLOG.ROTATE_EVENT)

	if isinstance(binlog_evt, (StopEvent)):
		return BuildPayload(evt_type=BINLOG.STOP_EVENT)

	if isinstance(binlog_evt, (GtidEvent)):
		return BuildPayload(evt_type=BINLOG.GTID_LOG_EVENT, gtid=binlog_evt.gtid, position=binlog_evt.packet.log_pos, timestamp = binlog_evt.timestamp)

	if isinstance(binlog_evt, (HeartbeatLogEvent)):
		return BuildPayload(evt_type=BINLOG.HEARTBEAT_LOG_EVENT, binlog=binlog_evt.ident,position=binlog_evt.packet.log_pos, timestamp = datetime.now().replace(microsecond=0).timestamp())

	if not isinstance(binlog_evt, RowsEvent):
		return BuildPayload(evt_type=binlog_evt.event_type)

	#messages = []
	message = BuildMessage(schema = getattr(binlog_evt, 'schema', ''), table=getattr(binlog_evt, 'table', ''))
	#for row in binlog_evt.rows:
	if isinstance(binlog_evt, WriteRowsEvent):
		# Insert
		#messages.append({'op':'INSERT', 'meta':meta, 'data':row['values']})
		message.op = 'c'
		message.rows = list([{'op': 'c','after':row['values']} for row in binlog_evt.rows])

	elif isinstance(binlog_evt, UpdateRowsEvent):
		# Update
		#messages.append({'op':'UPDATE', 'meta':meta, 'data':row['after_values']})
		message.op = 'u'
		message.rows = list([{'op': 'u','before':row['before_values'],'after':row['after_values']} for row in binlog_evt.rows])

	elif isinstance(binlog_evt, DeleteRowsEvent):
		# Delete
		#messages.append({'op':'DELETE', 'meta':meta, 'data':row['values']})
		message.op = 'd'
		message.rows = list([{'op': 'd','before':row['values']} for row in binlog_evt.rows])

	#print('binlog_evt.timestamp: {0}, meta: {1}.{2}, txn_rows: {3}'.format(datetime.fromtimestamp(binlog_evt.timestamp),len(binlog_evt.rows), binlog_evt.shema, binlog_evt.table))
	return BuildPayload(evt_type=binlog_evt.event_type, message=message, position = binlog_evt.packet.log_pos, timestamp = binlog_evt.timestamp)