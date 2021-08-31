import sys
import io
import os
import json
import time
import socket
from datetime import datetime, date
from decimal import Decimal

#from pymysqlreplication.binlogstream import ReportSlave

from pymysqlreplication.binlogstream import (
	BinLogStreamReader,
	ReportSlave
)

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
from kafka.errors import KafkaTimeoutError
from kafka.producer import (
	KafkaProducer
)

#from configuration import *
from replication import (
	StreamReplicator,
	BuildPayload,
	BuildMessage,
	MessageEncoder,
	datetime_handler
)

from messaging import build_message
from app_logging import logging as log

from replicate_to_neo4j import Neo4jMessageMapper

class BinlogServer(object):
	def __init__(self, mysql_conf, repl_conf, kafka_conf):
		self.mysql = mysql_conf
		self.repl = repl_conf
		self.kafka = kafka_conf

		self.binlog_stream:BinLogStreamReader = None
		self.kafka_producer:KafkaProducer = None

	def start(self):

		#print(self.repl);

		replicator = StreamReplicator(self.mysql)
		binlog_server_conf = int(self.repl['server_id'])
		slave_uuid_conf = self.repl['slave_uuid']
		auto_position_conf = replicator.get_auto_position(self.repl)
		slave_heartbeat_conf = int(self.repl['slave_heartbeat'])
		only_schemas_conf = list(str(self.repl['only_schemas']).split(','))
		only_tables_conf = list(str(self.
                    repl['only_tables']).split(','))
		only_events_conf = list([eval(cls_name) for cls_name in str(self.repl['only_events']).split(',')])

		hostname = socket.gethostname()
		slave_conf = dict({'hostname': hostname, 'username': self.mysql['user'], 'password': self.mysql['passwd'], 'port': int(self.mysql['port'])})
		#print(slave_conf)
		replicator.set_heartbeat_interval(slave_heartbeat_conf)
		# INIT: BinLogStreamReader
		mysql_settings = {
				'host': self.mysql['host']
				,'port': int(self.mysql['port'])
				,'user': self.mysql['user']
				,'passwd': self.mysql['passwd']
			}

		try:
		
			self.binlog_stream = BinLogStreamReader(connection_settings = mysql_settings
								, server_id=binlog_server_conf
								, resume_stream=False
								, blocking=True
								, auto_position=auto_position_conf
								, only_schemas=only_schemas_conf
								, only_tables = only_tables_conf
								, only_events = only_events_conf
								, slave_heartbeat = slave_heartbeat_conf
								, report_slave = slave_conf
								, slave_uuid = slave_uuid_conf
							)

			# INIT: KafkaProducer
			#broker_server_conf = self.kafka['broker_server']
			#broker_port_conf = int(self.kafka['broker_port'])
			#broker_retries_conf = int(self.kafka['retries'])
			#bootstrap_servers_conf = '{host}:{port}'.format(host=broker_server_conf,port=broker_port_conf)
			#self.kafka_producer = KafkaProducer(bootstrap_servers=bootstrap_servers_conf
			#							, value_serializer=lambda v: json.dumps(obj=v,default=str).encode('utf-8')
			#							, retries = broker_retries_conf)

			print("Connected. Listening for databases... %s" %str(only_schemas_conf))
			print("Connected. Listening for tables... %s" %str(only_tables_conf))
			print("connected. Listening for events... %s" %str(only_events_conf))
			print("Starting at auto_position: {0}".format(auto_position_conf))

			for evt in self.binlog_stream:
				payload = build_message(evt)
				#print(evt._dump())
				if payload.evt_type in [BINLOG.UPDATE_ROWS_EVENT_V2, BINLOG.WRITE_ROWS_EVENT_V2, BINLOG.DELETE_ROWS_EVENT_V2] and \
					payload.message:
					#for msg in payload.messages:
					try:
						send_topic = '{0}{1}'.format('',payload.message.table).lower()
						table_key = payload.message.table.lower()
						
						# FORMAT message to Kafka
						# message = {'schema': payload.message.schema, 'table': payload.message.table, 'op': payload.message.op, 'rows': payload.data}
						#response = self.kafka_producer.send(topic = send_topic
						#	, value = payload.data
						#	, key = table_key.encode('utf-8')
						#)
						
						# FORMAT message to Neo4j
						mapper = Neo4jMessageMapper(payload)
						mapper.sync()

						replicator.commit_auto_position(payload.timestamp, payload.position)
						log.info('send_topic: %s at %s' %(send_topic, datetime.fromtimestamp(payload.timestamp)))

					except BufferError as bex:
						log.error('%% Local producer queue is full (%s messages awaiting delivery): try again\n' %str(bex))

					#except KafkaTimeoutError as kex:
					#	log.error('%% KafkaTimeoutError (%s messages awaiting delivery): try again\n' %str(kex))

					except Exception as ex:
						log.exception('%% Unhandle exception (%s messages failed delivery): try again\n' %str(ex))
						#time.sleep(1)

				elif payload.evt_type == BINLOG.GTID_LOG_EVENT:
					replicator.set_gtid_executed(payload.gtid)
					replicator.commit_auto_position(payload.timestamp, payload.position)

				elif payload.evt_type == BINLOG.HEARTBEAT_LOG_EVENT:
					replicator.set_heartbeat(payload.binlog, payload.position)
					replicator.commit_auto_position(payload.timestamp, payload.position)
				
			self.binlog_stream.close()
			#self.kafka_producer.close()

		except Exception as ex:
			sys.stderr.write('%% Unhandle exception (%s MySQL Streamer connect failed): try again\n' %str(ex))

	def stop(self):
		if self.binlog_stream:
			self.binlog_stream.close()

		#if self.kafka_producer:
			#self.kafka_producer.close()
