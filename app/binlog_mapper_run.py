import sys
import io
import os
import json
import time
import socket
import pika
import functools
import threading
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

#from configuration import *
from replication import (
	StreamReplicator,
	BuildPayload,
	BuildMessage,
	BuildPayloadReceiver,
	MessageEncoder,
	datetime_handler
)

from messaging import build_message
from app_logging import logging as log
from replicate_to_neo4j import Neo4jMessageMapper
from pymysql import connect

class BinlogServer(object):
	def __init__(self, mysql_conf, repl_conf):
		self.mysql = mysql_conf
		self.repl = repl_conf

		self.binlog_stream:BinLogStreamReader = None

	def start(self, mode):

		#print(self.repl);
		if mode is None:
			mode = 'recovery'

		replicator = StreamReplicator(self.mysql)
		binlog_server_conf = int(self.repl['server_id'])
		slave_uuid_conf = self.repl['slave_uuid']
		auto_position_conf = replicator.get_auto_position(self.repl, mode)
		slave_heartbeat_conf = int(self.repl['slave_heartbeat'])
		only_schemas_conf = list(str(self.repl['only_schemas']).split(','))
		only_tables_conf = list(str(self.repl['only_tables']).split(','))
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

		# Get current position from db
		conn = connect(**mysql_settings)
		get_current_pos_cmd = "select gtid_executed FROM MonDB.neo4j_replication_status order by id desc limit 1;"
		cur = conn.cursor()
		cur.execute(get_current_pos_cmd)
		gtid_from_db = cur.fetchone()
		master_position = gtid_from_db[0].split(':');
		auto_position_conf = '{uuid}:1-{pos}'.format(uuid=master_position[0], pos=master_position[1])

		# Connect to RabbitMQ
		connection = pika.BlockingConnection(pika.ConnectionParameters('10.18.200.73'))
		channel = connection.channel()
		channel.queue_declare(queue='mysql-streamer')

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

			print("Connected. Listening for databases... %s" %str(only_schemas_conf))
			print("Connected. Listening for tables... %s" %str(only_tables_conf))
			print("connected. Listening for events... %s" %str(only_events_conf))
			print("Starting at auto_position: {0}".format(auto_position_conf))

			for evt in self.binlog_stream:
				payload = build_message(evt)
				messsage = json.dumps(payload, cls=MessageEncoder)
				channel.basic_publish(exchange='', routing_key='mysql-streamer', body=messsage)
				
			self.binlog_stream.close()
			connection.close()

		except Exception as ex:
			print('%% Unhandle exception (%s MySQL Streamer connect failed): try again\n' %str(ex))

	def stop(self):
		if self.binlog_stream:
			self.binlog_stream.close()
			connection.close()

	def read_message_from_queue(self):
		try:
			connection = pika.BlockingConnection(pika.ConnectionParameters(host='10.18.200.73'))
			channel = connection.channel()

			channel.queue_declare(queue='mysql-streamer')
			replicator = StreamReplicator(self.mysql)

			def ack_message(ch, delivery_tag):
				"""Note that `ch` must be the same pika channel instance via which
				the message being ACKed was retrieved (AMQP protocol constraint).
				"""
				if ch.is_open:
					ch.basic_ack(delivery_tag)
				else:
					# Channel is already closed, so we can't ACK this message;
					# log and/or do something that makes sense for your app in this case.
					pass

			def on_message(ch, method_frame, _header_frame, body, args):
				(conn, thrds) = args
				delivery_tag = method_frame.delivery_tag
				t = threading.Thread(target=handle_message, args=(conn, ch, delivery_tag, body))
				t.start()
				thrds.append(t)

			def handle_message(conn, ch, delivery_tag, body):
				messsage = json.loads(body)
				payload = BuildPayloadReceiver(**messsage)
				
				if payload.evt_type in [BINLOG.UPDATE_ROWS_EVENT_V2, BINLOG.WRITE_ROWS_EVENT_V2, BINLOG.DELETE_ROWS_EVENT_V2] and \
					payload.message:

					try:
						log.info('CUD event - position {0} - time {1} '.format(payload.position, datetime.fromtimestamp(payload.timestamp)))
						send_topic = '{0}{1}'.format('',payload.message.table).lower()
						
						# FORMAT message to Neo4j
						mapper = Neo4jMessageMapper(payload)
						response = mapper.sync()

						replicator.commit_auto_position(payload.timestamp, payload.position)
						log.info('proceed message: {0} at {1}. op: {2}. rows {3}'.format(send_topic, datetime.fromtimestamp(payload.timestamp),payload.message.op,len(payload.message.rows)))

					except BufferError as bex:
						print('%% Local producer queue is full (%s messages awaiting delivery): try again\n' %str(bex))
						log.error('%% Local producer queue is full (%s messages awaiting delivery): try again\n' %str(bex))

					except Exception as ex:
						print('%% Unhandle exception (%s messages failed delivery): try again\n' %str(ex))
						log.exception('%% Unhandle exception (%s messages failed delivery): try again\n' %str(ex))

						# Connect to RabbitMQ
						connectionOnError = pika.BlockingConnection(pika.ConnectionParameters('10.18.200.73'))
						channelOnError = connectionOnError.channel()
						channelOnError.queue_declare(queue='mysql-streamer-error')
						
						channel.basic_publish(exchange='', routing_key='mysql-streamer-error', body=body)
						raise Exception('There is an error in Replication')
						time.sleep(1)

				elif payload.evt_type == BINLOG.GTID_LOG_EVENT:
					replicator.set_gtid_executed(payload.gtid)
					replicator.commit_auto_position(payload.timestamp, payload.position)
					log.info('GTID log event - type {0} - GTID {1} - position {2} - time {3} '.format(payload.evt_type, payload.gtid, payload.position, datetime.fromtimestamp(payload.timestamp)))

				elif payload.evt_type == BINLOG.HEARTBEAT_LOG_EVENT:
					replicator.set_heartbeat(payload.binlog, payload.position)
					replicator.commit_auto_position(payload.timestamp, payload.position)
					log.info('Heart beat event - type {0} - GTID {1} - position {2} - time {3} '.format(payload.evt_type, payload.gtid, payload.position, datetime.fromtimestamp(payload.timestamp)))	

				cb = functools.partial(ack_message, ch, delivery_tag)
				conn.add_callback_threadsafe(cb)

			channel.basic_qos(prefetch_count=1)
			threads = []
			on_message_callback = functools.partial(on_message, args=(connection, threads))
			channel.basic_consume('mysql-streamer', on_message_callback)
			channel.start_consuming()

			for thread in threads:
				thread.join()

		except Exception as ex:
			#print('%% Unhandle exception (%s MySQL Streamer connect failed): try again\n' %str(ex))
			print(str(ex))
			raise Exception('There is an error in Replication')