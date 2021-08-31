import sys
import schedule
from datetime import datetime
import time
import json
from configuration import util_get_config
from pymysql import connect
from replication import StreamReplicator
from neo4j_collection import Neo4jCollector
import threading
from multiprocessing import shared_memory
from app_logging import logging as log
from app_const import CONF_PATH

def binlog_job():
    conf = util_get_config('mysql')
    mysql_settings = {'host': conf['host'],'port': int(conf['port']),'user': conf['user'],'passwd': conf['passwd'], 'autocommit': True}
    master_position = -1
    bytes_behind_master = 0
    second_behind_master = 0
    master_alive = 0
    try:
        conn = connect(**mysql_settings)
        show_master_cmd = "SHOW MASTER STATUS"
        cur = conn.cursor()
        cur.execute(show_master_cmd)
        master_status = cur.fetchone()
        master_position = int(master_status[1])
        master_alive = 1
        cur.close()
        conn.close()
    except Exception as ex:
        log.exception('%% Unhandle exception (%s mysql-source connect failed): try again\n' %str(ex))
    
    cts_shm = shared_memory.ShareableList(name='cts_shm')
    #print('datetime.now(): %s' %str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    #print(cts_shm);

    slave_gtid_executed = cts_shm[0]
    last_commit_time = datetime.strptime(cts_shm[1], '%Y-%m-%d %H:%M:%S')
    last_position = int(cts_shm[2])
    
    if last_position > 0:
      if master_position != -1:
          bytes_behind_master = master_position-last_position
  
      if bytes_behind_master != 0:
          second_behind_master = (datetime.now().replace(microsecond=0) - last_commit_time).total_seconds()
  
      # write file
      write_gtid_file='{0}/auto-gtid.txt'.format(CONF_PATH)
      with open(write_gtid_file,'w+') as auto_file:
          auto_file.write('{0}\t{1}\t{2}'.format(slave_gtid_executed, last_commit_time, last_position))
          auto_file.close()
  
      # detect heartbeat
      heartbeat = 1
      slave_heartbeat_interval = 0
      repl_conf = util_get_config('replication')
      if 'slave_heartbeat' in repl_conf:
          slave_heartbeat_interval = int(repl_conf['slave_heartbeat'])
      
      if master_position == -1 or (bytes_behind_master == 0 and second_behind_master > slave_heartbeat_interval):
          heartbeat = 0
  
      log.info('------------Binlog monitoring -----------------')
      #print('------> master_position: {0}, python_position: {1}'.format(master_position, last_position))
      #print('-----------------------------------------------------')
      
      try:
          monitor = util_get_config('monitoring')
          monitor_settings = {'host': monitor['host'],'port': int(monitor['port']),'user': monitor['user'],'passwd': monitor['passwd'], 'autocommit': True}
          conn_m = connect(**monitor_settings)
          insert_heartbeat = "insert into MonDB.neo4j_replication_status(heartbeat,insert_time,bytes_behind_master,gtid_executed,last_commit_time, event_type, second_behind_master)\
  	        values({0}, current_timestamp(4), {1}, '{2}', '{3}', 'latency', {4})".format(heartbeat,bytes_behind_master, slave_gtid_executed, last_commit_time, second_behind_master)
      
          cur_m = conn_m.cursor()
          cur_m.execute(insert_heartbeat)
          cur_m.close()
          conn_m.close()
      except Exception as ex:
          log.exception('%% Unhandle exception (%s mysql-monitoring connect failed): try again\n' %str(ex))

def neo4j_mode(mode:str='source'):
    result = {}
    try:
        conf_mode = ('neo4j-%s' %str(mode))
        conf = util_get_config(conf_mode)
        collector = Neo4jCollector(conf['uri'], conf['user'], conf['passwd'])
        node_stats = collector.get_node_stats()
        relationship_stats = collector.get_relationship_stats()
        collector.close()

        node_json = {}
        for val in node_stats:
            node_json[val.label] = val.total

        relationship_json = {}
        for val in relationship_stats:
            relationship_json[val.label] = val.total

        result['node'] = node_json
        result['relationship'] = relationship_json

    except Exception as ex:
        log.exception('%% Unhandle exception (%s neo4j connect failed): try again\n' %str(ex))

    return result

def neo4j_job():
    try:
        node_json = {}
        relationship_json = {}
        heartbeat = {}

        node_source = neo4j_mode('source')
        node_sink = neo4j_mode('sink')

        source_heartbeat = 0
        if 'node' in node_source:
            source_heartbeat = 1
            node_json['source'] = node_source['node']
            relationship_json['source'] = node_source['relationship']
        

        sink_heartbeat = 0
        if 'node' in node_sink:
            sink_heartbeat = 1
            node_json['sink'] = node_sink['node']
            relationship_json['sink'] = node_sink['relationship']

        heartbeat['source'] = source_heartbeat
        heartbeat['sink'] = sink_heartbeat

        monitor = util_get_config('monitoring')
        monitor_settings = {'host': monitor['host'],'port': int(monitor['port']),'user': monitor['user'],'passwd': monitor['passwd'], 'autocommit': True}
        conn_m = connect(**monitor_settings)

        insert_stats = "insert into MonDB.neo4j_collector_metric(insert_time,heartbeat,node,relationship)\
	        values(current_timestamp(4), '{0}', '{1}', '{2}')".format(json.dumps(heartbeat),json.dumps(node_json), json.dumps(relationship_json))
    
        log.info('------------Neo4j monitoring -----------------')
        #print('------> nodes: {0}, relationships: {1}'.format(json.dumps(node_json), json.dumps(relationship_json)))
        #print('-----------------------------------------------------')

        #print(insert_stats)
        cur_m = conn_m.cursor()
        cur_m.execute(insert_stats)
        cur_m.close()
        conn_m.close()
    except Exception as ex:
        log.exception('%% Unhandle exception (%s neo4j monitoring failed): try again\n' %str(ex))



schedule.every(5).minutes.do(binlog_job)
#chedule.every(1).minutes.do(neo4j_job)
#schedule.every().hour.do(job)
#schedule.every().day.at("10:30").do(job)
#schedule.every(5).to(10).minutes.do(job)
#schedule.every().monday.do(job)
#schedule.every().wednesday.at("13:15").do(job)
#schedule.every().minute.at(":17").do(job)
#
#
def run_monitoring(thread_name, delay):
    log.info('Multiplethreading %s.' %str(thread_name))
    while True:
        schedule.run_pending()
        time.sleep(delay)

#if __name__ == "__main__":
#    run_monitoring()
