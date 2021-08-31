from configuration import util_get_config
from binlog_mapper_run import BinlogServer


# replicator
mysql_conf = util_get_config('mysql')
repl_conf = util_get_config('replication')
kafka_conf = util_get_config('kafka')



class BinlogFactory:
    binlog = BinlogServer(mysql_conf, repl_conf, kafka_conf)
    
    @staticmethod
    def process(act, mode):
        if act == 'start':
            BinlogFactory.binlog.start(mode)
        elif act == 'stop':
            BinlogFactory.binlog.stop()

# create info static method
#BinlogFactory.process = staticmethod(BinlogFactory.process)
