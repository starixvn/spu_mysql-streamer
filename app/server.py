from app.configuration import util_get_config
from app.binlog_mapper_run import BinlogServer


# replicator
mysql_conf = util_get_config('mysql')
repl_conf = util_get_config('replication')



class BinlogFactory:
    binlog = BinlogServer(mysql_conf, repl_conf)
    
    @staticmethod
    def process(act, mode):
        if act == 'start':
            BinlogFactory.binlog.start(mode)
        elif act == 'read-from-queue':
            BinlogFactory.binlog.read_message_from_queue()
        elif act == 'stop':
            BinlogFactory.binlog.stop()

# create info static method
#BinlogFactory.process = staticmethod(BinlogFactory.process)