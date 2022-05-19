import sys
import time
import threading
from multiprocessing import shared_memory
from server import BinlogFactory
from monitoring import run_monitoring

act=None
if len(sys.argv) > 1:
    act=str(sys.argv[1])

if not act:
    act = 'start'

if __name__ == '__main__':
    try:
       t = threading.Thread(target=run_monitoring, args = ('Thread - run_monitoring', 1, ), daemon=True)
       t.start()
    except Exception as ex:
       sys.stderr.write('%%Error: (%s) unable to start thread.' %str(ex))

    try:
        cts_shm = shared_memory.ShareableList(name='cts_shm')
    except :
        cts_shm = shared_memory.ShareableList([str('6f78a381-5583-11eb-bc7d-0242ac170002:100000000000'),str('yyyy-mm-dd hh:MM:ss'),0],name='cts_shm')

    BinlogFactory.process('read-from-queue', '')