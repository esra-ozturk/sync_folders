from ast import excepthandler
import os
import sys
import logging
import filecmp
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler


list_file = {}
#variables
try :
    path_source = sys.argv[1] if len(sys.argv)>1  else  os.environ.get("path_source")
    path_replica = sys.argv[2] if len(sys.argv)>1  else os.environ.get("path_replica")
    synchronization_interval = int(sys.argv[3]) if len(sys.argv)>1 else int(os.environ.get("synchronization_interval"))
    path_log_file = sys.argv[4] if len(sys.argv)>1 else os.environ.get("path_log_file")
except :
    raise Exception("One or more variables is missing.")

#logging
root_logger = logging.getLogger(__name__) 
root_logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
file_handler = logging.FileHandler('/{}/log{}.log'.format(path_log_file,datetime.strftime(datetime.now(), '%Y%m%d%H%M%S_%f')))
file_handler.setFormatter(formatter)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
root_logger.addHandler(stream_handler)
root_logger.addHandler(file_handler)

def move_file(path_source,path_replica):
    message_source = ' Source : {0} '.format(path_source)
    root_logger.info('%s',message_source)
            
    message_replica = ' Target : {0} '.format(path_replica)
    root_logger.info('%s',message_replica)
            
    with open(path_source, 'rb' ) as f_s:
        with open(path_replica, 'wb' ) as f_r:
            f_r.write(f_s.read())


def compare_files(f1,f2):
    # deep comparison
    result = filecmp.cmp(f1, f2, shallow=False)
    return result

def main(): 
    for file in os.listdir(path_source):
        file_source = path_source+os.sep+file
        file_replica = path_replica+os.sep+file
        if os.path.isfile(file_replica):
            if compare_files(file_source,file_replica) == False :
                move_file(file_source,file_replica)
        else:
            move_file(file_source,file_replica)

    for file in os.listdir(path_replica): 
        file_source = path_source+os.sep+file
        file_replica = path_replica+os.sep+file
        if not os.path.isfile(file_source):
            message_replica = ' {0} is deleted from : {1} '.format(file, path_replica)
            root_logger.info('%s',message_replica)
            os.remove(file_replica)


if __name__ == '__main__':
    main()
    scheduler = BlockingScheduler()
    scheduler.add_job(main, 'interval', minutes= synchronization_interval)
    scheduler.start()