from ctripPTSpider import CtripPTSpider
import threading
import time

threading_num = 28

ctp = CtripPTSpider()
rdb = ctp.con_redis()
mdb = ctp.con_mongo()
for i in range(threading_num):
    threading.Thread(target=ctp.write_database, args=(rdb, mdb)).start()
time.sleep(2)
threading.Thread(target=ctp.check_threadnum, args=(len(threading.enumerate()),rdb)).start()
