from ctripPTSpider import CtripPTSpider
import mylogging
import threading

ctp = CtripPTSpider()
rdb = ctp.con_redis()
mdb = ctp.con_mongo()
# cityscode = ctp.get_city_code()
# ctp.write_parameter(cityscode,rdb)
threading.Thread(target=ctp.show_req_count,args=(rdb,mdb)).start()