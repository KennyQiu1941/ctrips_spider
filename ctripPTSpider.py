import requests
import pymongo
import redis
import json
import logging
from mylogging import MyHandlers
import configparser
import threading
import mylogging
import time
import copy

# 初始化log模块
stlogger = logging.getLogger('st')
idblogger = logging.getLogger('idb')
wdblogger = logging.getLogger('wdb')
stlogger.setLevel(logging.DEBUG)
idblogger.setLevel(logging.INFO)  # 导入将log写入数据库的handler
wdblogger.setLevel(logging.WARNING)  # 导入将log写入数据库的handler
sthandler = logging.StreamHandler()
fmt = '%(asctime)s - %(threadName)s - %(pathname)s%(lineno)d行 %(levelname)s %(message)s'
fmtstr = logging.Formatter(fmt)
sthandler.setFormatter(fmtstr)
my_infodb_handler = MyHandlers(dbname='ctripPTinfo')
my_warningdb_handler = MyHandlers(dbname='ctripPTwarning')
idblogger.addHandler(my_infodb_handler)
wdblogger.addHandler(my_warningdb_handler)
stlogger.addHandler(sthandler)


# stlogger.debug('sst')
# wdblogger.warning('wdb')
# idblogger.info('idb')


# 发起请求和处理reop数据主类
class CtripPTSpider:
    def __init__(self):
        self.error_count = 'ctripPTSpider_error_count'
        self.mongoCode = {'n': 1, 'nModified': 1, 'ok': 1.0, 'updatedExisting': True}
        self.rdb_post_data_name = 'flightspostdata'
        self.rdb_unreq_name = 'flightnotairline'
        self.apiurl = 'https://flights.ctrip.com/itinerary/api/12808/products'
        self.header = {
            'authority': 'flights.ctrip.com',
            'method': 'POST',
            'path': '/itinerary/api/12808/products',
            'scheme': 'https',
            'accept': '*/*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9',
            'content-length': '223',
            'content-type': 'application/json',
            'origin': 'https://flights.ctrip.com',
            'referer': 'https://flights.ctrip.com/itinerary/oneway/bjs-sha?date=2019-02-11',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'
        }

    # 读取数据库配置参数
    def load_conf(self):
        conf = configparser.ConfigParser()
        conf.read('logsetting.ini')
        mhost = conf.get('CtripPTSpider', 'mongohost')
        rhost = conf.get('CtripPTSpider', 'redishost')
        mongoport = conf.getint('CtripPTSpider', 'mongoport')
        redisport = conf.getint('CtripPTSpider', 'redisport')
        return mhost, mongoport, rhost, redisport

    # 创建mongo连接并验证连接是否有效连接错误重写读取数据重写创建
    def con_mongo(self):
        mhost, mongoport, rhost, redisport = self.load_conf()
        con = pymongo.MongoClient(mhost, mongoport, serverSelectionTimeoutMS=3)
        db = con['ctripflights']
        return db

    # 创建redis连接并验证
    def con_redis(self):
        mhost, mongoport, rhost, redisport = self.load_conf()
        rdb = redis.Redis(rhost, redisport, db=0, socket_timeout=3)
        return rdb

    # 获取所有的城市code数据（作用是获取航班信息的post参数）这函数只需单独运行24小时获取一次就可以
    def get_city_code(self):
        headers = {
            'authority': 'flights.ctrip.com',
            'method': 'GET',
            'path': '/itinerary/api/poi/get',
            'scheme': 'https',
            'accept': '*/*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'
        }
        citysApi = 'https://flights.ctrip.com/itinerary/api/poi/get'
        reop = requests.get(url=citysApi, headers=headers)
        citycode = reop.json()['data']
        del citycode['热门']
        return citycode

    # 预处理cityscode的json数据生成器
    def preprocessing(self, cityscode):
        for zimulist in cityscode:
            zimucitys = cityscode[zimulist]
            for zimu in zimulist:
                try:
                    citys = zimucitys[zimu]
                except KeyError:
                    pass
                else:
                    for city in citys:
                        yield city['display'], city['data']

    # 生成需要查询的时间 明天时间往后+60天生成
    def gendate(self, days=60, nowtime=time.time()):
        for i in range(days):
            nowtime += 86400
            yield time.strftime('%Y-%m-%d', time.localtime(nowtime))

    # 检查线程数发现以外错误并返回
    def check_threadnum(self, thread_num, rdb):
        stlogger.info('检查以外退出程序启动')
        raw_threadList = threading.enumerate()
        while True:
            now_threadList = threading.enumerate()
            now_tlnum = len(now_threadList)
            if now_tlnum < thread_num:
                s = '发现以外错误{}个原进程列表:{}现在进程列表：{}'.format(thread_num - now_tlnum, raw_threadList, now_threadList)
                wdblogger.error(s)
                mylogging.sender(s)
                rdb.incr(self.error_count)
                raw_threadList = now_threadList
                thread_num = now_tlnum
            time.sleep(15)

    # 提取post参数并写入redis数据库供多线程
    def req_parameter(self, date, cityscode, rdb):
        allcityscode = self.preprocessing(cityscode)
        for each_dcity in allcityscode:
            dcityname = each_dcity[0]
            tmp = each_dcity[1].split('|')
            dcityzimu = tmp[3]
            dcitynum = tmp[2]
            aallcityscode = self.preprocessing(cityscode)
            for each_acity in aallcityscode:
                if each_acity != each_dcity:
                    acityname = each_acity[0]
                    tmp = each_acity[1].split('|')
                    acityzimu = tmp[3]
                    acitynum = tmp[2]
                    if not rdb.sismember(self.rdb_unreq_name, json.dumps('{}->{}'.format(dcityname, acityname))):
                        rdb.lpush(self.rdb_post_data_name, json.dumps(
                            [dcityzimu, acityzimu, dcityname, acityname, date, dcitynum, acitynum]))

    def write_parameter(self, cityscode, rdb):
        for date in self.gendate():
            print(date)
            threading.Thread(target=self.req_parameter, args=(date, cityscode, rdb)).start()

    # 由于请求航班有的航班是不同航在req_parameter和write_database这两个函数内实现将不通航的航班放入redis数据库名为
    # self.rdb_unreq_name = 'flightnotairline'的集合内做到不在对这个航班发出请求
    # 下面这个函数功能就是每过5天将这个集合删除用于重新看下航班是否通航
    def delTimelinessData(self, rdb):
        while True:
            time.sleep(5 * 60 * 60 * 24)
            rdb.delete(self.rdb_unreq_name)

    # 将请求的到的数据处理并写入数据库
    def req(self, postjsondata, date, dcityname, acityname, rdb):
        # stlogger.info('{}: {}->{}'.format(date, dcityname, acityname))
        try:
            reop = requests.post(url=self.apiurl, headers=self.header,
                                 json=postjsondata)
            json_data = reop.json()['data']
        except Exception as e:
            wdblogger.error('{}: {}'.format(e, postjsondata))
            rdb.incr(self.error_count)
            return None
        else:
            return json_data

    # 从返回处理过的json数据中获取出发，目的地的机场和时间
    def geteachflightinfo(self, eachdata, routeinfo, routetype, rdb):
        tmplist = list()
        flightId = str(time.time()).replace('.', '0')
        tmpdict = {'routetype': routetype, 'flightId': flightId}
        for eachinfo in routeinfo:
            hangbaninfo = eachinfo['flight']
            flightNumber = hangbaninfo['flightNumber']  # 航班号
            airlineName = hangbaninfo['airlineName']  # 航空公司名
            b = hangbaninfo['departureAirportInfo']['airportName']  # 起点机场
            bcityname = hangbaninfo['departureAirportInfo']['cityName']  # 起点城市
            c = hangbaninfo['arrivalAirportInfo']['airportName']  # 终点机场
            acityname = hangbaninfo['arrivalAirportInfo']['cityName']
            d = hangbaninfo['departureDate']  # 起飞时间
            e = hangbaninfo['arrivalDate']  # 降落时间
            f = {'flightNumber': flightNumber,
                 'airlineName': airlineName,
                 'dairportname': '{}: {}'.format(bcityname, b),
                 'aairportname': '{}: {}'.format(acityname, c),
                 'departureDate': d,
                 'arrivalDate': e}
            tmplist.append(f)
            if routetype == 'Flight':
                try:
                    price = eachinfo['cabins'][0]['price']['price']
                except Exception:
                    stlogger.info('无效数据')
                else:
                    tmpdict['info'] = tmplist
                    return tmpdict, '{}price'.format(flightId), [price, time.time()]
        if routetype == 'Transit':
            try:
                price = eachdata['transitPrice']
            except Exception as e:
                wdblogger.error('解析价格是发生错误 {} 疑似未知结构数据{}'.format(e, eachdata))
                rdb.incr(self.error_count)
            else:
                tmpdict['info'] = tmplist
                return tmpdict, '{}price'.format(flightId), [price, time.time()]

    # 更新mongodb价格数据
    def updatePrice(self, coll, price, mongodocuments,tmpmongodata,rawpriceId):
        mongodb_reop = coll.update_one(tmpmongodata, {'$push':{rawpriceId:{'$each':price}}})
        if mongodb_reop.modified_count:
            stlogger.info('{} 价格更新'.format(tmpmongodata))
        else:
            wdblogger.info('{}价格更新失败{}'.format(tmpmongodata,mongodocuments))

    # 检查新获取的数据是否在数据库内不在就添加如数据库，数据库有就核对价格价格变化传给updatePrice函数更新数据
    def check_price(self, coll, tmpdict, mongodocuments, tmpmongodata, routetype, priceKey,price):
        flightList = mongodocuments['infolist']
        tmpdata = tmpdict['info']
        sameDataSign = False  # 用来标记需要验证的航班是否是新的航班
        for i in flightList:
            if i['routetype'] == routetype:
                newinfo = tmpdata
                rawinfo = i['info']
                if newinfo == rawinfo:
                    newprice = price[0]
                    rawpriceId = '{}price'.format(i['flightId'])
                    rawprice = mongodocuments[rawpriceId][-2]
                    sameDataSign = True
                    if newprice != rawprice:
                        self.updatePrice(coll, price, mongodocuments,tmpmongodata,rawpriceId)
                    else:
                        stlogger.info('{} {}价格不变'.format(tmpmongodata,rawpriceId))
        if not sameDataSign:  # 插入新的航班数据
            infomongodb_reop = coll.update_one(tmpmongodata, {'$push': {'infolist': tmpdict,priceKey:{'$each':price}}})
            if infomongodb_reop.modified_count:
                stlogger.info('{}发现新的航班并插入数据库'.format(tmpmongodata))
            else:
                wdblogger.info('{}价格更新失败{}'.format(tmpmongodata,mongodocuments))

    # 从redis数控获取需要爬的fromtodate并继续请求和筛选写入数据库
    def write_database(self, rdb, db):
        while True:
            if rdb.exists(self.rdb_post_data_name):
                postdata = rdb.lpop(self.rdb_post_data_name)
                if postdata:
                    dcityzimu, acityzimu, dcityname, acityname, date, dcitynum, acitynum = json.loads(postdata)
                    postjsondata = {"flightWay": "Oneway", "classType": "ALL", "hasChild": 'false', "hasBaby": 'false',
                                    "searchIndex": 1,
                                    "airportParams": [
                                        {"dcity": dcityzimu, "acity": acityzimu, "dcityname": dcityname,
                                         "acityname": acityname,
                                         "date": date,
                                         "dcityid": dcitynum,
                                         "acityid": acitynum}]}
                    coll = db[dcityname]
                    jsondata = self.req(postjsondata, date, dcityname, acityname, rdb)
                    fromto = '{}->{}'.format(dcityname, acityname)
                    mongodata = {'from-to': '{}->{}{}'.format(dcityname, acityname, date)}
                    tmpmongodata = copy.deepcopy(mongodata)
                    if not jsondata:
                        rdb.lpush(self.rdb_post_data_name, postdata)
                        continue
                    else:
                        routeStatus = jsondata['error']
                        if not routeStatus or (routeStatus['code'] == '102'):
                            alldata = jsondata['routeList']
                            mongodocuments = coll.find_one(tmpmongodata)
                            infolist = list()
                            if alldata:
                                for eachdata in alldata:
                                    routetype = eachdata['routeType']
                                    if routetype == 'Transit' or routetype == 'Flight':
                                        routeinfo = eachdata['legs']
                                        try:
                                            tmpdict, priceKey, price = self.geteachflightinfo(eachdata, routeinfo,
                                                                                              routetype, rdb)
                                        except Exception as e:
                                            wdblogger.error('geteachflightinfo函数返回发生错{}'.format(e))
                                            continue
                                        if mongodocuments:
                                            self.check_price(coll, tmpdict, mongodocuments, tmpmongodata,
                                                             routetype,  priceKey, price)
                                        else:
                                            infolist.append(tmpdict)
                                            mongodata[priceKey] = price
                                    else:
                                        if routetype != 'FlightBus' and routetype != 'FlightTrain':
                                            wdblogger.error('线路类出现未知类型：{}具体数据：{}'.format(routetype, eachdata))
                                            mylogging.sender('线路类出现未知类型：{}具体数据：{}'.format(routetype, eachdata))
                                            rdb.incr(self.error_count)
                                            raise Exception
                                if infolist:
                                    mongodata['infolist'] = infolist
                                    coll.insert(mongodata)
                                    stlogger.info('加入全新{}数据'.format(tmpmongodata))
                            else:
                                stlogger.info('{}状态码返回显示有航班但是无数据加入redis下次不请求'.format(tmpmongodata))
                                rdb.sadd(self.rdb_unreq_name, json.dumps('{}'.format(fromto)))
                                continue
                        else:
                            routeStatusnum = routeStatus['code']
                            if routeStatusnum == '103':
                                rdb.sadd(self.rdb_unreq_name, json.dumps('{}'.format(fromto)))
                                stlogger.info(
                                    '请求{}数据返回状态码{}表明无航班加入reids数据库'.format(tmpmongodata, routeStatusnum))
                            elif routeStatusnum == '101':
                                stlogger.info('{}全票售完'.format(tmpmongodata))
                            else:
                                es = '发现新的状态码{} {}'.format(routeStatusnum, tmpmongodata)
                                wdblogger.error(es)
                                rdb.incr(self.error_count)
                                mylogging.sender(es)
                else:
                    time.sleep(1)

    # 用于统计每分钟请求的次数
    def show_req_count(self, rdb, db, interval=60):
        coll = db['req_count']
        while True:
            startnumr = rdb.llen(self.rdb_post_data_name)
            startnume = json.loads(rdb.get(self.error_count))
            time.sleep(interval)
            endnumr = rdb.llen(self.rdb_post_data_name)
            endnume = json.loads(rdb.get(self.error_count))
            countr = endnumr - startnumr
            counte = endnume - startnume
            s = '在{}分钟内请求次数{}\n错误次数{}'.format(interval / 60, countr, counte)
            idblogger.info(s)
            try:
                coll.insert({'time': time.time(), 'count': s})
            except Exception as e:
                wdblogger.error('写入mongodb失败：{}重新连接'.format(e))
                mylogging.sender('mongodb连接失败')
                rdb.incr(self.error_count)
                db = self.con_mongo()
                coll = db['req_count']

