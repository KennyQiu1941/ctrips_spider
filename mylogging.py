import logging
import pymongo
import time
import redis
import zmail
import configparser
import sys
import json

'''MyHandlers类是Handler继承重写emit实现logging对各种数据库的写入
调用方法基本和默认方法一样
ler = logging.getLogger()
ler.setLevel(logging.WARNING)
handle = MyHandlers('test',dbtype='mongodb',host='192.168.1.188',port=32778)
ler.addHandler(handle)
ler.info('fffff')
Mylogging类是用来调用logging模块和重写的dbhandler
实例化Mylogging.dblogger.info('aaa')
'''


def load_conf():
    log_conf = configparser.ConfigParser()
    log_conf.read('logsetting.ini')
    conf_data = log_conf.items(log_conf.sections()[0])
    return conf_data[0][1], conf_data[1][1], int(conf_data[2][1])


def sender(content):
    conf = configparser.ConfigParser()
    conf.read('logsetting.ini')  # 读取配置文件
    addr_key = [i for i in conf.items('qq')]  # 提取conf文件下qq的每一个key
    target_addr = [conf.get('qq', i[0]) for i in addr_key if 'target_mail' in str(i)]  # 每个目标地址提取成一个列表
    server = zmail.server(conf.get('qq', 'username'), conf.get('qq', 'password'))
    mail = {
        'from': '服务器故障推送',
        'subject': '',
        'content_text': content,

    }
    server.send_mail(target_addr, mail)


def logDataTimelinessCheck(dbname, day=30, db=0, projectType='spider'):
    '''自动删除数据库中过时的log数据这个函数需要单独运行或者独立建立一个线程'''
    while True:
        dbtype, host, port = load_conf()
        con = pymongo.MongoClient(host, port)
        logs = con['{}Logs'.format(projectType)]
        coll = logs['{}Logs'.format(dbname)]
        rdb = redis.Redis(host=host, port=port, db=db)
        nowtime = time.time()
        allmongolog = coll.find().sort('nowtime', pymongo.ASCENDING)
        for i in allmongolog:
            logtime = i['nowtime']
            if nowtime - logtime > day * 60 * 60 * 24:
                coll.delete_one({'nowtime': logtime})
            else:
                break
        time.sleep(60 * 60 * 24)


class MyHandlers(logging.Handler):
    def __init__(self, dbname, db=0, projectType='spider'):
        tmplogtime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        logging.Handler.__init__(self)
        dbtype, host, port = load_conf()  # 读取conf文件中的参数
        self.dbtype = dbtype
        if 'mongodb' in dbtype:
            con = pymongo.MongoClient(host, port,serverSelectionTimeoutMS=3)
            logs = con['{}Logs'.format(projectType)]
            self.coll = logs['{}Logs'.format(dbname)]
            tmpLog = 'mongodb初始化成功{}'.format(tmplogtime)
        elif 'redis' in dbtype:
            self.rdb = redis.Redis(host=host, port=port, db=db)
            self.keyname = '{}logs'.format(dbname)
            tmpLog = 'reids初始化成功{}'.format(tmplogtime)
        else:
            tmpLog = '初始化失败dbtype参数输入error{} 正确：mongodb,redis'.format(tmplogtime)
        with open('tmpLog.log', 'a+') as f:
            if 'error' in tmpLog:
                f.write(tmpLog)
                raise Exception('dbtypeError')
            else:
                f.write(tmpLog)
                print(tmpLog)

    def get_lineno(self):
        '''通过主动抛出异常获取所在行号和函数名'''
        try:
            raise Exception
        except:
            f = sys.exc_info()[2].tb_frame.f_back
        return '{}函数: 第{}行'.format(f.f_code.co_name, f.f_lineno)

    def fmtprint(self, record):
        '''格式化log信息'''
        logtime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        logstr = '{} {} {}line {}:{}'.format(logtime, record.pathname, record.lineno, record.levelname,
                                             record.getMessage())  #
        print(logstr)
        return logstr

    def return_formail(self, record):
        return self.fmtprint(record)

    def localerror_str(self, e, lineno):
        return '{} {} {}: {}'.format(self.dbtype, lineno, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), e)

    def emit(self, record):
        '''改写emit，使handler可以讲log信息写入数据库是这次重写的主要目的'''
        logstr = self.fmtprint(record)
        nowtime = time.time()
        logdict = {'log': logstr, 'nowtime': nowtime}
        if 'mongodb' in self.dbtype:
            try:
                self.coll.insert(logdict)
            except Exception as e:
                lineno = self.get_lineno()
                slstr = self.localerror_str(e=e, lineno=lineno)
                print(slstr)
                sender(slstr)
            # else:
            # print('写入{}成功'.format(self.dbtype))
        elif 'redis' in self.dbtype:
            try:
                self.rdb.lpush(self.keyname, json.dumps(logdict))
            except Exception as e:
                lineno = self.get_lineno()
                slstr = self.localerror_str(e=e, lineno=lineno)
                print(slstr)
                sender(slstr)
            # else:
            # print('写入{}成功'.format(self.dbtype))


class Mylogging():
    def __init__(self):
        # 创建两个logger
        self.stlogger = logging.getLogger('stream')
        self.dblogger = logging.getLogger('db')
        # 对logger进行level设定
        self.stlogger.setLevel(logging.DEBUG)
        self.dblogger.setLevel(logging.INFO)
        # 创建handler
        self.my_db_handler = MyHandlers()
        sh = logging.StreamHandler()
        # 对handler设定格式
        sh.setFormatter(
            logging.Formatter('%(asctime)s %(threadName)s %(pathname)s%(lineno)d行 %(levelname)s %(message)s'))
        # handler添加入logger
        self.stlogger.addHandler(sh)
        self.dblogger.addHandler(self.my_db_handler)

    # def reload_dblogger(self):
    #     log_conf = configparser.ConfigParser()
    #     log_conf.read('logsetting.ini')
    #     conf_data = log_conf.items(log_conf.sections()[0])
    #     projectName = conf_data[0][1]
    #     dbtype = conf_data[1][1]
    #     host = conf_data[2][1]
    #     port = int(conf_data[3][1])
    #     my_db_handler = MyHandlers(projectName,dbtype,host,port)
    #     self.dblogger.addHandler(my_db_handler)
    #     return self.dblogger


if __name__ == '__main__':
    # from mylogging import  MyHandlers
    # import logging
    idblogger = logging.getLogger('idb')
    wdblogger = logging.getLogger('wdb')
    idblogger.setLevel(logging.INFO)
    wdblogger.setLevel(logging.WARNING)
    my_infodb_handler = MyHandlers(dbname='ctripPTinfo')
    my_warningdb_handler = MyHandlers(dbname='ctripPTwarning')
    idblogger.addHandler(my_infodb_handler)
    wdblogger.addHandler(my_warningdb_handler)
