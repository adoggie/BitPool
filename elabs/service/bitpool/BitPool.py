#coding:utf-8

import datetime
import random
import string
import sys,os
import time
import shutil
import traceback

from dateutil.parser import parse
from multiprocessing import Process
# from pyelf.elutil import get_bar_time
from elabs.utils.useful import singleton
from elabs.service.bitpool.get_bar_time import get_bar_time
from elabs.service.bitpool.shared_struct import SharedDataManager,Bar
# import zmq
import fire
from elabs.utils.useful import Timer
from elabs.service.bitpool import config
from elabs.service.bitpool.get_data import pull_data
# from congoriver import CongoRiver


def hm2num(hm):
  return hm[0]*100 + hm[1]

def num2hm(num):
  return num/100 , num%100

def make_datetime(num,dt=None):
  h,m = num2hm(num)
  if not dt:
    dt = datetime.datetime.now()
  return datetime.datetime(dt.year,dt.month,dt.day,h,m,0)

@singleton
class BitPool(object):
    def __init__(self):
        self.cfgs = {}
        # self.ctx = zmq.Context()
        self.sub_sock = None        # subscribe for K
        self.pub_sock = None
        self.sub_svc_sock = None
        self.sdm = None
        self.period_sdms = {}

    def init_mx(self,):
        self.sub_sock = self.ctx.socket(zmq.SUB)
        # sub_sock.setsockopt(zmq.SUBSCRIBE, b"elabs/CTP_XZ/au/K/1m")  # 订阅指定品种
        self.sub_sock.setsockopt(zmq.SUBSCRIBE, config.TOPIC_SUB_QUOTES)  # 订阅所有品种
        self.sub_sock.connect(config.MX_QUOTES_ADDR)

        self.pub_sock = self.ctx.socket(zmq.PUB)
        self.pub_sock.bind(config.MX_PUB_ST_ADDR)
        return self

    def init_shared_mem(self):
        # SharedDataManager.remove_shared(config.SHM_DATA_ID)
        used_size = 0
        for period in config.PERIODS:
            # print('init peroid:',period)
            sdm = SharedDataManager()
            ID = int('%03d'%period,16)
            sdm.init(shm_key=config.SHM_DATA_ID_BASE + ID, period = period, symbols=config.SYMBOL_LIST,
                start= config.BUFF_ZONE_START,
                end = config.BUFF_ZONE_END,
                days = config.BUFF_ZONE_DAYS,
                mongodb = config.MONGODB,
                # fx_get_bar_time = config.fx_get_bar_time
                # init_data = config.INIT_DATA,
                # mem_zero = self.cfgs.get('mem_zero',False)
                )
            self.period_sdms[period] = sdm

            print('use space:',period , int(sdm.used_space/(1024*1024.)), 'M')
        return self
    
    def mem_used(self):
        size = 0
        for period in config.PERIODS:
            sdm = self.period_sdms[period]
            size += sdm.used_space
        return size
    
    def init(self,**args):
        self.cfgs.update(**args)
        # if not args.get('mx_skip'):
        #     self.init_mx()
        self.init_shared_mem()
        # if not args.get('congo_skip'):
        #     self.init_congo()
        return self
    
    def init_data(self):
        # if config.INIT_DATA:
        #     self.sdm.load_data()
        return self
    
    def init_congo(self):
        self.congo_timer = Timer(self.service_status, config.CONGO_SERVICE['interval'])
        cr = CongoRiver().init()
        cr.set_confs('brokers/pub', config.CONGO_SERVICE['pub_url'])  # 设置发送目的zmq地址
        cr.set_confs_local(service_type= config.CONGO_SERVICE['service_type'],
                           service_id= config.CONGO_SERVICE['service_id'],
                           name='first',
                           version='0.1',
                           ip=config.CONGO_SERVICE['ip'])
        cr.set_confs('timers/heartbeat/interval', 0)  # 设置心跳时间
        # cr.set_confs_topic('topics/status/pub','pub')
        # cr.open()
        return self

    def service_status(self):
        print('congo status..')

        try:
            CongoRiver().send_status(status=1, detail='i am  okay')
            dbname = config.CONGO_SERVICE['ip']
            data = dict(uptime=datetime.datetime.now(),
                        service_type=config.CONGO_SERVICE['service_type'],
                        service_id=config.CONGO_SERVICE['service_id'],
                        ip=config.CONGO_SERVICE['ip'],

                        )
            CongoRiver().send_any(data=data,
                                  delta=dict(db='EL_Global', table='Service_Status',
                                             update_keys=['service_type', 'service_id','ip']))
        except:
                pass

    def mem_zero(self):
        for sdm in self.period_sdms.values():
            sdm.empty_data()
        # self.sdm.empty_data()
        return self


    def mx_recv(self):
        last_recv_time = time.time()
        recv_timeout = config.QUOTES_RECV_TIMEOUT

        poller = zmq.Poller()
        poller.register(self.sub_sock, zmq.POLLIN)
        # poller.register(self.sub_svc_sock, zmq.POLLIN)
        CongoRiver().open()
        print 'mx_recv ready..'
        while True:
            events = dict(poller.poll(1000))
            self.congo_timer.kick()

            try:
                if self.sub_sock in events:
                    message = self.sub_sock.recv_string()
                    # elabs/CTP_XZ/K/1m || UR,2021-01-25 13:53:00,2021-01-25,13:53:00,1971,1971,1971,1971,52,197016,346801770.0,2021-01-25 13:53:22.420681"
                    #  symbol,time,xx,opeigh,low,close,vol,open_int,amount,xxx
                    chan, msg = message.split(" || ")
                    # print "{chan} ==> [{msg}]".format(chan=chan, msg=msg)
                    self.on_data_quotes(msg)

                    last_recv_time = time.time()
                    recv_timeout = config.QUOTES_RECV_TIMEOUT
                if self.sub_svc_sock in events:
                    self.on_svc_msg_recv(message)
            except:
                print('Error: dirty message . ' , message)

    def run(self):
        # self.mx_recv( )
        self.sync_data()
    
    def sync_data(self,start):
        try:
            if not start:
                start = datetime.datetime.now() - datetime.timedelta(minutes=config.RELOAD_HISTORY_TIME)
            rs = pull_data('',start = start)
            self.on_data_quotes(rs)
        except:
            traceback.print_exc()
        
        # while True:
        #     try:
        #         time.sleep(  config.DATA_PULL_INTERVAL )
        #         rs = pull_data('',start = datetime.datetime.now() - datetime.timedelta(minutes=config.DATA_PULL_STRIDES))
        #         self.on_data_quotes(rs)
        #     except:
        #         traceback.print_exc()
            
    
    def fill_index(self,period = 0,symbol=''):
        sdms = self.period_sdms.values()
        if period:
            sdms = [ self.period_sdms[period]]
          
        for sdm in sdms:
            sdm.fill_index( symbol)
          
        # for period,sdm in self.period_sdms.items():
        #     print 'fill index:',period
        #     sdm.fill_index()
        
    def on_svc_msg_recv(self, message):
        pass

    def on_data_quotes(self,rs):

        for r in rs:
            symbol = r['symbol']
            if symbol not in config.SYMBOL_LIST:
                continue
            bar = Bar()
            bar.name = symbol
            bar.time = r['stockdate']
            bar.open = r['open']
            bar.high = r['high']
            bar.low = r['low']
            bar.close = r['close']
            bar.opi = 0
            bar.vol = r['amount']
            print 'bar put into shared mem :', bar.__dict__
            self.put_data(symbol,1,bar,'update')

    def sythesize(self,m1,mode='update'):
        # make N minutes Bar from 1 M bar
        m1.sdm = self.period_sdms[1]
        for period,sdm in self.period_sdms.items():
            if period <= 1:
                continue
            sdm.sythesize(m1,mode)
    
    def mem_dump(self,symbol,start,end,print_text = False):

        rs = self.sdm.get_data(symbol,start,end)
        size = len(rs['time'])
        all =[]
        for n in range(size):
            text = '{},{},{},{},{},{},{}'.format(
                                                rs['time'][n],
                                                 rs['open'][n],
                                                 rs['high'][n],
                                                 rs['low'][n],
                                                 rs['close'][n],
                                                 rs['open_interest'][n],
                                                 rs['volume'][n],
                                                 )
            if print_text:
                print(text)
            all.append(text)
        return all

    def import_data(self,symbol,period,bar):
        pass

    def clear_data(self,symbol,st,et):
        """清除合约所有周期缓存的K线记录"""
        for sdm in self.period_sdms.values():
            sdm.clear_data(symbol,st,et)

    def put_data(self, symbol,period, m1,mode='update'):
        """接受1M k线传入，存储到1M缓存，并计算其他周期的K线
        mode - load   历史csv加载
               update 实时更新
        """
        bar = m1
        if period != 1:
            return

        sdm = self.period_sdms.get(period)
        if not sdm:
            return
        sdm.put_data(symbol,bar)
        if period == 1:
            self.sythesize(bar,mode) # 合成其他周期


    def get_latest(self,symbol,period,num=1):
        return self.get_data(symbol,period,num = num)

    def get_data(self,symbols,period,start='',end='',num=1):
        if end:
            if isinstance( end,str):
                end = parse(end)
        # else:
        #     end = datetime.datetime.now()

        if start :
            if isinstance( start,str):
                start = parse(start)

        if period not in self.period_sdms:
            return []

        _symbs = symbols
        if isinstance(symbols,str):
            _symbs = [symbols]
        symbols = _symbs

        sdm = self.period_sdms[period]
        data = []
        if len(symbols) == 1:
            data = sdm.get_data(symbols[0],start,end,num)
        else:
            for symbol in symbols:
                data.append( sdm.get_data(symbol,start,end,num))
        return data
    
    def load_data(self):
        for sdm in self.period_sdms.values():
            sdm.load_data()

def fill_index(index=0,symbol='',pool=None):
    print 'Index Filling..'
    if not pool:
        pool = client()
    def multi_works(*args):
        pool.fill_index(*args)
    
    if index:
        return pool.fill_index(index,symbol)

    jobs = []
    for period in config.PERIODS:
        p = Process(target=multi_works, args=(period,symbol))
        jobs.append(p)
        p.start()
    C = 0
    for p in jobs:
        p.join()
        print 'finished jobs:', C, len(config.PERIODS)
        C += 1
    
# def run(renew=False):
#     pot = BitPool()
#     if renew :
#         reset()
#     pot.init(mem_zero=renew)
#     if renew:
#         pot.mem_zero()
#     pot.init_data()
#     used_size = pot.mem_used()
#     print 'total mem used:', used_size /1024/1024 ,'M'
#
#     pot.run()

def sync(start=''):
    c = client()
    c.sync_data(start)

pool = None
def client():
    global pool
    if not pool:
        pool = BitPool()
        pool.init(mx_skip=True,congo_skip=True)
    # pool.init_data()
    return pool

def _data_reload():
    pool = BitPool()
    pool.init(mx_skip=True, congo_skip=True)
    pool.load_data()
    
def reset():
    for period in config.PERIODS:
        ID = config.SHM_DATA_ID_BASE + int('%03d' % period, 16)
        cmd = 'ipcrm -M 0x%x'%ID
        os.system(cmd)

def destroy():
    reset()

def create():
    pool = client()
    pool.mem_zero()
    fill_index(pool=pool)
    
def mem_zero():
    BitPool().init(mx_skip=True,congo_skip=True).mem_zero()

def mem_dump(symbol):
    return BitPool().init(mx_skip=True,congo_skip=True).mem_dump(symbol,False)


def mem_dump_file(symbol=''):
    pot = BitPool().init(mx_skip=True)
    symbols = config.SYMBOL_LIST
    if  symbol :
        symbols.append(symbol)

    for symbol in symbols:
        all = pot.mem_dump(symbol)
        if not all :
            continue

        path = os.path.join( config.POT_DUMP_DATA_DIR,symbol)
        if not os.path.exists( path ):
            os.makedirs(path)
        name = os.path.join(path,'K1M_{}.txt'.format( str(datetime.datetime.now()).split(' ')[0]) )
        fp = open(name,'w')
        for line in all:
            fp.write(line +'\n')
        fp.close()

def edit(name='config.py'):
  from elabs.fundamental.utils.cmd import vi_edit
  fn = os.path.join( os.path.dirname( os.path.abspath(__file__) ) , name)
  vi_edit(fn)

def help():
    usage = """bitpool 0.2 2021/9/5    
    * watch -n 2 -d  'python BitPot.py mem_dump A | tail -n 4 ' 
    """
    print usage

def get_data(symbol,period,start,end):
    pool = client()
    if isinstance(start,str) and start:
        start = parse(start)
    if isinstance(end,str) and end:
        end = parse(end)
    return pool.get_data([symbol],period,start,end)

def get_latest(symbol,period,num=1):
    pool = client()
    return pool.get_latest(symbol,period,num)

def show(prefix='f0'):
    cmd = "ipcs -m --human "
    if prefix:
        cmd = "ipcs -m --human | grep "+prefix
    print os.popen(cmd).read()

# def run_get_data(symbol,period,num):
#     pool = client()
#     while True:
#         print pool.get_data([symbol],period,'','',num)
#         time.sleep(.5)
#
# def get_data_tss(symbol,period,start,end,tss):
#     result = get_data(symbol,period,start,end,0)
#     data = dict(itime=[], time=[], open=[], high=[], low=[], close=[], opi=[], vol=[])
#
#     for n, tx in enumerate(result['time']):
#         for ts in tss:
#             st = make_datetime(ts[0], tx)  # ([hour,min],[hour,min])
#             et = make_datetime(ts[1], tx)
#             if st <= tx < et:
#                 data['time'].append(tx)
#                 data['open'].append(result['open'][n])
#                 data['high'].append(result['high'][n])
#                 data['low'].append(result['low'][n])
#                 data['close'].append(result['close'][n])
#                 data['vol'].append(result['vol'][n])
#                 data['opi'].append(result['opi'][n])
#     return data
      
if __name__ == '__main__':
    fire.Fire()


"""
python -m elabs.service.bitpool.make-bar nosql_to_mem 'A' 5 2021-5-21 2021-5-30

python -m elabs.service.bitpool.make-bar nosql_to_mem 'A' 5 2021-5-21 2021-5-3
python -m elabs.service.bitpool.BitPool get_data A 5 '' '' 10
"""