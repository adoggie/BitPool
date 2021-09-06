# coding:utf-8

"""
pull bar data from mysql and compare with shared-memory area .

"""

import pymysql
from datetime import datetime, timedelta
import time
# import settings as config
import fire

from dateutil.parser import parse

# import settings
import  spoon
from shared_struct import Bar,SharedDataManager

from congoriver import CongoRiver
import config

def init_congo():

  cr = CongoRiver().init()
  cr.set_confs('brokers/pub', config.CONGO_SERVICE['pub_url'])  # 设置发送目的zmq地址
  cr.set_confs_local(service_type=config.CONGO_SERVICE['service_type'],
                     service_id=config.CONGO_SERVICE['service_id'],
                     name='first',
                     version='0.1',
                     ip=config.CONGO_SERVICE['ip'])
  cr.set_confs('timers/heartbeat/interval', 0)  # 设置心跳时间
  cr.open()

init_congo()

def dump(symbol,shm_id = config.SHM_DATA_ID):
  sdm = SharedDataManager()
  today = str(datetime.now()).split(' ')[0]
  start = parse(today)
  sdm.init(shm_key= shm_id, symbols=config.SYMBOL_LIST, start=start)
  block = sdm.get_block(symbol)
  if not block:
    return

  rs = block.get_data(sdm.start, sdm.end)

  size = len(rs['time'])
  all = []
  for n in range(size):
    text = '{},{},{},{},{},{},{}'.format(
      rs['time'][n],
      rs['open'][n],
      rs['high'][n],
      rs['low'][n],
      rs['close'][n],

      rs['volume'][n],
      rs['open_interest'][n],
    )
    all.append(text)
  return all



def check(symbols,alert=False,shm_id = config.SHM_DATA_ID,start='',days=2):
  """
  :param symbols:  'a,b,c'  or '' means all
  :return:
  """
  # print alert
  table_name = config.CTP_INDEX

  if not start:
    start = str(datetime.now().date())
  start = parse(start)

  start = parse( str(datetime.now().date())) + timedelta(hours=9)

  # end = start + timedelta(days=days)
  end = parse( str(datetime.now()).split('.')[0]) # - timedelta(minutes=0)
  end = datetime(end.year,end.month,end.day,end.hour,end.minute)
  print '-'*10,start,end

  if not symbols:
    symbols = ','.join(config.SYMBOL_LIST)
  symbols = symbols.split(',')
  symbols = map(lambda s: s.upper(), symbols)

  #-- init shared memory --
  # sdm = spoon.init_shared_mem(shm_id,config.SYMBOL_LIST,start = str(start) )
  sdm = spoon.init_shared_mem(shm_id,config.SYMBOL_LIST )
  #-- end sdm --

  cnxn = pymysql.connect(**config.MYSQL_ASSEMBLE)
  cursor = cnxn.cursor()
  for symbol in symbols:

    # print "retrieving data : ", symbol
    sql = "Select StockDate,O,H,L,C,V,OPI from " + table_name + " where Symbol = '" + symbol + "' and StockDate >= '{}' and StockDate < '{}'".format( str(start),str(end)) + " order by StockDate"
    try:
      # print sql
      cursor.execute(sql)
    except Exception, e:
      print "Exception:", e
      cnxn.close()
      break

    block = sdm.get_block(symbol)
    if not block:
      print  'error: block not found , :',symbol
      continue

    columns = [column[0] for column in cursor.description]
    data = cursor.fetchall()
    # cnxn.close()
    for d in data[:-1]:
    # for d in data:
      s = d[0]
      e = s + timedelta(minutes=1)
      bars = block.get_data(s,e)
      if len(bars['time']) == 0:
        text = 'No Bar In Mem: {} {} {}'.format(symbol,str(s),str(e) )
        print(text)
        if alert:
          send_alert(text)
        return
      # a = list(d[1:-2])
      a = list(d[1:])
      b = [
        bars['open'][0],
        bars['high'][0],
        bars['low'][0],
        bars['close'][0],
        bars['volume'][0],
        bars['open_interest'][0],
           ]
      # print symbol,d[0],a,b
      if a!=b:
        text = 'Bar Diff: {} {} {}'.format(symbol, str(s), str(e) )
        print( text )
        print('MySql:',a)
        print('InMem:',b)
        if alert:
          text += 'MySql: {}'.format(a)
          text += 'InMem: {}'.format(b)
          send_alert(text)
        return

def send_alert(text):
  time.sleep(1)
  delta = dict(db='EL_Global', table='BitPot_MemCheck', sms=config.SMS, email=config.Mail)
  CongoRiver().send_any( data=dict(detail=text,datetime=datetime.now()), delta=delta)
  delta = dict(db='BitPot_MemCheck', table='{}'.format( datetime.now().date() ))
  CongoRiver().send_any( data=dict(detail=text,datetime=datetime.now()), delta=delta)

if __name__ == "__main__":
  fire.Fire()


