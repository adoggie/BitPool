#coding:utf-8

import os,os.path,time,datetime,traceback
from dateutil.parser import  parse
import fire
import csv
from functools import partial
from multiprocessing import Process, Queue

from elabs.service.bitpool.get_bar_time import get_bar_time
from elabs.fundamental.utils.useful import open_file

from elabs.service.bitpool.BitPool import client as bitpool_client
from elabs.service.bitpool.shared_struct import Bar
from elabs.service.bitpool import config


"""
python -m elabs.service.bitpool.csv_to_mem load_csv AXSUSDT '2021-1-1' '2022-1-1'
"""
client = bitpool_client()
def load_csv(symbol,start,end,debug=False):
  csv = os.path.join(config.CSV_PATH, '{}.csv'.format(symbol))
  print 'Loading CSV Data ', symbol, csv, start,end
  if not os.path.exists(csv):
    print '-- Not Found:',csv
    return

  if isinstance(start,str):
    start = parse(start)
  if isinstance(end,str):
    end = parse(end)
  start = datetime.datetime(year=start.year,month=start.month,day=start.day)
  end = datetime.datetime(year=end.year,month=end.month,day=end.day)

  client.clear_data(symbol,start,end) # 清除
  # return
  lines = open(csv).readlines()
  data = []
  # 读取合约1M数据
  for line in lines:
    line = line.strip()
    if not line:
      continue
    fs = line.split(',')
    ymd, hMs = fs[:2]
    dt = datetime.datetime(year=int(ymd[:4]), month=int(ymd[4:6]), day=int(ymd[6:]),
                           hour=int(hMs[:2]), minute=int(hMs[2:4]), second=0)
    if dt < start or dt >=end:
      continue
    _open, high, low, close, vol, opi = map(lambda _: float(_), fs[2:])
    data.append([dt, _open, high, low, close, vol, opi])
    bar = Bar()
    bar.name = symbol
    bar.time = dt
    bar.open = _open
    bar.high = high
    bar.low = low
    bar.close = close
    bar.opi = opi
    bar.vol = vol
    client.put_data(symbol,1,bar,'load')
    if debug:
      print dt,_open, high, low, close, vol, opi

def load_csv_all(start,end):
  jobs = []
  
  def multi_works(*args):
    load_csv(*args)


  for symbol in config.SYMBOL_LIST:
    csv = os.path.join(config.CSV_PATH,'{}.csv'.format(symbol))
    p = Process(target=multi_works, args=(symbol, start,end))
    jobs.append(p)
    p.start()
  C = 0
  for p in jobs:
    p.join()
    print 'finished jobs:', C, len(config.SYMBOL_LIST)
    C += 1
    
if __name__ == '__main__':
  fire.Fire()

"""
python -m elabs.service.bitpool.make-bar nosql_to_mem 'A' 5 2021-5-21 2021-5-30

"""