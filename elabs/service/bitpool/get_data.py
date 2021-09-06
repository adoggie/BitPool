# coding:utf-8

import pymysql
from datetime import datetime, timedelta
import fire
from dateutil.parser import parse
from elabs.service.bitpool import config

TABLE = 'bian_spot_origin_realtime_1min'

def pull_data(symbol='',start='',end=''):
  print("Start Pulling StockDate ..")
  if start:
    if isinstance(start,str):
      start = parse(start)
  else:
    start = datetime.now() - timedelta(days=1)
    
  if end:
    if isinstance(end,str):
      end = parse(end)
  else:
    end = datetime.now()

  table_name = TABLE
  
  cnxn = pymysql.connect(**config.MYSQL_ASSEMBLE)
  cursor = cnxn.cursor(pymysql.cursors.DictCursor)

  if symbol:
    sql = "select stockdate,symbol,open,high,low,close,volume,amount from " + table_name + " where symbol = '" + symbol + "' and stockdate between '{}' and '{}' order by stockdate"
  else:
    sql = "select stockdate,symbol,open,high,low,close,volume,amount from " + table_name + " where  stockdate between '{}' and '{}' order by stockdate"
  sql = sql.format(start,end)
  try:
    print sql
    cursor.execute(sql)
  except Exception, e:
    print("Exception:", e)
    cursor.close()
    return False

  bars = cursor.fetchall()
  bars = list(bars)
  return bars

def test():
  # print datetime.now()- timedelta(hours=1)
  return pull_data('',datetime.now()- timedelta(hours=1))
#
if __name__ == "__main__":
  fire.Fire()


