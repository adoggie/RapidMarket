# coding:utf-8

"""
pull bar data from mysql into shared-memory area .

"""

import pymysql
from datetime import datetime, timedelta
import settings as config
import fire

from dateutil.parser import parse

import settings
import  spoon
from shared_struct import Bar,SharedDataManager


def dump(symbol,shm_id = settings.SHM_DATA_ID):
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



def pull(symbols,shm_id = settings.SHM_DATA_ID,start='',days=2):
  """
  :param symbols:  'a,b,c'  or '' means all
  :return:
  """

  table_name = settings.CTP_INDEX

  if not start:
    start = str(datetime.now().date())
  start = parse(start)
  end = start + timedelta(days=days)

  if not symbols:
    symbols = ','.join(settings.SYMBOL_LIST)
  symbols = symbols.split(',')
  symbols = map(lambda s: s.upper(), symbols)

  #-- init shared memory --
  sdm = spoon.init_shared_mem(shm_id,settings.SYMBOL_LIST,start = str(start) )
  #-- end sdm --

  cnxn = pymysql.connect(**config.MYSQL_ASSEMBLE)
  cursor = cnxn.cursor()
  for symbol in symbols:

    print "retrieving data : ", symbol
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
    for d in data:
      bar = Bar()
      bar.name = symbol
      bar.time,bar.open,bar.high,bar.low,bar.close,bar.volume,bar.open_interest = d
      block.put(bar)
      # print d

def pull_all():
  pull('')

if __name__ == "__main__":
  fire.Fire()


