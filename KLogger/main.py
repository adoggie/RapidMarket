#coding:utf-8

"""
KLogger
订阅来自行情接入服务分发的行情记录 kline （1m） ，本地文件持久化 和 nosql 持久化

"""
import os
import json
import time
import datetime
from threading import Thread
import signal
import sys
import fire
import cmd
import pymongo
import pytz
from elabs.fundamental.utils.useful import singleton,input_params
from elabs.app.core.controller import Controller
from elabs.app.core.behavior import Behavior
from elabs.app.core import logger
from elabs.app.core.registry_client import RegistryClient
from elabs.app.core.message import Tick,KLine,OrderBook
from elabs.app.core.market_receiver import MarketReceiver
from elabs.utils.useful import Timer


PWD = os.path.dirname(os.path.abspath(__file__))

@singleton
class KLogger(Behavior,cmd.Cmd):

  prompt = 'KLogger > '
  intro = 'welcome to elabs..'
  def __init__(self):
    Behavior.__init__(self)
    cmd.Cmd.__init__(self )
    self.running = False
    self.conn = None
    self.timer = None

  def init(self,**kvs):
    Behavior.init(self,**kvs)

    if self.cfgs.get('registry_client.enable', 1):
      RegistryClient().init(**kvs)
    MarketReceiver().init(**kvs).addUser(self)

    interval = self.cfgs.get('keep_alive_interval',5)
    self.timer = Timer(self.keep_alive,interval)
    return self

  def keep_alive(self,**kvs):
    """定时上报状态信息"""
    Controller().keep_alive()

  def open(self):
    if self.cfgs.get('registry_client.enable', 1):
      RegistryClient().open()
    MarketReceiver().open()
    return self

  def close(self):
    self.running = False
    # self.thread.join()

  def onKline(self, kline: KLine):
    """ serialize into file and nosql db """
    logger.debug("KLogger onKline :", kline.marshall())
    self.into_file(kline)
    self.into_nosql( kline )

  def into_file(self,kline: KLine):
    """写入文件系统"""
    pass

  def into_nosql(self,kline: KLine):
    conn = self.db_conn()
    db = conn[kline.exchange.upper()]
    symbol = kline.symbol.replace("/", "_")
    name = f"{kline.tt}_{kline.period}_{symbol}"
    coll = db[name]
    # dt = datetime.datetime.fromtimestamp(kline.datetime/1000,pytz.UTC)
    kline.datetime = int(kline.datetime/1000)
    dt = datetime.datetime.fromtimestamp(kline.datetime)
    # dt.replace(tzinfo= datetime.timezone.utc)
    data = dict(
      DT = dt,
      TS=kline.datetime,
      O=kline.open,
      H=kline.high,
      L=kline.low,
      C=kline.close,
      V=kline.vol,
      AMT=kline.amt,
      OPI=kline.opi,
      TRAN=kline.transactions,
      MKR=kline.is_maker,
      BV=kline.buy_vol,
      BAMT=kline.buy_amt
    )
    coll.update_one(dict(TS=data['TS']), {'$set': data}, upsert=True)
    coll.create_index([('TS', 1)])

  def db_conn(self):
    if not self.conn:
      self.conn = pymongo.MongoClient(**self.cfgs['mongodb'])
    return self.conn

  def do_exit(self,*args):
    Controller().close()
    print('bye bye!')
    return True

  def do_show(self,line):
    args = input_params(line,['pos'])
    if args:
     pass

  def signal_handler(signal, frame):
    sys.exit(0)

def signal_handler(signal,frame):
  Controller().close()
  print('bye bye!')
  sys.exit(0)

#------------------------------------------
FN = os.path.join(PWD,  'settings.json')

def run(id = '',fn='',noprompt=False):
  global FN
  if fn:
    FN = fn
  params = json.loads(open(FN).read())
  if id:
    params['service_id'] = id

  Controller().init(**params).addBehavior("market",KLogger()).open()
  if noprompt:
    signal.signal(signal.SIGINT, signal_handler)
    print("")
    print("~~ Press Ctrl+C to kill .. ~~")
    while True:
      time.sleep(1)
  else:
    KLogger().cmdloop()


if __name__ == '__main__':
  run()
  # fire.Fire()