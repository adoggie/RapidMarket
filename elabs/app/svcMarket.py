#coding:utf-8

import os
import json
import time
import datetime
from threading import Thread
import signal
import sys
import fire
import cmd

from elabs.fundamental.utils.importutils import import_class
from elabs.fundamental.utils.useful import singleton,input_params
from elabs.app.core.controller import Controller
from elabs.app.core.behavior import Behavior
from elabs.app.core import logger
from elabs.app.core.registry_client import RegistryClient
from elabs.app.core.position_receiver import PosReceiver
from elabs.app.core.tradecmd import TradeCmd
from elabs.app.core.market_publish import MarketPublisher
from elabs.app.core.position_receiver import PosReceiver
from elabs.app.core.message import Tick,KLine,OrderBook
from elabs.app.core.command import PositionSignal
from elabs.app.core.base import MarketBase
from elabs.app.core.klinecache import KlineLocalCache

PWD = os.path.dirname(os.path.abspath(__file__))

@singleton
class MarketInstance(Behavior,cmd.Cmd):

  prompt = 'Market > '
  intro = 'welcome to elabs..'
  def __init__(self):
    Behavior.__init__(self)
    cmd.Cmd.__init__(self )
    # TradeCmd.__init__(self)
    self.running = False
    self.market_impl:MarketBase = None

  def init(self,**kvs):
    Behavior.init(self,**kvs)
    MarketPublisher().init(**kvs)
    RegistryClient().init(**kvs)
    KlineLocalCache().init(**kvs)

    impl_cls = import_class(self.cfgs.get('class'))
    self.market_impl = impl_cls()
    self.market_impl.init(**kvs)
    return self

  def open(self):
    ok = RegistryClient().open()
    if not ok:
      return self
    MarketPublisher().open()
    KlineLocalCache().open()
    self.market_impl.open()
    return self

  def close(self):
    self.market_impl.close()
    self.running = False
    # self.thread.join()

  def onTick(self, tick: Tick):
    MarketPublisher().publish_loc(tick)

  def onKline(self, kline: KLine):
    KlineLocalCache().write(kline)
    MarketPublisher().publish_remote(kline)

  def onOrderBook(self, orderbook: OrderBook):
    MarketPublisher().publish_loc(orderbook)

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
FN = os.path.join(PWD,  'market.json')

def run(id = '',fn='',noprompt=False):
  global FN
  if fn:
    FN = fn
  params = json.loads(open(FN).read())
  if id:
    params['service_id'] = id

  Controller().init(**params).addBehavior("market",MarketInstance()).open()
  if noprompt:
    signal.signal(signal.SIGINT, signal_handler)
    print("")
    print("~~ Press Ctrl+C to kill .. ~~")
    while True:
      time.sleep(1)
  else:
    MarketInstance().cmdloop()

if __name__ == '__main__':
  # run()
  fire.Fire()