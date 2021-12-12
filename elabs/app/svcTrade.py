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

from elabs.fundamental.utils.useful import input_params
from elabs.fundamental.utils.useful import singleton,input_params
from elabs.app.core.controller import Controller
from elabs.app.core.behavior import Behavior
from elabs.app.core import logger
from elabs.app.core.registry_client import RegistryClient
from elabs.app.core.tradecmd import TradeCmd
from elabs.app.core.position_receiver import PosReceiver
from elabs.app.core.market_receiver import MarketReceiver
from elabs.app.core.message import Tick,KLine,OrderBook
from elabs.app.core.command import PositionSignal
from elabs.app.core.base import TradeBase
from elabs.fundamental.utils.importutils import import_class

PWD = os.path.dirname(os.path.abspath(__file__))

@singleton
class TradeInstance(Behavior,cmd.Cmd):

  prompt = 'Trade > '
  intro = 'welcome to elabs..'
  def __init__(self):
    Behavior.__init__(self)
    cmd.Cmd.__init__(self )
    # TradeCmd.__init__(self)

    self.running = True
    self.trade_impl:TradeBase = None


  def init(self,**kvs):
    Behavior.init(self,**kvs)
    RegistryClient().init(**kvs)
    PosReceiver().init(**kvs).addUser(self)
    MarketReceiver().init(**kvs).addUser(self)

    impl_cls = import_class( self.cfgs.get('class'))
    self.trade_impl = impl_cls()
    self.trade_impl.init(**kvs)

  def open(self):
    RegistryClient().open()
    PosReceiver().open()
    MarketReceiver().open()
    self.trade_impl.open()
    return self

  def close(self):
    self.trade_impl.close()
    self.running = False
    self.thread.join()

  def onTick(self, tick: Tick):
    """行情服务转发来的分时记录"""
    self.trade_impl.onTick(tick)

  def onKline(self, kline: KLine):
    self.trade_impl.onKline(kline)

  def onOrderBook(self, orderbook: OrderBook):
    self.trade_impl.onOrderBook(orderbook)

  def onPositionSignal(self, pos: PositionSignal):
    self.trade_impl.onPositionSignal(pos) # 收到发送到达的仓位信号转发给 报单服务模块

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
FN = os.path.join(PWD,  'trade.json')

def run(id='',fn='',noprompt=False):
  global FN
  if fn:
    FN = fn
  params = json.loads(open(FN).read())
  if id:
    params['service_id'] = id

  Controller().init(**params).addBehavior("trade",TradeInstance()).open()
  if noprompt:
    signal.signal(signal.SIGINT, signal_handler)
    print("")
    print("~~ Press Ctrl+C to kill .. ~~")
    while True:
      time.sleep(1)
  else:
    TradeInstance().cmdloop()


if __name__ == '__main__':
  # run()
  fire.Fire()