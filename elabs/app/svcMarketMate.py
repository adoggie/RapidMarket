#coding:utf-8

"""
svcMarketMate.py
行情服务配套，用于行情补偿
接收系统总线上的KlineAttach请求，读取行情kline并发送到kline总线


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
from elabs.app.core.command import KlineAttach
from elabs.app.core.klinecache import KlineLocalCache

PWD = os.path.dirname(os.path.abspath(__file__))

@singleton
class MarketMateInstance(Behavior,cmd.Cmd):

  prompt = 'MarketMate > '
  intro = 'welcome to elabs..'
  def __init__(self):
    Behavior.__init__(self)
    cmd.Cmd.__init__(self )
    self.running = False


  def init(self,**kvs):
    Behavior.init(self,**kvs)
    MarketPublisher().init(**kvs)
    RegistryClient().init(**kvs).addUser(self)
    KlineLocalCache().init(**kvs)
    return self

  def open(self):
    ok = RegistryClient().open()
    if not ok:
      return self
    MarketPublisher().open()
    KlineLocalCache().open()
    return self

  def close(self):
    self.running = False
    RegistryClient().stop()
    # self.thread.join()

  def onRegClientMessage(self,message):
    # 来自registryclient 部件产生的消息回调
    if isinstance(message,KlineAttach):
      # 补缺 kline
      self.onKlineAttachHandler(message)

  def onKlineAttachHandler(self,message:KlineAttach):
    """接收到kline补尝消息，读取历史kline ，发送kline """
    start = datetime.datetime.fromtimestamp( int(message.start/1000))
    end = datetime.datetime.fromtimestamp( int(message.end/1000))
    lines = KlineLocalCache().read(message.exchange,message.tt,message.period,message.symbol,start,end)
    for line in lines:
      MarketPublisher().publish_remote_rawdata(line)

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
FN = os.path.join(PWD,  'marketmate.json')

def run(id = '',fn='',noprompt=False):
  global FN
  if fn:
    FN = fn
  params = json.loads(open(FN).read())
  if id:
    params['service_id'] = id

  Controller().init(**params).addBehavior("market",MarketMateInstance()).open()
  if noprompt:
    signal.signal(signal.SIGINT, signal_handler)
    print("")
    print("~~ Press Ctrl+C to kill .. ~~")
    while True:
      time.sleep(1)
  else:
    MarketMateInstance().cmdloop()

if __name__ == '__main__':
  # run()
  fire.Fire()