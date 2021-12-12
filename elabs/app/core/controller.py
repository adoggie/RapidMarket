#coding:utf-8

"""
Controller
1. 全局控制器，数据转换中继器，解偶

"""
from elabs.fundamental.utils.useful import singleton,utc_timestap
from elabs.app.core.market_receiver import MarketReceiver
from elabs.app.core.position_receiver import PosReceiver
from elabs.app.core.registry_client import RegistryClient
from elabs.app.core.logger import Logger
from elabs.app.core.message import Tick,KLine,OrderBook
from elabs.app.core.command import CommandBase,PositionSignal,ServiceLogText


@singleton
class Controller(object):
  def __init__(self):
    self.cfgs = {}
    self.behaviors = {}
    self.pos_reciever = None
    self.registry_client = None

  def getConfig(self):
    return self.cfgs

  def init(self,**kvs):
    Logger().init(**kvs).open()
    self.cfgs.update(**kvs)
    Logger().hook = self

    return self

  def log_write(self,level,text):
    log_level = self.cfgs.get('logger.level', 'DEBUG').upper()
    level_defs = ['DEBUG', 'INFO', 'WARN', 'ERROR']
    n1 = level_defs.index(log_level)
    n2 = level_defs.index(level)
    if n2 >= n1:
      log = ServiceLogText()
      log.from_service = self.cfgs.get('service_type')
      log.from_id = self.cfgs.get('service_id')

      log.timestamp = utc_timestap()
      log.level = level
      log.text = text
      RegistryClient().send_log(log)

  def open(self):
    for b in self.behaviors.values():
      b.init(**self.cfgs)

    for b in self.behaviors.values():
      b.open()

  def close(self):
    for b in self.behaviors.values():
      b.close()

  def addBehavior(self,name,behavior):
    self.behaviors[name] = behavior

    return self

  def onTick(self,tick:Tick):
    for b in self.behaviors.values():
      b.onTick(tick)

  def onKline(self,kline:KLine):
    for b in self.behaviors.values():
      b.onKline(kline)

  def onOrderBook(self,orderbook:OrderBook):
    for b in self.behaviors.values():
      b.onOrderBook(orderbook)

  def onPositionSignal(self,pos:PositionSignal):
    for b in self.behaviors.values():
      b.onPositionSignal(pos)

  def keep_alive(self,**kvs):
    # if self.cfgs.get("registry_client.enable"):
    RegistryClient().keep_alive(**kvs)

  def send_message(self,message:CommandBase):
    # if self.cfgs.get("registry_client.enable"):
    RegistryClient().send_message(message)
