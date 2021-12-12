#coding:utf-8


""""
Behavior
1.基础服务定义，不同业务功能派生一个Behavior，添加入Controller并受其管理

"""
import datetime

from threading import RLock
from elabs.app.core.message import Tick,KLine,OrderBook
from elabs.app.core.command import PositionSignal

class Behavior(object):
  def __init__(self):
    self.cfgs = {}
    self.lock = RLock()

  def onTick(self,tick:Tick):
    pass

  def onKline(self,kline:KLine):
    pass

  def onOrderBook(self,orderbook:OrderBook):
    pass

  def init(self,**kvs):
    self.cfgs.update(**kvs)

  def stop(self):
    pass

  def onPositionSignal(self,pos:PositionSignal):
    pass