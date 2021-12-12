#coding:utf-8

from elabs.app.core.message import Tick,KLine,OrderBook
from elabs.app.core.command import PositionSignal

class MarketBase(object):
    def __init__(self):
        self.cfgs = {}

    def init(self,**kvs):
        self.cfgs.update(**kvs)

    def open(self):
        pass

    def close(self):
        pass


class TradeBase(object):
    def __init__(self):
        pass

    def init(self,**kvs):
        pass

    def open(self):
        pass

    def close(self):
        pass

    def onTick(self, tick: Tick):
        pass

    def onKline(self, kline: KLine):
        pass

    def onOrderBook(self, orderbook: OrderBook):
        pass

    def onPositionSignal(self, pos: PositionSignal):
        pass