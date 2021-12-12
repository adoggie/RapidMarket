#coding:utf-8

from elabs.app.core.base import TradeBase
from elabs.app.core.message import Tick,KLine,OrderBook
from elabs.app.core.command import PositionSignal
from elabs.app.core import logger


class MyTradeImpl(TradeBase):
    """简单演示的发单模块"""
    def __init__(self):
        TradeBase.__init__(self)

    def init(self,**kvs):
        pass

    def open(self):
        pass

    def close(self):
        pass

    def onTick(self, tick: Tick):
        logger.debug("TradeImpl Got Tick: ", tick.marshall())

    def onKline(self, kline: KLine):
        logger.debug("TradeImpl Got KLine: ", kline.marshall())

    def onOrderBook(self, orderbook: OrderBook):
        logger.debug("TradeImpl Got OrderBook: ", orderbook.marshall())

    def onPositionSignal(self, pos: PositionSignal):
        logger.debug("TradeImpl Got PositionSignal: ", pos.marshall())
