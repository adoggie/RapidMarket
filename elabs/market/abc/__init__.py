#coding:utf-8

import threading,time,datetime,traceback,random
from elabs.app.core.base import MarketBase
from elabs.app.core.message import KLine,Tick,OrderBook,ExchangeType,TradeType
from elabs.app.core.command import ServiceAlarmData,ExchangeSymbolUp
from elabs.app.core import logger
from elabs.app.core.controller import Controller

class MyMarketImpl(MarketBase):
    def __init__(self):
        MarketBase.__init__(self)

    def init(self,**kvs):
        MarketBase.init(self,**kvs)

    def work_thread(self):
        self.running = True
        while self.running:
            time.sleep(1)
            # kline = KLine.rand_one()
            # Controller().onKline(kline)
            remainder = random.randint(1,10) %7
            if remainder == 0:
                kline = KLine.rand_one()
                Controller().onKline(kline)
            elif remainder == 1:
                ob = OrderBook.rand_one()
                Controller().onOrderBook(ob)
            elif remainder == 2:
                tick = Tick.rand_one()
                Controller().onTick(tick)
            elif remainder == 3 :
                Controller().keep_alive( **dict(time=int(time.time()),cat='reality'))
            elif remainder == 4:
                logger.info("market:abc test loopping...")
            elif remainder == 5:
                # 发送报警
                ad = ServiceAlarmData.rand_one()
                Controller().send_message(ad)
            elif remainder == 6:
                # 发送交易所交易对信息 symbols
                es = ExchangeSymbolUp.rand_one()
                # Controller().send_message(es)

        logger.debug("stoping market service thread...")

    def open(self):
        self.thr = threading.Thread(target=self.work_thread)
        self.thr.daemon = True
        self.thr.start()

    def close(self):
        self.running = False
        self.thr.join()


