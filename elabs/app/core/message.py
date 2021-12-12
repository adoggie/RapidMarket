#coding:utf-8

import datetime
import time
import traceback
import json,base64
import random
import pytz
# from elabs.fundamental.utils.useful import utc_timestap
from elabs.fundamental.utils.timeutils import localtime2utc

class ExchangeType(object):
    Undefined = (0, '')
    Okex = (1,'okex')
    Binance =(2,'bsc')
    FTX = (3,'ftx')
    all = [Okex,Binance,FTX]


class TradeType(object):
    Undefined = (0,'')
    SPOT = (1,'spot')
    SWAP = (2,'swap')

class MessageType(object):
    Undefined = (0, '')
    Tick = (1,'tk')
    KLine = (2,'kl')
    OrderBook = (3,'ob')
    Position = (6,'ps')

class RapidMessage(object):
    Type = ''
    def __init__(self):
        self.ver = '1.0'
        self.type = self.Type
        self.exchange = ExchangeType.Binance[1]
        self.tt = TradeType.SPOT[1]
        self.symbol = ''
        self.datetime = int(localtime2utc().timestamp()*1000)  # utc timestamp

    def marshall(self):
        raise NotImplementedError

    def body(self):
        return ''

class KLine(RapidMessage):
    Type = MessageType.KLine[1]
    def __init__(self):
        RapidMessage.__init__(self)
        self.period = '1m'
        self.open = 0
        self.high = 0
        self.low = 0
        self.close = 0
        self.vol = 0
        self.amt = 0
        self.opi = 0
        self.transactions = 0
        self.is_maker = None
        self.buy_vol = 0
        self.buy_amt = 0

    def marshall(self):
        fs = [  self.ver,self.type,self.exchange,self.tt,self.period,self.symbol,self.datetime,
                self.open,self.high,self.low,self.close,self.vol,self.amt,self.transactions,
                self.is_maker,self.buy_vol,self.buy_amt ]
        fs = ','.join( map(str,fs) )
        return fs

    @classmethod
    def parse(cls, fs):
        m = cls()
        m.ver,m.type,m.exchange,m.tt,m.period,m.symbol,m.datetime,\
        m.open,m.high,m.low,m.close,m.vol,m.amt,m.transactions,m.is_maker,\
        m.buy_vol,m.buy_amt, *others = fs
        m.period = m.period.replace('m','')
        m.period = int(m.period)
        m.datetime = int(float(m.datetime))
        if m.datetime < 1e+11:
            m.datetime = m.datetime * 1000
            # m.datetime = int( m.datetime/1000)
        # m.datetime = datetime.datetime.utcfromtimestamp(m.datetime)

        m.open = float(m.open)
        m.high = float(m.high)
        m.low = float(m.low)
        m.close = float(m.close)
        m.vol = float(m.vol)
        m.amt = float(m.amt)
        m.transactions = float(m.transactions)
        # m.is_maker = int(m.is_maker)
        m.buy_vol = float(m.buy_vol)
        m.buy_amt = float(m.buy_amt)
        return m

    #@property
    #def time(self):
    #    return datetime.datetime.fromtimestamp( self.datetime/1000, pytz.UTC)

    @staticmethod
    def rand_one():
        kline = KLine()
        kline.exchange = ExchangeType.FTX[1]
        kline.tt = TradeType.SPOT[1]
        kline.symbol = 'btc/usdt'
        kline.datetime = int(localtime2utc().timestamp()*1000)
        kline.open = random.randint(100,1000)
        kline.high = random.randint(100,1000)
        kline.low = random.randint(100,1000)
        kline.close = random.randint(100,1000)
        kline.vol = random.randint(100,1000)
        kline.amt = random.randint(100,1000)
        kline.transactions = random.randint(100,1000)
        kline.is_maker = 1
        kline.buy_vol = random.randint(100,1000)
        kline.buy_amt = random.randint(100,1000)
        return kline
#
# class Position(RapidMessage):
#     Type = MessageType.Position[1]
#     def __init__(self):
#         RapidMessage.__init__(self)
#         self.account = ''
#         self.pos = 0
#
#     def marshall(self):
#         fs = [  self.ver, self.type[0], self.exchange[0], self.account,self.tt[0],
#                 self.symbol,self.pos,self.datetime ]
#         fs = ','.join(map(str, fs))
#         return fs
#
#     @classmethod
#     def parse(cls, data):
#         m = cls()
#         m.exchange = data.get('exchange')
#         m.account = data.get('account')
#         m.tt = data.get('tt')
#         m.symbol = data.get('symbol')
#         m.pos = data.get('pos')
#         m.datetime = data.get('datetime')
#         return m

class Tick(RapidMessage):
    Type = MessageType.Tick[1]
    def __init__(self):
        RapidMessage.__init__(self)
        self.bid_price = 0
        self.bid_vol = 0
        self.ask_price = 0
        self.ask_vol =0

    def marshall(self):
        fs = [  self.ver, self.type, self.exchange, self.tt,
                self.symbol,self.datetime,self.bid_price,self.bid_vol,
                self.ask_price,self.ask_vol]
        fs = ','.join(map(str, fs))
        return fs

    @classmethod
    def parse(cls, fs):
        m = cls()
        m.ver, m.type, m.exchange, m.tt, m.symbol, m.datetime, \
        m.bid_price, m.bid_vol,m.ask_price,m.ask_vol, *others = fs
        return m

    @staticmethod
    def rand_one():
        tick = Tick()
        tick.symbol = 'btc/usdt'
        tick.bid_price = random.randint(100,1000)
        tick.bid_vol = random.randint(100,1000)
        tick.ask_price = random.randint(100,1000)
        tick.ask_vol = random.randint(100,1000)
        return tick

class OrderBook(RapidMessage):
    Type = MessageType.OrderBook[1]

    def __init__(self):
        RapidMessage.__init__(self)

    def marshall(self):
        fs = [  self.ver, self.type, self.exchange, self.tt,
                self.symbol,self.datetime ]
        fs = ','.join(map(str, fs))
        return fs

    @classmethod
    def parse(cls, fs):
        m = cls()
        m.ver, m.type, m.exchange, m.tt, m.symbol, m.datetime, *others = fs
        return m

    @staticmethod
    def rand_one():
        ob = OrderBook()
        ob.symbol = 'btc/usdt'
        return ob

MessageDefinedList = [
  Tick,
  KLine,
  # Position,
  OrderBook
]

def parseMessage(text)->RapidMessage:
    """解析消息"""
    fs = text.split(',')
    if len(fs) <= 3:
        return None
    ver, type_, *body = fs
    if ver != '1.0':
        return None
    m = None
    for md in MessageDefinedList:
        m = None
        try:
            if md.Type == type_:
                m = md.parse(fs)
        except:
            traceback.print_exc()
        if m:
            break
    return m

def test_serde():
    print()
    text = KLine.rand_one().marshall()
    print(text)
    m = parseMessage(text)
    text2 = m.marshall()
    print(text2)
    assert( text == text2)

    text = Tick.rand_one().marshall()
    print(text)
    m = parseMessage(text)
    text2 = m.marshall()
    print(text2)
    assert (text == text2)

    text = OrderBook.rand_one().marshall()
    print(text)
    m = parseMessage(text)
    text2 = m.marshall()
    print(text2)
    assert (text == text2)


