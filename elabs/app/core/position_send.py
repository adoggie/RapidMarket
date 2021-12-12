#coding:utf-8

from elabs.app.core.command import PositionSignal
from elabs.app.core.message import *
import zmq
import fire
MX_PUB_ADDR= "tcp://127.0.0.1:15555"

class PositionSender(object):
    def __init__(self):
        self.cfgs = {}

    def init(self,**kvs):
        self.cfgs.update(**kvs)
        self.ctx = zmq.Context()
        self.sock = self.ctx.socket(zmq.PUB)
        return self

    def open(self):
        addr = self.cfgs.get('position_broker_addr',MX_PUB_ADDR)
        self.sock.connect(addr)
        return self

    def send(self,exchange:ExchangeType,tt:TradeType,symbol,pos,
             dest_service='trade' , dest_id = 'trade',
             from_service='stnode' , from_id = 'stdnode'):
        ps = PositionSignal()
        ps.exchange = exchange[1]
        ps.tt = tt[1]
        ps.symbol = symbol
        ps.pos = pos
        ps.dest_service = dest_service
        ps.dest_id = dest_id
        ps.from_service = self.cfgs.get('service_type',from_service)
        ps.from_id = self.cfgs.get('service_id',from_id)
        text = ps.marshall()
        self.sock.send(text.encode())

def test_send(pub_addr= "tcp://127.0.0.1:15555"):
    ps = PositionSender()
    ps.init(position_broker_addr=pub_addr).open()
    for n in range(100):
        time.sleep(1)
        ps.send(ExchangeType.FTX,TradeType.SPOT,'eth/usdt',n*10)