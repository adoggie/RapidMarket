#coding:utf-8

"""
make_pos_signal.py

模拟 仓位新号，发送到 mx ，报单服务接收进行下单aaasssdddddd
"""

from elabs.app.core.command import PositionSignal
from elabs.app.core.message import *
import time ,datetime,os,os.path,sys,traceback
import zmq
import fire
MX_PUB_ADDR= "tcp://127.0.0.1:15555"

ctx = zmq.Context()
sock = ctx.socket(zmq.PUB)
def position_generate():
    sock.connect(MX_PUB_ADDR)
    ps = PositionSignal()
    ps.exchange = ExchangeType.Binance[1]
    ps.symbol = 'btc/usdt'
    ps.pos = 111
    ps.dest_service = 'trade'
    ps.dest_id = 'trade'
    ps.from_service = 'test'
    ps.from_id = 'test'
    text = ps.marshall()
    sock.send(text.encode())
    print(text)

for n in range(100):
    position_generate()
    time.sleep(2)


