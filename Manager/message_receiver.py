#coding:utf-8

"""
MessageReceiver
1.行情消息接收服务
2. tick, kline, orderbook,order ,trade
"""

import threading
import time, traceback
import json
from elabs.fundamental.utils.useful import object_assign, singleton
from elabs.app.core.command import  *

import zmq

@singleton
class MessageReceiver(object):
    """"""
    def __init__(self):
        self.cfgs = {}
        self.users = []
        self.broker = None
        self.ctx = zmq.Context()
        self.sock = self.ctx.socket(zmq.SUB)
        self.running = False

    def init(self,**cfgs):
        '''
         market_topic :
         market_broker_addr :
        '''
        self.cfgs.update(**cfgs)
        topic = self.cfgs.get('message_topic','')
        if isinstance(topic,(tuple,list)):
            for tp in topic:
                tp = tp.encode()
                self.sock.setsockopt(zmq.SUBSCRIBE, tp)
        else:
            topic =  topic.encode()
            self.sock.setsockopt(zmq.SUBSCRIBE,topic)  # 订阅所有
        addr = self.cfgs.get('system_broker_addr')
        self.sock.connect( addr )
        return self

    def addUser(self,user):
        self.users.append(user)
        return self

    def _recv_thread(self):
        self.running = True
        poller = zmq.Poller()
        poller.register(self.sock, zmq.POLLIN)
        while self.running:
            text = ''
            try:
                events = dict(poller.poll(1000))
                if self.sock in events:
                    text = self.sock.recv_string()
                    self.parse(text)
            except:
                traceback.print_exc()
                print(text)
                time.sleep(.1)

    def parse(self,text):
        message = parseMessage(text)
        for user in self.users:
            if isinstance(message,ServiceKeepAlive):
                user.onServiceKeepAlive(message)
            elif isinstance(message,ServiceLogText):
                user.onServiceLogText(message)
            elif isinstance(message,ServiceAlarmData):
                user.onServiceAlarmData(message)
            elif isinstance(message,ExchangeSymbolUp):
                user.onExchangeSymbolUp(message)



    def open(self):
        self.thread = threading.Thread(target=self._recv_thread)
        self.thread.daemon = True
        self.thread.start()
        return self

    def close(self):
        self.running = False
        self.sock.close()
        #self.thread.join()

    def join(self):
        self.thread.join()

