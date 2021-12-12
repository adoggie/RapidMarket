#coding:utf-8

"""
PosReceiver
1.接收来自策略系统分派的仓位信号，并转发给仓位订阅者

"""
import datetime
import threading
import traceback
import fire
import zmq
import os
import json
from typing import List
from elabs.fundamental.utils.useful import singleton,open_file
from elabs.trade.position import PositionUser
from elabs.app.core.command import parseMessage,PositionSignal

PWD = os.path.dirname(os.path.abspath(__file__))

@singleton
class PosReceiver(object):
  def __init__(self):
    self.cfgs = {}
    self.running = False
    self.ctx = zmq.Context()
    self.topic = ''
    self.users:List[PositionUser] = []  #
    self.thread = None

  def init(self,**kvs):
    self.cfgs.update(**kvs)
    return self

  def addUser(self,user:PositionUser):
    self.users.append(user)
    return self

  def open(self):
    self.sock = self.ctx.socket(zmq.SUB)
    prefix = self.cfgs.get('position_sub_topic_prefix','')
    if not prefix:
      prefix =  "2.0,{}:{}".format( self.cfgs.get('service_type') ,self.cfgs.get('service_id'))

    topic = f"{prefix},{PositionSignal.Type}"

    self.sock.setsockopt(zmq.SUBSCRIBE, topic.encode())  # 订阅
    addr = self.cfgs["position_broker_addr"]

    self.sock.connect(addr)
    self.thread = threading.Thread(target = self.recv_thread)
    self.thread.daemon = True
    self.thread.start()
    return self

  def recv_thread(self):
    self.running = True
    poller = zmq.Poller()
    poller.register(self.sock, zmq.POLLIN)

    while self.running:
      events = dict(poller.poll(1000))
      try:
        if self.sock in events:
          text = self.sock.recv()
          self.parse(text)
      except:
        traceback.print_exc()

  def parse(self,text):
    message = parseMessage(text)
    if isinstance(message,PositionSignal):
      for user in self.users:
        user.onPositionSignal(message)  # 回调给关注者仓位改变

  def log_pos(self,name,pos):
    if not self.cfgs.get("position_receiver.log",0):
      return
    fn = os.path.join(self.cfgs.get("position_receiver.log.path","pos"),"ps_{}.txt".format(name))
    fp = open_file(fn,'a')
    now = datetime.datetime.now()
    fp.write( "{} {} {} \n".format(str(now),name, pos) )
    fp.close()

  def close(self):
    self.running = False
    self.sock.close()
    return self

def test():
  FN = os.path.join(PWD, 'settings.json')
  kvs = json.loads(open(FN).read())
  PosReceiver().init(**kvs).open()
  PosReceiver().thread.join()

if __name__ == '__main__':
  fire.Fire()