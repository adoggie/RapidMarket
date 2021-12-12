#coding:utf-8

"""
Manager　
接收各个服务发送的日志消息、心跳消息，写入数据库

"""
import os
import json
import time
import datetime
from threading import Thread
import signal
import sys
import fire
import cmd
import pymongo
import pytz
import zmq
from elabs.fundamental.utils.useful import singleton,input_params
from elabs.fundamental.utils.timeutils import localtime2utc,utctime2local
from elabs.app.core.controller import Controller
from elabs.app.core.behavior import Behavior
from elabs.app.core import logger
from elabs.app.core.registry_client import RegistryClient
from elabs.app.core.command import ServiceKeepAlive,ServiceLogText,ServiceAlarmData,ExchangeSymbolUp

from elabs.utils.useful import Timer
from elabs.app.core.message import  *
from message_receiver import MessageReceiver


PWD = os.path.dirname(os.path.abspath(__file__))

@singleton
class Manager(Behavior,cmd.Cmd):

  prompt = 'Manager > '
  intro = 'welcome to elabs..'
  def __init__(self):
    Behavior.__init__(self)
    cmd.Cmd.__init__(self )
    self.running = False
    self.conn = None
    self.timer = None
    self.ctx = zmq.Context()
    self.sock = self.ctx.socket(zmq.SUB)
    self.running = False

  def init(self,**kvs):
    Behavior.init(self,**kvs)

    # if self.cfgs.get('registry_client.enable', 1):
    RegistryClient().init(**kvs)
    MessageReceiver().init(**kvs).addUser(self)

    interval = self.cfgs.get('keep_alive_interval',5)
    self.timer = Timer(self.keep_alive,interval)
    return self

  def keep_alive(self,**kvs):
    """定时上报状态信息"""
    Controller().keep_alive()

  def open(self):
    # if self.cfgs.get('registry_client.enable', 1):
    RegistryClient().open()
    MessageReceiver().open()
    return self

  def close(self):
    self.running = False
    self.thread.join()

  def onServiceKeepAlive(self, m: ServiceKeepAlive):
    """ serialize into file and nosql db """
    logger.debug("ServiceKeepAlive :", m.marshall())
    data = dict(
      service_type = m.service_type,
      service_id = m.service_id,
      pid = m.pid,
      start = utctime2local( datetime.datetime.fromtimestamp( int(m.start)/1000) ),
      now = utctime2local( datetime.datetime.fromtimestamp( int(m.now)/1000) ),
      params = m.params,
      uptime = datetime.datetime.now()
    )
    conn = self.db_conn()
    db = conn['ServiceKeepAlive']
    coll = db[f"{m.service_type}_{m.service_id}"]
    coll.insert_one( data )
    coll.create_index([('uptime',1)])


  def onServiceLogText(self,m: ServiceLogText):
    logger.debug("ServiceLogText :", m.marshall())
    data = dict(
      service_type=m.from_service,
      service_id=m.from_id,
      timestamp = utctime2local( datetime.datetime.fromtimestamp(m.timestamp/1000) ),
      level= m.level,
      text = m.text,
      uptime=datetime.datetime.now()
    )
    conn = self.db_conn()
    db = conn['ServiceLogText']
    coll = db[f"{m.from_service}_{m.from_id}"]
    coll.insert_one(data)
    coll.create_index( [('uptime',1)])

  def onServiceAlarmData(self,m: ServiceAlarmData):
    logger.debug("onServiceAlarmData :", m.marshall())
    data = dict(
      service_type=m.from_service,
      service_id=m.from_id,
      timestamp = utctime2local( datetime.datetime.fromtimestamp(m.timestamp/1000) ),
      type= m.type,
      level = m.level,
      tag = m.tag,
      detail = m.detail,
      data = m.data,
      uptime=datetime.datetime.now()
    )
    conn = self.db_conn()
    db = conn['ServiceAlarmData']
    coll = db[f"{m.from_service}_{m.from_id}"]
    coll.insert_one(data)
    coll.create_index([('uptime',1)])

  def onExchangeSymbolUp(self,m: ExchangeSymbolUp):
    logger.debug("onExchangeSymbolUp :", m.marshall())
    data = dict(
      service_type=m.from_service,
      service_id=m.from_id,
      timestamp = utctime2local( datetime.datetime.fromtimestamp(m.timestamp/1000) ),
      exchange= m.exchange,
      tt = m.tt,
      uptime=datetime.datetime.now()
    )
    conn = self.db_conn()
    db = conn['RapidMarket']
    coll = db["ExchangeSymbols"]
    coll.update_one(dict(exchange=m.exchange), {'$set': data}, upsert=True)
    coll.create_index([('uptime',1)])


  def db_conn(self):
    if not self.conn:
      self.conn = pymongo.MongoClient(**self.cfgs['mongodb'])
    return self.conn

  def do_exit(self,*args):
    Controller().close()
    print('bye bye!')
    return True

  def do_show(self,line):
    args = input_params(line,['pos'])
    if args:
     pass

  def signal_handler(signal, frame):
    sys.exit(0)

def signal_handler(signal,frame):
  Controller().close()
  print('bye bye!')
  sys.exit(0)

#------------------------------------------
FN = os.path.join(PWD,  'settings.json')

def run(id = '',fn='',noprompt=False):
  global FN
  if fn:
    FN = fn
  params = json.loads(open(FN).read())
  if id:
    params['service_id'] = id

  Controller().init(**params).addBehavior("market",Manager()).open()
  if noprompt:
    signal.signal(signal.SIGINT, signal_handler)
    print("")
    print("~~ Press Ctrl+C to kill .. ~~")
    while True:
      time.sleep(1)
  else:
    Manager().cmdloop()


if __name__ == '__main__':
  run()
  # fire.Fire()