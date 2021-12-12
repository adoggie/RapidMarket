#coding:utf-8

'''
pip install pymssql pymysql
https://pypi.org/project/pymssql/
'''

"""
RegistryClient 
1. 注册服务到服务中心 
2. 接收管理平台发送的控制指令并操作 
3. 上报服务运行状态 

- http 连接 registry server
- 连接mx，从管理系统接收控制消息，发送消息到监控平台 
"""

import fire
import json
import time
import datetime
import os
import traceback,base64,threading
import requests,zmq
from elabs.fundamental.utils.sign_and_aes import sign_check_and_get_data,sign_data,\
  aes_encode_ecb,aes_decode_ecb,simple_encrpyt,simple_decrypt

from elabs.fundamental.utils.useful import singleton
from elabs.fundamental.utils.timeutils import  localtime2utc
from elabs.app.core.command import parseMessage,\
  ServiceStatusRequest,ServiceStatusReport,\
  ServiceKeepAlive,PositionSignal,ServiceLogText,CommandBase
from elabs.app.core import logger

@singleton
class RegistryClient(object):
  def __init__(self):
    self.cfgs = {}
    self.ctx = zmq.Context()
    self.sock_s = self.ctx.socket(zmq.SUB)
    self.sock_p = self.ctx.socket(zmq.PUB)
    self.start = datetime.datetime.now()
    self.regOk = False
    self.user = None

  def getConfigs(self):
    return self.cfgs

  def init(self,**kvs):
    self.cfgs.update(**kvs)
    return self

  def addUser(self,user):
    self.user = user
    return self

  def register(self):
    logger.debug("try to register..")
    retry = True
    service_type = self.cfgs.get("service_type","market")
    registry_url = self.cfgs.get("registry_url",f"http://localhost:9700")
    registry_url = "{}/api/service/init".format(registry_url)
    params = dict(
      service_id = self.cfgs.get("service_id"),
      service_type = self.cfgs.get("service_type"),
      timestamp = int(localtime2utc().timestamp()*1000)
    )
    secret_key = self.cfgs.get("secret_key")
    data = sign_data(params,secret_key)

    try:
      res = requests.post(registry_url,json=data,timeout=5000).json()
      if res['status'] !=0:
        retry = False
        logger.error("register error return. status:",str(res))
        return False,retry

      # register okey ,获得加密配置信息
      result = res['result']
      # ok,data = sign_check_and_get_data(result,secret_key)
      raw_text = simple_decrypt(secret_key,result).decode()
      if not raw_text:
        logger.error("registry_client: simple_decrypt() failed!")
        return False,False
      data = json.loads(raw_text)
      self.cfgs.update(** data)
    except:
      traceback.print_exc()
      logger.error("register exception:", str(traceback.format_exc()))
      return False,True

    return True,False

  def open(self):
    """
      1.连接到registry server，登录注册获得运行配置参数, 循环注册，直到成功或者尝试次数达到上限
      params:
        - registry_retry_times = 5  注册重试次数
        - registry_retry_wait = 5   等待时间
        - registry_url =  xxx  注册服务器地址
    """

    # prepare zmq publish
    topic = "2.0,{}:{}".format( self.cfgs.get('service_type') ,self.cfgs.get('service_id'))
    self.sock_s.setsockopt(zmq.SUBSCRIBE, topic.encode('utf-8'))
    addr = self.cfgs.get('system_broker_addr')
    self.sock_s.connect(addr) # 接收
    self.sock_p.connect(addr) # 发送

    logger.debug("RegClient open()")

    if self.cfgs.get('registry_client.enable', 1):
      retry_times = self.cfgs.get('registry_retry_times', 50000000)
      wait = self.cfgs.get("registry_retry_wait", 5)
      while retry_times :
        ok,retry = self.register()
        if ok:
          break
        if not ok :
          time.sleep( wait )
        retry_times -=  1

      if retry_times == 0:
        return False

    self.thread = threading.Thread(target=self.recv_thread)
    self.thread.daemon = True
    self.thread.start()

    self.regOk = True
    return True

  def recv_thread(self):
    self.running = True
    poller = zmq.Poller()
    poller.register(self.sock_s, zmq.POLLIN)
    while self.running:
      text = ''
      try:
        events = dict(poller.poll(1000))
        if self.sock_s in events:
          text = self.sock_s.recv_string()
          self.parse(text)
      except:
        traceback.print_exc()
        print(text)
        time.sleep(.1)

  def parse(self,text):
    """收到来自 管理系统的命令请求 """

    message = parseMessage( text )
    if  message:
      self.dispatch(message)
      if self.user:
        self.user.onRegClientMessage(message)

  def dispatch(self,message):
    if isinstance(message,ServiceStatusRequest):
      # 请求状态查询
      self.onServiceQuery(message)

  def onServiceQuery(self,req):
    """服务查询"""
    resp = ServiceStatusReport()

    resp.from_id = self.cfgs.get("service_id")
    resp.from_service = self.cfgs.get("service_type")
    resp.service_type = self.cfgs.get('service_type')
    resp.service_id = self.cfgs.get("service_id")
    resp.start = int( localtime2utc(self.start).timestamp()*1000)
    resp.pid = os.getpid()
    resp.now = int( localtime2utc().timestamp()* 1000)
    self.send_message(resp)
    # text = resp.marshall()
    # self.sock_p.send( text.encode())


  def keep_alive(self,**kvs):
    """服务保活"""
    resp = ServiceKeepAlive()
    resp.from_id = self.cfgs.get("service_id")
    resp.from_service = self.cfgs.get("service_type")
    resp.service_type = self.cfgs.get('service_type')
    resp.service_id = self.cfgs.get("service_id")
    resp.start = int(localtime2utc(self.start).timestamp()*1000)
    resp.pid = os.getpid()
    resp.now = int( localtime2utc().timestamp()*1000)
    resp.params = kvs
    self.send_message(resp)
    # text = resp.marshall()
    # self.sock_p.send(text.encode())

  def send_log(self,log:ServiceLogText):
    self.send_message(log)
    # text = log.marshall()
    # self.sock_p.send(text.encode())

  def send_message(self,message:CommandBase):
    message.from_service = self.cfgs.get('service_type','')
    message.from_id = self.cfgs.get('service_id','')

    text = message.marshall()

    self.sock_p.send(text.encode())

  def close(self):
    return self

def test ():
  pass
  # RegistryClient().init( **kvs).open()

if __name__ == '__main__':
  fire.Fire()


"""
薛岳，李宗仁，卫立煌，张自忠，戴安澜，傅作义
"""
