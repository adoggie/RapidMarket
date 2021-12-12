#coding:utf-8


"""

ver,dest_service:dest_id,from_service:from_id,msg_type

1.0,market:001,manager:m01,status_query,plain,text
1.0,market:001,manager:m01,status_query,base64:json,

"""

import datetime,json,base64
import os
import time
import traceback
# from elabs.fundamental.utils.useful import utc_timestap
from elabs.fundamental.utils.timeutils import localtime2utc

from elabs.app.core.message import TradeType,ExchangeType

class CommandBase(object):
  Type = ''
  def __init__(self):
    self.ver = '2.0'
    self.dest_service = 'manager' # 中心管理服务
    self.dest_id = 'manager'
    self.from_service = ''
    self.from_id = ''
    self.timestamp = int(localtime2utc().timestamp()*1000)
    self.msg_type = self.Type
    self.encode = 'plain'

  def body(self):
    return ''

  def marshall(self):
    signature = 'nosig'
    text = f"{self.ver},{self.dest_service}:{self.dest_id},{self.msg_type}," \
           f"{self.from_service}:{self.from_id},{self.timestamp}," \
           f"{signature},{self.encode},{self.body()}"
    return text


class PositionSignal(CommandBase):
  """仓位信号"""
  Type = 'position_signal'
  def __init__(self):
    CommandBase.__init__(self)
    self.exchange = ''
    self.account = ''
    self.tt = TradeType.SPOT[1]
    self.symbol = ''
    self.pos = 0
    self.datetime = localtime2utc().timestamp()
    self.timestamp = localtime2utc().timestamp()
    self.encode = 'base64:json'

  def body(self):
    data = dict(exchange = self.exchange,
                account = self.account,
                tt = self.tt,
                symbol = self.symbol,
                pos = self.pos,
                datetime = self.datetime)
    text = base64.b64encode(json.dumps(data).encode()).decode()
    return text

  @classmethod
  def parse(cls,data):
    m = cls()
    m.exchange = data.get('exchange')
    m.account = data.get('account')
    m.tt = data.get('tt')
    m.symbol = data.get('symbol')
    m.pos = data.get('pos')
    m.datetime = data.get('datetime')
    return m

  @staticmethod
  def rand_one():
    ps = PositionSignal()
    ps.exchange = ExchangeType.Binance[1]
    ps.account = "acc001"
    ps.tt = TradeType.SPOT[1]
    ps.symbol = 'btc/ustd'
    ps.pos = 11
    ps.datetime = localtime2utc().timestamp()
    return ps

class ExchangeSymbolUp(CommandBase):
  """仓位信号"""
  Type = 'exg_symbol_up'
  def __init__(self):
    CommandBase.__init__(self)
    self.exchange = ''
    self.tt = {}    # { spot: [...] , swap:[...] }

    self.timestamp = int(localtime2utc().timestamp()*1000)
    self.encode = 'base64:json'

  def body(self):
    data = dict(exchange = self.exchange,
                tt = self.tt)
    text = base64.b64encode(json.dumps(data).encode()).decode()
    return text

  @classmethod
  def parse(cls,data):
    m = cls()
    m.exchange = data.get('exchange')
    m.tt = data.get('tt')
    return m

  @staticmethod
  def rand_one():
    m = ExchangeSymbolUp()
    m.exchange = ExchangeType.Binance[1]
    m.tt = {
     "spot":{"btc/usdt": [1, 0.0001, 100000, 10000, 100, 0.0001],
         "eth/usdt": [1, 0.001, 5000, 1000, 1000, 0.001]},
     }
    return m

class ServiceLogText(CommandBase):
  """服务消息"""
  Type = 'svc_log'
  def __init__(self):
    CommandBase.__init__(self)
    self.timestamp = 0
    self.level = 'D'
    self.text = ''
    self.encode = 'base64:json'

  def body(self):
    data = dict(timestamp = self.timestamp,
                level = self.level,
                text = self.text
               )
    text = base64.b64encode(json.dumps(data).encode()).decode()
    return text

  @classmethod
  def parse(cls,data):
    m = cls()
    m.timestamp = data.get('timestamp',0)
    m.level = data.get('level','')
    m.text = data.get('text','')
    return m

  @classmethod
  def rand_one(cls):
    ps = cls()
    ps.timestamp = int(localtime2utc().timestamp()*1000)
    ps.level = "I"
    ps.text = "system busy!"
    return ps

class ServiceAlarmData(CommandBase):
  """服务消息"""
  Type = 'alarm'
  def __init__(self):
    CommandBase.__init__(self)
    self.type = 'app'
    self.level = 0
    self.tag = ''
    self.detail = ''
    self.data = {}
    self.encode = 'base64:json'

  def body(self):
    data = dict(type = self.type,
                level = self.level,
                tag = self.tag,
                detail = self.detail,
                data = self.data
               )
    text = base64.b64encode(json.dumps(data).encode()).decode()
    return text

  @classmethod
  def parse(cls,data):
    m = cls()
    m.type = data.get('type',0)
    m.level = data.get('level',0)
    m.tag = data.get('tag','')
    m.detail = data.get('detail','')
    m.data = data.get('data',{})
    return m

  @classmethod
  def rand_one(cls):
    ps = cls()
    # ps.type =
    ps.level = 1
    ps.tag = "red"
    ps.detail = "wwwwww..."
    ps.data = dict(name='abc',water=99)
    return ps



class ServiceStatusRequest(CommandBase):
  """服务状态查询"""
  Type = 'service_status_request'
  def __init__(self):
    CommandBase.__init__(self)

  @classmethod
  def parse(cls,data):
    m = cls()
    return m

  @staticmethod
  def rand_one():
    m = ServiceStatusRequest()
    return m


class ServiceStatusReport(CommandBase):
  """上报本服务状态信息"""
  Type = 'service_status_report'
  def __init__(self):
    CommandBase.__init__(self)
    self.service_type = ''
    self.service_id = ''
    self.pid = 0
    self.start = 0
    self.params = {}    # 附加的服务状态信息
    self.encode = 'base64:json'
    self.now = 0
    self.ip = ''

  def body(self):
    data = dict(service_type= self.service_type,
                service_id = self.service_id,
                now = self.now,
                pid = self.pid,
                start = self.start,
                params = self.params
                )
    text = base64.b64encode(json.dumps(data).encode()).decode()
    return text

  @classmethod
  def parse(cls,data):
    m = cls()

    m.service_type = data.get('service_type')
    m.service_id = data.get('service_id')
    m.pid = data.get('pid')
    m.start = data.get('start')
    m.now = data.get('now')
    m.params = data.get('params',{})
    return m

  @classmethod
  def rand_one(cls):
    m = cls()
    m.service_type = 'market'
    m.service_id = 'market01'
    m.pid = 10021
    m.start = int(localtime2utc().timestamp()*1000)
    m.now = m.start
    m.params = {'k':1001}
    return m


class ServiceKeepAlive(ServiceStatusReport):
  Type = 'service_keep_alive'
  def __init__(self):
    ServiceStatusReport.__init__(self)

class KlineAttach(CommandBase):
  """请求kline补偿"""
  Type = 'kline_attach'
  def __init__(self):
    CommandBase.__init__(self)
    self.exchange = ''
    self.tt = ''
    self.symbol = ''
    self.period = 1
    self.start = 0
    self.end = 0

    self.timestamp = int(localtime2utc().timestamp()*1000)
    self.encode = 'base64:json'

  def body(self):
    data = dict(exchange = self.exchange,
                tt = self.tt,
                symbol = self.symbol,
                period = self.period,
                start = self.start,
                end = self.end)
    text = base64.b64encode(json.dumps(data).encode()).decode()
    return text

  @classmethod
  def parse(cls,data):
    m = cls()
    m.exchange = data.get('exchange','')
    m.tt = data.get('tt','')
    m.symbol = data.get('symbol','')
    m.period = data.get('period',1)
    m.start = int(data.get('start',0))
    m.end = int( data.get('end',0))

    return m

  @staticmethod
  def rand_one():
    m = KlineAttach()
    m.exchange = ExchangeType.FTX[1]
    m.tt = 'swap'
    m.symbol = 'btc/usdt'
    m.start = int( (localtime2utc().timestamp() - 100) * 1000)
    m.end = int( localtime2utc().timestamp() * 1000)

    return m


MessageDefinedList = [
  ServiceStatusRequest,
  ServiceStatusReport,
  PositionSignal,
  ServiceKeepAlive,
  ServiceLogText,
  ServiceAlarmData,
  ExchangeSymbolUp,
  KlineAttach
]

def parseMessage(text):
  """解析消息报文"""
  fs = text.split(',')
  if len(fs) < 8:
    return None
  ver ,dest,msg_type,from_,timestamp,signature,encode,body,*others = fs
  if ver !='2.0':
    return None
  m = None
  for md in MessageDefinedList:
    m = None
    try:
      if md.Type == msg_type:
        encs= encode.split(':')
        for enc in encs:
          if enc == 'base64':
            body = base64.b64decode(body)
          elif enc == 'json':
            body = json.loads(body)

        m = md.parse(body)
        m.ver = ver
        m.dest_service, m.dest_id = dest.split(':')
        m.msg_type = msg_type
        m.from_service, m.from_id = from_.split(':')
        m.timestamp = int(timestamp)
        m.signature = signature
    except:
      traceback.print_exc()
      m = None
    if m:
      break
  return m

def test_serde():
  print()
  text = PositionSignal.rand_one().marshall()
  print(text)
  m = parseMessage(text)
  text2 = m.marshall()
  print(text2)
  assert (text == text2)

  text = ServiceStatusRequest.rand_one().marshall()
  print(text)
  m = parseMessage(text)
  text2 = m.marshall()
  print(text2)
  assert (text == text2)

  text = ServiceStatusReport.rand_one().marshall()
  print(text)
  m = parseMessage(text)
  text2 = m.marshall()
  print(text2)
  assert (text == text2)

  text = ServiceKeepAlive.rand_one().marshall()
  print(text)
  m = parseMessage(text)
  text2 = m.marshall()
  print(text2)
  assert (text == text2)

  text = ServiceLogText.rand_one().marshall()
  print(text)
  m = parseMessage(text)
  text2 = m.marshall()
  print(text2)
  assert (text == text2)
