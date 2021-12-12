#coding:utf-8

"""
into_file.py
定时读取nosql存储的kline，转储到文件系统
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

from elabs.fundamental.utils.importutils import import_class
from elabs.fundamental.utils.useful import singleton,input_params
from elabs.app.core.controller import Controller
from elabs.app.core.behavior import Behavior
from elabs.app.core import logger
from elabs.app.core.registry_client import RegistryClient
from elabs.app.core.message import Tick,KLine,OrderBook
from elabs.app.core.market_receiver import MarketReceiver

PWD = os.path.dirname(os.path.abspath(__file__))

#------------------------------------------
FN = os.path.join(PWD,  'settings.json')

params = {}
def db_conn():
  conn = pymongo.MongoClient(**params['mongodb'])
  return conn

def run(fn='',noprompt=False):
  global FN,params
  if fn:
    FN = fn
  params = json.loads(open(FN).read())

  #
  conn = db_conn()
  now = datetime.datetime.now()
  s = now - datetime.timedelta(seconds=params['data_dump_elapsed'])
  start = datetime.datetime(s.year,s.month,s.day,s.hour)







if __name__ == '__main__':
  fire.Fire()