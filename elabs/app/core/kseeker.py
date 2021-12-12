#coding:utf-8

"""
kseeker.py

client for KSeeker
"""

import os,os.path,time,datetime,traceback,sys,json
from dateutil.parser import parse

import fire
import requests
import pandas as pd

base_url = ''

def get_kline(symbol,start=None, end=None,num=0, exchange='ftx',
              tt='spot',period='1m',pdf=True,url='',fs=''):
  global base_url
  if url:
    base_url = url
  if base_url.find('/api/kseeker/kline') == -1:
    base_url = base_url+'/api/kseeker/kline'

  params = dict(exchange = exchange,tt = tt,symbol = symbol,period=period,num = num )

  data = requests.get(base_url,data=params,timeout=10000).json()
  result = data['result']
  if pdf :
    result = pd.DataFrame(result)
  return result


def test_get_kline():
  global base_url
  base_url = "http://f4:17028/api/kseeker/kline"
  df = get_kline('btc_usdt',exchange='ftx')
  print(df )


if __name__ == '__main__':
    fire.Fire()