#coding:utf-8
import datetime
import os,os.path
import json
import traceback
from dateutil.parser import parse
import flask
from flask import Flask,send_file
from flask import Response,request,redirect,url_for
from flask import render_template
from flask import Flask
# from elabs.fundamental.application import  instance
import pandas as pd
from bson import BSON
from bson.objectid import ObjectId
import base64
import config
from elabs.fundamental.utils.sign_and_aes import sign_check_and_get_data,sign_data,\
    aes_encode_ecb,aes_decode_ecb,simple_decrypt,simple_encrpyt
from elabs.fundamental.utils.timeutils import localtime2utc
from app import app
from flask import make_response
from functools import wraps, update_wrapper
from elabs.fundamental.http.webapi import CallReturn,ErrorReturn
import pymongo
import config

PWD = os.path.dirname(os.path.abspath(__file__))

def nocache(view):
    @wraps(view)
    def no_cache(*args, **kwargs):
        response = make_response(view(*args, **kwargs))
        response.headers['Last-Modified'] = datetime.datetime.now()
        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, post-check=0, pre-check=0, max-age=0'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '-1'
        return response

    return update_wrapper(no_cache, view)

PWD = os.path.dirname(os.path.abspath(__file__))
FN = os.path.join(PWD,  'settings.json')
cfgs = json.loads(open(FN).read())

def db_conn():
    conn = pymongo.MongoClient(**cfgs['mongodb'])
    return conn

"""
curl -X POST -d '{"service_id":"market01", "service_type":"market" }' http://127.0.0.1:5000/api/service/init -H 'Content-Type: application/json'
"""
@app.route('/api/service/init',methods=['POST'])
@nocache
def service_init():
    """
    parameters
      - service_id
      - service_type
      - timestamp
    """
    data = request.json
    service_id = data.get('service_id')
    service_type = data.get('service_type')

    FN = os.path.join(PWD, 'account.json')
    accounts = json.loads(open(FN).read())
    secret_key = accounts.get(service_id, '')
    if not secret_key:
        return ErrorReturn(112, 'Parameter Invalid!').response

    ok,data = sign_check_and_get_data(data , secret_key)
    if not ok:
        return ErrorReturn(111,'signature check error, Please check service id or secret_key!').response

    fn = os.path.join(PWD, f"./repo/{service_id}.json")
    params = json.loads(open(fn).read())
    jdata = json.dumps(params)
    cipher_text = simple_encrpyt(secret_key,jdata.encode()).decode()
    cr = CallReturn(result = cipher_text)
    return cr.response

"""

curl -X POST -d '{"service_id":"market01", "service_type":"market" }' http://127.0.0.1:5000/api/service/init -H 'Content-Type: application/json'
"""
@app.route('/api/market/symbol',methods=['POST'])
@nocache
def market_symbol_update():
    """
    行情服务上报 交易对
    content:  application/json
    parameters
      - service_id
      - service_type
      - timestamp   unix timestamp  (ms )
      - exchange
      - tt
        - spot [btc/usdt,eth/ustd]
        - swap [ ... ]
    """
    data = request.json
    service_id = data.get('service_id')
    service_type = data.get('service_type')
    exchange = data.get('exchange')
    tt = data.get('tt')
    if not service_id  or not service_type or not exchange or not tt:
        return ErrorReturn(101,'Parameter Invalid!').response
    data['uptime'] = datetime.datetime.now()

    db = db_conn()['RapidMarket']
    coll = db['ExchangeSymbol']
    coll.update_one(dict( exchange= exchange), {'$set': data}, upsert=True)

    return CallReturn().response


@app.route('/api/market/symbol',methods=['GET'])
@nocache
def market_symbol_get():
    """
    查询 交易所的交易对信息
    content:  application/x-www-form-urlencoded
    parameters
      - exchange
      - tt
    return:
      - exchange
      - tt
        - spot [btc/usdt,eth/ustd]
        - swap [ ... ]
      - timestamp   unix timestamp  (ms )
    """
    data = request.values
    exchange = data.get('exchange')

    db = db_conn()['RapidMarket']
    coll = db['ExchangeSymbols']
    rs = []
    if exchange:
        r = coll.find_one(dict( exchange=exchange))
        rs = [r]
    else:
        rs = coll.find({}).sort('exchange',1)
    rs = list(rs)
    for r in rs:
        del r['_id']
        r['timestamp'] = str(r.get('timestamp',''))
        r['uptime'] = localtime2utc( r['uptime'] ).timestamp() * 1000

    cr = CallReturn(result = rs)
    return cr.response