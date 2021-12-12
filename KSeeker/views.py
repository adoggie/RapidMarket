#coding:utf-8
import datetime
import os,os.path,json
from dateutil.parser import parse
from flask import Response,request,redirect,url_for

from app import app
from flask import make_response
from functools import wraps, update_wrapper
from elabs.fundamental.http.webapi import CallReturn
import pymongo

PWD = os.path.dirname(os.path.abspath(__file__))
FN = os.path.join(PWD,  'settings.json')
cfgs = json.loads( open(FN).read() )

def db_conn():
    conn = pymongo.MongoClient(**cfgs['mongodb'])
    return conn

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


# """
# http://localhost:9991/api/kseeker/kline?exchange=ftx&tt=spot&period=1&symbol=btc/usdt&start=2021-01-10&end=2021-12-23&num=10
# """
@app.route('/api/kseeker/kline',methods=['POST','GET'])
# @nocache
def get_kline():
    """
    parameters
      - exchange  交易所
      - tt        交易类型
      - period    周期 1m
      - symbol    交易对            √

      - start     开始时间 [optional]
      - end       结束时间 [optional] ，默认当前时间
      - num       数量 , 默认 ： 100
      - fs        查询返回字段 fs=DT,TS,O,H,L,C,V,AMT,TRAN,MKR,BV,BAMT
    """
    data = request.values
    # print(data)
    exchange = data['exchange'].upper()
    symbol = data['symbol']
    symbol = symbol.replace("/", "_")
    tt = data.get('tt','')
    fs = data.get('fs','DT,TS,O,H,L,C,V,OPI')
    num = request.values.get('num',0,int)
    start = data.get('start','')
    end = data.get('end','')
    if not end:
        end = str(datetime.datetime.now())
    end = parse(end )
    # end = datetime.datetime.utcfromtimestamp( end.timestamp())
    # if not start:
    #     start = str(datetime.datetime.now())
    if start:
        start = parse(start)
        # start = datetime.datetime.utcfromtimestamp( start.timestamp())

    period = '1'
    name = f"{tt}_{period}_{symbol}"
    db = db_conn()[exchange]
    # print(db.list_collection_names())
    coll = db[name]
    # rs = list(coll.find())

    # coll = db_conn()['FTX']['spot_1_btc_usdt']

    fs = list(map(lambda  s:s.strip().upper() , fs.split(',')))

    queries = dict( DT = {'$lte':end})
    fs_prj = {}
    for f in fs:
        fs_prj[f] = 1
    rs = []
    # rs = list(coll.find())
    # rs = list(rs)
    if not num:
        num = 100
    if start:
        queries['DT']['$gte']  = start
        # rs = coll.find(queries,fs_prj).sort('TS',-1).limit(num)
    # else:
    #     rs = coll.find(queries, fs_prj).limit(num)
    rs = coll.find(queries, fs_prj).sort('TS', -1).limit(num)

    result = {}
    for  f in fs:
        result[f] = []
    rs = list(rs)
    rs.reverse()
    for r in rs:
        for f in fs :
            v = r[f]
            if f == 'DT':
                v = str(v).split('.')[0]
            result[f].append( v )

    cr = CallReturn(result = result)
    return cr.response

