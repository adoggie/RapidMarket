#coding:utf-8

import fire
import json
import time
import datetime
import os
import traceback,base64,threading
import requests
import fire

base_url = ''
def get_exchange_symbols(exchange='',tt='',url=''):
    """查询交易所交易对信息
    :parameter  exchange  交易所名称
            tt  交易类型  spot,swap,futures
    """
    global base_url
    if url:
        base_url = url
    if base_url.find('/api/market/symbol') == -1:
        base_url = base_url + '/api/market/symbol'
    # print(url,base_url)
    data = dict(exchange = exchange)
    result = requests.get(base_url,data,timeout=5000).json()
    return result['result']


def get_symbol_names(exchange,tt,url='http://172.16.10.253:17027'):
    """
    python -m elabs.app.core.registry get_symbol_names bsc spot
    """
    exs = get_exchange_symbols(exchange, tt, url)
    result = []
    for ex in exs:
        if ex['exchange'] == exchange:
            result = ex.get('tt',{}).get(tt,{}).keys()
    result = list(result)
    result.sort()
    return result

def test_get_ex_symbols():
    # return get_exchange_symbols('bsc','spot',url='http://172.16.10.253:17027')
    print( get_symbol_names('bsc','spot',url='http://172.16.10.253:17027') )



if __name__ == '__main__':
    fire.Fire()