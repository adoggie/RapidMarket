#coding:utf-8

"""
check-mem.py

检查 sdm 的kline是否存在空洞

"""

import datetime
import json
import sys,os
import traceback
import fire
from dateutil.parser import parse

from  elabs.bitpool.BitPool import client,make_shm_id,get_data

PWD = os.path.dirname(os.path.abspath(__file__))
FN = os.path.join( os.path.dirname( os.path.abspath(__file__) ) , 'settings.json')
cfgs = json.loads( open(FN).read())


def check_miss(exchange,start,end=''):
    """
    查询截止N分钟之前的一段，例如： 当前 14：05，
    查询 start=13：30  ， end = 14：00

    Return:   遗漏缺失的时间kline （1m）
    """
    start = parse(start)
    if not end:
        end =str(datetime.datetime.now()).split(".")[0]
    end = parse(end)

    miss_list = []
    cc = client()
    for name, ex in cfgs.get('exchange', {}).items():
        # name = name.upper()
        for tt, var in ex['tt'].items():  # 交易类型

            file = os.path.join(PWD, var['symbol'])
            symbols = open(file).readlines()
            symbols = list(filter(lambda s: s, map(lambda s: s.strip(), symbols)))

            # for period in var['period']:
            for period in [1]:      # 仅处理 1m 周期kline
                for symbol in symbols:
                    result = get_data(ex,tt,period,symbol,start,end)
                    mins = int((end - start).total_seconds() / 60)
                    if mins != len(result['time']): # 中间存在空洞
                        print(f"{exchange},{tt},{period},{symbol},"
                              f"{str(start)},{str(end)}, {mins}->{len(result['time'])},"
                              f"missed:{mins - len(result['time']) }")
                        miss = dict(exchange=exchange, tt=tt,period=period,symbol=symbol,
                                 start = str(start) , end = str(end),
                                 missed_mins = mins - len(result['time'])
                                 )
                        miss_list.append(miss)
    return miss_list

if __name__ == '__main__':
    fire.Fire()