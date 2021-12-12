#coding:utf-8

"""
dumpkline.py
从 keesker 持久化 kline到本地 csv 文件

"""

import datetime
import json
import sys,os,os.path
import traceback
import fire
from dateutil.parser import parse
from elabs.utils.useful import open_file

from elabs.app.core import kseeker
PWD = os.path.dirname(os.path.abspath(__file__))
FN = os.path.join( os.path.dirname( os.path.abspath(__file__) ) , 'settings.json')
cfgs = json.loads( open(FN).read())

def dumpkline(start, end, exchange, tt, symbol_='',output_path=''):
    """加载1m kline 到shm区域，并从事计算N周期kline"""
    kseeker.base_url = cfgs.get('kseeker.base_url')
    if not isinstance(start, datetime.datetime):
        start = parse(start)
    if not isinstance(end, datetime.datetime):
        end = parse(end)

    tt_info = cfgs['exchange'].get(exchange, {}).get('tt', {}).get(tt, {})
    if not tt_info:
        print(f"Error: tt {tt}is undefined!")
        return
    fd = sys.stdout

    fn = tt_info.get('symbol', '')
    fn = os.path.join(PWD, fn)
    symbols = list(map(lambda s: s.strip(), open(fn).readlines()))
    symbols = list(filter(lambda s: s, symbols))
    items = symbols
    for symbol in symbols:
        if symbol_ and symbol_ == symbol:
            items = [symbol]
            break

    for symbol  in items:
        data = kseeker.get_kline(symbol, str(start), str(end), exchange=exchange, tt=tt, period=1)
        if output_path :
            fn = os.path.join(output_path,exchange,tt,symbol)
            fd = open(fn,'w')
        fs = ('DT', 'O', 'H', 'L', 'C', 'V', 'OPI')
        line = ','.join(fs)
        fd.write(line)
        fd.write("\n")
        for n, ts in enumerate(data['TS']):
            dt = datetime.datetime.fromtimestamp(ts)
            fs = (str(dt),data['O'][n],data['H'][n],data['L'][n],data['C'][n],data['V'][n],data['OPI'][n])
            line = ','.join(fs)
            fd.write(line)
            fd.write("\n")
            fd.flush()

if __name__ == '__main__':
    fire.Fire()