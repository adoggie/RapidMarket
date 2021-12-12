#coding:utf-8

import time, json, os.path

from elabs.app.core.broker import TradeApi,OrderRequest

PWD = os.path.dirname(os.path.abspath(__file__))
FN = os.path.join(PWD, '../settings.json')

params = json.loads(open(FN).read())

def test_order_send():
    TradeApi().init(**params)

    for n in range(10000):
        time.sleep(.2)
        req = OrderRequest('CF201',21700,1)
        TradeApi().sendOrder(req)
        TradeApi().cancelOrder('ddff')
        print('req:',n)


test_order_send()