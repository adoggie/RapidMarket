#coding:utf-8

import elabs.bitpool.config
import time
import fire

from elabs.bitpool.BitPool import client
from elabs.app.core.message import ExchangeType,TradeType

def simple():
    # return client().get_data('AXSUSDT',40,'2021-9-2','2021-9-3')
    #
    ks = client().get_data(ExchangeType.FTX[1],TradeType.SPOT[1],1,'AXSUSDT','2021-1-1','2021-12-30')
    return ks
    # return client().get_latest('AXSUSDT',40)

if __name__ == '__main__':
    fire.Fire()