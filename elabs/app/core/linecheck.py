#coding:utf-8

from elabs.fundamental.utils.useful import singleton

@singleton
class LineChecker(object) :
    """线路状态监控检查"""
    def __init__(self):
        self.pos_up_time = None     # 仓位更新时间
        self.order_up_time = None   # 委托查询返回时间
        self.inss = {}
        self.cfgs = {}

    def init(self,**kvs):
        self.cfgs.update(**kvs)

    def check_tick(self):

        return False

    def set_instruments(self,inss):
        for ins in inss:
            self.inss[ins] = 1
