#coding:utf-8

import os,sys,time,datetime,dateutil,json,os.path
import fire
from elabs.fundamental.utils.useful import singleton,object_assign

@singleton
class RiskManager(object):
    def __init__(self):
        pass


class RiskFeature(object):
    def __init__(self):
        pass

    # def allow(self,):

class SelfTrade(RiskFeature):
    """自成交"""
    def __init__(self):
        pass

class OrderCreateLimit(RiskFeature):
    """"""
    def __init__(self):
        pass

class OrderCancelLimit(RiskFeature):
    def __init__(self):
        pass
