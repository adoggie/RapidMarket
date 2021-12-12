#coding:utf-8


""""
仓位信息
"""

class  PositionUser(object):
    """仓位用户"""
    def __init__(self):
        pass

    def onPositionSignal(self,message):
        raise NotImplementedError
