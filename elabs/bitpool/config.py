#coding:utf-8
import os,os.path
PWD = os.path.dirname(os.path.abspath(__file__))
file = os.path.join(PWD,'symbols.txt')

EXCHANGE = {
    'ftx': ( 1,  ('spot','ftx_spot_symbols.txt',[1,30,60]),('swap','ftx_swap_symbols.txt',[1,30,60]) )
}

SERVICE_ID = 'bitpool-v.3'

# 共享内存标识
SHM_DATA_ID = 0xffe0
SHM_DATA_ID_BASE = 0
PERIODS = range(5,60,2)
PERIODS = [1,40,60,90]
# PERIODS = [1,40]

#交易合约列表

symbols = open(file).readlines()
symbols = filter(lambda s: s,map(lambda s:s.strip(),symbols))
SYMBOL_LIST = symbols
# CSV_PATH='/home/eladmin/projects/data/BITPOOL/_daynight'


# MYSQL_ASSEMBLE = dict(host="", port=3306, user="", passwd="", database="",charset="utf8")
DATA_PULL_INTERVAL = 30
DATA_PULL_STRIDES = 5
RELOAD_HISTORY_TIME = 5  # minutes


INIT_DATA = True
# BUFF_ZONE_DAYS = 30
# BUFF_ZONE_START = '2021-1-1'
# BUFF_ZONE_START = '2021-5-21'
# BUFF_ZONE_END = '2023-1-1'

SHM_DATA_START = '2021-1-1'
SHM_DATA_END = '2022-1-1'
SHM_DATA_DAYS = 30

KSeeker = "http://127.0.0.1:8091/api/kseeker"
