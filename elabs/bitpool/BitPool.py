#coding:utf-8

import datetime
import json
import sys,os
import traceback
import fire
from dateutil.parser import parse
from multiprocessing import Process

from elabs.utils.useful import singleton
from elabs.utils.useful import Timer
from elabs.bitpool.shared_struct import SharedDataManager

from elabs.bitpool import config
# from elabs.bitpool.get_data import pull_data
from elabs.app.core.market_receiver import MarketReceiver
from elabs.app.core.message import KLine
from elabs.app.core import kseeker
from elabs.app.core.registry_client import RegistryClient

PWD = os.path.dirname(os.path.abspath(__file__))
FN = os.path.join( os.path.dirname( os.path.abspath(__file__) ) , 'settings.json')
CFGS = json.loads( open(FN).read())

# def hm2num(hm):
#   return hm[0]*100 + hm[1]
#
# def num2hm(num):
#   return num/100 , num%100
#
# def make_datetime(num,dt=None):
#   h,m = num2hm(num)
#   if not dt:
#     dt = datetime.datetime.now()
#   return datetime.datetime(dt.year,dt.month,dt.day,h,m,0)


def make_shm_id( exchange, tt, period):
    ex = CFGS['exchange'][exchange]
    ex_id = ex['id']
    tt_id = ex['tt'][tt]['id']

    ID = int('F%04d%02d%d' % (period, ex_id, tt_id), 16)
    KEY = f"{exchange}_{tt}_{period}"
    return ID, KEY

@singleton
class BitPool(object):
    def __init__(self):
        # self.ctx = zmq.Context()
        self.sub_sock = None        # subscribe for K
        self.pub_sock = None
        self.sub_svc_sock = None
        self.sdm = None
        self.period_sdms = {}
        self.cfgs = CFGS
        self.inited = False
        self.ka_timer:Timer = None

    def onKeepAlive(self):
        # 保持服务心跳
        RegistryClient().keep_alive()

    def onKline(self,kline:KLine):
        self.ka_timer.kick()

        kline.datetime = int(kline.datetime/1000)
        kline.time = datetime.datetime.fromtimestamp(kline.datetime)
        if kline.symbol == 'ALT-PERP':
            print( str(kline.time ),kline.symbol)

        tt_info = self.get_tt_info(kline.exchange, kline.tt)
        if not tt_info:
            print(f"Error: Kline {kline.symbol}/{kline.exchange}/{kline.tt} is not defined in settings.json!")
            return
        # 时区偏移
        time_offset = tt_info.get("time_offset", 0)
        if time_offset:
            kline.time = kline.time + datetime.timedelta(minutes=time_offset)

        #----------------------------------------------
        ID,KEY = make_shm_id(kline.exchange,kline.tt,kline.period)
        sdm = self.period_sdms.get(KEY)
        if not sdm:
            return
        sdm.put_data(kline)
        #if kline.period == 1:
        kline.sdm = sdm
        self.sythesize(kline)

    def init_mx(self):
        """
        订阅指定交易所的kline
        """
        market_topic = []
        for name,ex in self.cfgs.get('exchange').items():
            tp = f"1.0,kl,{name}"         # 交易所id
            market_topic.append(tp)
        self.cfgs['market_topic'] = market_topic

        self.ka_timer = Timer(self.onKeepAlive, self.cfgs.get('keep_alive_interval', 60))

        MarketReceiver().init(**self.cfgs).addUser(self).open()
        RegistryClient().init(**self.cfgs).open()


        return self

    # def make_shm_id(self,exchange,tt,period):
    #     ID = int('F%03d%03d%d' % (period, exchange, tt), 16)
    #     KEY = f"{exchange}_{tt}_{period}"
    #     return ID,KEY

    def init_shared_mem(self):
        # SharedDataManager.remove_shared(config.SHM_DATA_ID)
        used_size = 0
        for name,ex in self.cfgs.get('exchange',{}).items():
            # name = name.upper()
            for tt,var in ex['tt'].items():  # 交易类型

                file = os.path.join(PWD, var['symbol'])
                symbols = open(file).readlines()
                symbols = list( filter(lambda s: s, map(lambda s: s.strip(), symbols)) )

                for period in var['period']:
                    sdm = SharedDataManager()
                    ID,KEY = make_shm_id(name,tt,period)
                    # ID = int('F%03d%03d%d'%(period,ex[id],var[id]),16)

                    sdm.init(shm_key= self.cfgs['SHM_DATA_ID_BASE'] + ID,
                        period = period,
                        symbols= symbols,
                        start= self.cfgs['SHM_DATA_START'],
                        end = self.cfgs['SHM_DATA_END'],
                        days = self.cfgs['SHM_DATA_DAYS'],
                        time_offset = var['time_offset']
                        )
                    key = f"{name}_{tt}_{period}"
                    self.period_sdms[key] = sdm

                    print('use space:',period , int(sdm.used_space/(1024*1024.)), 'M')
        return self
    
    def mem_used(self):
        size = 0
        for period in config.PERIODS:
            sdm = self.period_sdms[period]
            size += sdm.used_space
        return size
    
    def init(self):
        if not self.inited:
            self.init_shared_mem()
            self.inited = True
        return self
    
    def init_data(self):
        # if config.INIT_DATA:
        #     self.sdm.load_data()
        return self

    def mem_zero(self):
        for sdm in self.period_sdms.values():
            sdm.empty_data()
        # self.sdm.empty_data()
        return self

    # def run(self):
    #     self.sync_data()

    
    # def fill_index(self,period = 0,symbol=''):
    #     sdms = self.period_sdms.values()
    #     if period:
    #         sdms = [ self.period_sdms[period]]
    #
    #     for sdm in sdms:
    #         sdm.fill_index( symbol)
          
        # for period,sdm in self.period_sdms.items():
        #     print 'fill index:',period
        #     sdm.fill_index()
        
    #def on_svc_msg_recv(self, message):
    #    pass

    # def on_data_quotes(self,rs):
    #
    #     for r in rs:
    #         symbol = r['symbol']
    #         if symbol not in config.SYMBOL_LIST:
    #             continue
    #         bar = Bar()
    #         bar.name = symbol
    #         bar.time = r['stockdate']
    #         bar.open = r['open']
    #         bar.high = r['high']
    #         bar.low = r['low']
    #         bar.close = r['close']
    #         bar.opi = 0
    #         bar.vol = r['amount']
    #         print 'bar put into shared mem :', bar.__dict__
    #         self.put_data(symbol,1,bar,'update')

    def sythesize(self,kline:KLine,mode='update'):
        # make N minutes Bar from 1 M bar
        # ID,KEY = self.make_shm_id(kline.exchange,kline.tt,kline.period)
        for k,sdm in self.period_sdms.items():
            exname,ttname,period = k.split('_')
            if exname != kline.exchange:
                continue
            if ttname != kline.tt:
                continue
            if period in (1,'1m','1'):
                continue

            sdm.sythesize(kline,mode)

        # m1.sdm = self.period_sdms[1]    #
        # for period,sdm in self.period_sdms.items():
        #     if period <= 1:
        #         continue
        #     sdm.sythesize(m1,mode)
    
    def mem_dump(self,symbol,start,end,print_text = False):

        rs = self.sdm.get_data(symbol,start,end)
        size = len(rs['time'])
        all =[]
        for n in range(size):
            text = '{},{},{},{},{},{},{}'.format(
                                                rs['time'][n],
                                                 rs['open'][n],
                                                 rs['high'][n],
                                                 rs['low'][n],
                                                 rs['close'][n],
                                                 rs['open_interest'][n],
                                                 rs['volume'][n],
                                                 )
            if print_text:
                print(text)
            all.append(text)
        return all

    def import_data(self,symbol,period,bar):
        pass

    def clear_data(self,symbol,st,et):
        """清除合约所有周期缓存的K线记录"""
        for sdm in self.period_sdms.values():
            sdm.clear_data(symbol,st,et)

    # def put_data(self, symbol,period, m1,mode='update'):
    #     """接受1M k线传入，存储到1M缓存，并计算其他周期的K线
    #     mode - load   历史csv加载
    #            update 实时更新
    #     """
    #     bar = m1
    #     if period != 1:
    #         return
    #
    #     sdm = self.period_sdms.get(period)
    #     if not sdm:
    #         return
    #     sdm.put_data(symbol,bar)
    #     if period == 1:
    #         self.sythesize(bar,mode) # 合成其他周期


    def get_latest(self,exchange,tt,period,symbol,num=1):
        return self.get_data(exchange,tt,period,symbol,num = num)

    def get_data(self,exchange,tt,period,symbol,start='',end='',num=1):
        if not end:
            now = datetime.datetime.now() + datetime.timedelta(minutes=1)
            end = str( now ).split('.')[0]

        if isinstance( end,str):
            end = parse(end)
        if start :
            if isinstance( start,str):
                start = parse(start)

        #if period not in self.period_sdms:
        #    return []

        # _symbs = symbols
        # if isinstance(symbols,str):
        #     _symbs = [symbols]
        # symbols = _symbs
        ID,Key = make_shm_id(exchange,tt,period)
        sdm:SharedDataManager = self.period_sdms.get(Key)
        if not sdm:
            return None
        return sdm.get_data(symbol,start,end,num)

        # data = []
        # if len(symbols) == 1:
        #     data = sdm.get_data(symbols[0],start,end,num)
        # else:
        #     for symbol in symbols:
        #         data.append( sdm.get_data(symbol,start,end,num))
        # return data
    
    # def load_data(self):
    #     """加载所有的shm范围内的行情数据"""
    #     for sdm in self.period_sdms.values():
    #         sdm.load_data()

    def get_tt_info(self,exchange,tt):
        tt_info = CFGS['exchange'].get(exchange,{}).get('tt',{}).get(tt,{})
        return tt_info

    def load(self,start,end,exchange,tt,symbol):
        """加载1m kline 到shm区域，并从事计算N周期kline"""
        kseeker.base_url = self.cfgs.get('kseeker.base_url')
        if not isinstance(start,datetime.datetime):
            start = parse(start)
        if not isinstance(end,datetime.datetime):
            end = parse(end)

        tt_info = self.get_tt_info(exchange,tt)
        if not tt_info:
            print(f"Error: tt {tt}is undefined!")
            return
        # 时区偏移
        time_offset = tt_info.get("time_offset",0)

        for key, sdm in self.period_sdms.items():
            exname, ttname, period = key.split('_')
            if exchange and exchange != exname:
                continue
            if tt and ttname != tt:
                continue
            # if index and index != period:
            #     continue
            data = kseeker.get_kline(symbol,str(start),str(end),exchange= exchange,tt=tt,period=1)
            for n,ts in enumerate(data['TS']):
                k = KLine()
                k.symbol = symbol
                k.datetime = ts
                k.open = data['O'][n]
                k.high = data['H'][n]
                k.low = data['L'][n]
                k.close = data['C'][n]
                k.vol = data['V'][n]
                k.opi = data['OPI'][n]
                sdm.put_data(k)

                k.sdm = sdm
                k.time = datetime.datetime.fromtimestamp(k.datetime)
                if time_offset:
                    k.time = k.time + datetime.timedelta(minutes=time_offset)

                self.sythesize(k,mode='load')

def fill_index(exchange='',tt='',index=0,symbol=''):
    print('Index Filling..')

    def multi_works(sdm:SharedDataManager,symbol):
        print("start index filling .. ", symbol)
        sdm.fill_index(symbol)

    jobs = []
    for key,sdm  in BitPool().period_sdms.items():
        exname,ttname,period = key.split('_')
        if exchange and exchange != exname:
            continue
        if tt and ttname != tt:
            continue
        if index and index != period:
            continue

        p = Process(target=multi_works, args=(sdm,symbol))
        jobs.append(p)
        p.start()
    C = 0
    for p in jobs:
        p.join()
        print('finished jobs:', C, len(jobs) )
        C += 1


def load(start,end,exchange,tt=''):
    """加载历史kline到shm"""
    c = client()
    for name,ex in CFGS['exchange'].items():
        if name == exchange:
            tts = ex['tt'].values()
            if tt and tt in ex['tt']:
                    tts = [  ex['tt'][tt] ]

            for tt in tts:
                fn = tt.get('symbol','')
                fn = os.path.join(PWD,fn)
                symbols = list( map ( lambda  s: s.strip(),open(fn).readlines()) )
                symbols = list( filter(lambda s: s, symbols))
                for symbol in symbols:
                    c.load(start,end,exchange,tt,symbol)

def client()->BitPool:
    return BitPool().init()
    
def reset():
    for name,ex in CFGS['exchange'].items():
        ex_id = ex['id']
        for tt_name,tt in ex['tt'].items():
            tt_id = tt['id']
            for period in tt['period']:
                ID = int('F%04d%02d%d' % (period, ex_id, tt_id), 16)
                cmd = 'ipcrm -M 0x%x' % ID
                print(cmd)
                os.system(cmd)


def destroy():
    reset()

def create():
    client().mem_zero()
    fill_index()

def run():
    client().init_mx()
    MarketReceiver().join()

def mem_zero():
    client().mem_zero()

def mem_dump(symbol):
    return client().mem_dump(symbol,False)


def mem_dump_file(symbol=''):
    pot = BitPool().init()
    symbols = config.SYMBOL_LIST
    if  symbol :
        symbols.append(symbol)

    for symbol in symbols:
        all = pot.mem_dump(symbol)
        if not all :
            continue

        path = os.path.join( config.POT_DUMP_DATA_DIR,symbol)
        if not os.path.exists( path ):
            os.makedirs(path)
        name = os.path.join(path,'K1M_{}.txt'.format( str(datetime.datetime.now()).split(' ')[0]) )
        fp = open(name,'w')
        for line in all:
            fp.write(line +'\n')
        fp.close()

# def edit(name='config.py'):
#   from elabs.fundamental.utils.cmd import vi_edit
#   fn = os.path.join( os.path.dirname( os.path.abspath(__file__) ) , name)
#   vi_edit(fn)

def help():
    usage = """bitpool 1.0 2021/12/5    
    * watch -n 2 -d  'python BitPot.py mem_dump A | tail -n 4 ' 
    """
    print(usage)

def get_data(exchange,tt,period,symbol,start,end):
    pool = client()
    if isinstance(start,str) and start:
        start = parse(start)
    if isinstance(end,str) and end:
        end = parse(end)
    return pool.get_data(exchange,tt,period,period,symbol,start,end)

def get_latest(exchange,tt,period,symbol,num=1):

    return client().get_latest(exchange,tt,period,symbol,num)

def show(prefix='F'):
    cmd = "ipcs -m --human "
    if prefix:
        cmd = "ipcs -m --human | grep "+prefix
    print(os.popen(cmd).read())

# def run_get_data(symbol,period,num):
#     pool = client()
#     while True:
#         print pool.get_data([symbol],period,'','',num)
#         time.sleep(.5)
#
# def get_data_tss(symbol,period,start,end,tss):
#     result = get_data(symbol,period,start,end,0)
#     data = dict(itime=[], time=[], open=[], high=[], low=[], close=[], opi=[], vol=[])
#
#     for n, tx in enumerate(result['time']):
#         for ts in tss:
#             st = make_datetime(ts[0], tx)  # ([hour,min],[hour,min])
#             et = make_datetime(ts[1], tx)
#             if st <= tx < et:
#                 data['time'].append(tx)
#                 data['open'].append(result['open'][n])
#                 data['high'].append(result['high'][n])
#                 data['low'].append(result['low'][n])
#                 data['close'].append(result['close'][n])
#                 data['vol'].append(result['vol'][n])
#                 data['opi'].append(result['opi'][n])
#     return data
      
if __name__ == '__main__':
    fire.Fire()


"""
python -m elabs.service.bitpool.make-bar nosql_to_mem 'A' 5 2021-5-21 2021-5-30

python -m elabs.service.bitpool.make-bar nosql_to_mem 'A' 5 2021-5-21 2021-5-3
python -m elabs.service.bitpool.BitPool get_data A 5 '' '' 10
"""