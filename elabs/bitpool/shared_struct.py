#coding:utf-8

"""
https://blog.csdn.net/AMDS123/article/details/80316781
https://github.com/omnisci/pymapd/blob/master/pymapd/ipc.py
https://www.cnblogs.com/52php/p/5861372.html
"""
from  collections import OrderedDict
import datetime
from ctypes import *
# import numpy as np
from dateutil.parser import parse
from elabs.bitpool import config
# from elabs.bitpool.basetype import Bar
from elabs.app.core.message import *

IPC_CREAT = 512
IPC_RMID = 0

# libc = CDLL("", use_errno=True, use_last_error=True)
libc = CDLL('librt.so')
# int shmget(key_t key, size_t size, int shmflg);
shmget = libc.shmget
shmget.restype = c_int
shmget.argtypes = (c_int, c_size_t, c_int)

# void* shmat(int shmid, const void *shmaddr, int shmflg);
shmat = libc.shmat
shmat.restype = c_void_p
shmat.argtypes = (c_int, c_void_p, c_int)

# int shmdt(const void *shmaddr);
shmdt = libc.shmdt
shmdt.restype = c_int
shmdt.argtypes = (c_void_p,)

# https://github.com/albertz/playground/blob/master/shared_mem.py
# int shmctl(int shmid, int cmd, struct shmid_ds *buf);
shmctl = libc.shmctl
shmctl.restype = c_int
shmctl.argtypes = (c_int, c_int, c_void_p)

# void* memcpy( void *dest, const void *src, size_t count );
memcpy = libc.memcpy
memcpy.restype = c_void_p
memcpy.argtypes = (c_void_p, c_void_p, c_size_t)

# void *memset(void *s, int c, size_t n);
memset = libc.memset
memset.restype = c_void_p
memset.argtypes = (c_void_p, c_void_p, c_size_t)


# Columns = dict( time = 0, open= 1,high =2,low=3, close=4 ,open_interest=5,volume=6)
# DefaultCapacity = 240 * 250 * 10 * 50 * 7  # 10 years , 50 symbols , 240 bars one day , 7 fields in bar , 250 trade days
# 240 * 250 * 10 * 50 * 7 / 1024/1024  => 200 M

# K_NUM_DAY =  60*24  # 1440
# Days = config.BUFF_ZONE_DAYS
# DefaultCapacity = K_NUM_DAY * Days #
DefaultValueSize = 8 # 每个值占用8个字节

DoubleSize = 8

# _field_list = ("time","open","high","low","close","open_interest","volume")

# class KData(Structure):
# 	_fields_ = [
# 		("time", c_uint64 * DefaultCapacity ),
# 		("open", c_double * DefaultCapacity ),
# 		("high", c_double * DefaultCapacity ),
# 		("low", c_double * DefaultCapacity ),
# 		("close", c_double * DefaultCapacity ),
# 		("open_interest", c_double * DefaultCapacity ),
# 		("volume", c_double * DefaultCapacity ),
# 	]
# 	@classmethod
# 	def set_fields(cls,fields):
# 		cls._fields_ = fields
		
# from pyelf.elutil import get_bar_time

from elabs.bitpool.data_bucket import DataBucket
# from data_bucket import DataBucket

		
class SharedDataManager(object):
	def __init__(self):
		self.cfgs =  OrderedDict()
		self.symbols = OrderedDict()
		self.shm_id = 0
		self.ptr = None
		self.all_size = 0
		self.start = None
		self.end = None
		# self.zones = [ None,None]
		self.bucket:DataBucket = None
		self.used_space = 0
		# self.head = None

	@classmethod
	def remove_shared(cls,shmid):
		ret = shmctl(shmid, IPC_RMID, 0)
		if ret < 0:
			print(libc.perror('=='))
		print(ret)

	def get_period(self):
		return self.cfgs['period']
	
	def get_config(self,name):
		return self.cfgs.get(name)
	
	def init(self,**kwargs):
		self.cfgs.update(**kwargs)
		key = self.cfgs.get('shm_key')
		symbols = self.cfgs.get('symbols', [])
		start = self.cfgs.get('start')
		end = self.cfgs.get('end')
		days = self.cfgs.get('days',5)
		
		#mongodb  = self.cfgs.get('mongodb',{})
		init_data = self.cfgs.get('init_data',False)
		# mem_zero = self.cfgs.get('mem_zero',False)
		
		symbols = list(map(lambda s: s.upper(),symbols))
		symbols.sort()
		
		if not start:
			start = str(datetime.datetime.now()).split(' ')[0]
			start = parse(start)
		if not isinstance(start,datetime.datetime):
			start = parse(start)
		
		if end:
			if isinstance(end,str):
				end = parse(end)
			days = (end-start).days
		
		self.bucket = DataBucket(self, start, days, symbols,self.cfgs['period'],self.cfgs['time_offset'])
		multiple = 1

		used_space = self.bucket.get_size() * multiple
		self.used_space = used_space
		self.shm_id = shmget(key, used_space, 0o666 | IPC_CREAT)
		if self.shm_id < 0 :
			return None

		self.ptr = shmat(self.shm_id, 0, 0)
		if not self.ptr:
			return None
		st = datetime.datetime.now()
		self.bucket.map_ptr(self.ptr ,False)
		et = datetime.datetime.now()
		# print 'est:', (et-st).total_seconds()
		
		# self.zones[1].map_ptr(self.ptr + self.zones[0].get_size(),False )
	def fill_index(self,symbol=''):
		self.bucket.fill_index(symbol)
		
	# def load_data(self):
	# 	mongodb = self.cfgs.get('mongodb', {})
	# 	self.bucket.load_data(mongodb=mongodb, init_once=True)
	#
	def empty_data(self):
		self.bucket.empty_data()

	def clear_data(self, symbol, st, et):
		self.bucket.clear_data(symbol,st,et)

	def get_data(self,symbol,start,end,num):
		"""
		:param symbol:
		:param start:  开始时间
		:param end:
		:param num:   kline数量
		:return:
		"""
		return self.bucket.get_data(symbol, start, end,num)
		
		# zone = self.zones[0]
		# result = zone.get_data(symbol,start,end)
		# return result

	def put_data(self,kline:KLine):
		# 实时推送k线到本地内存池
		## 1。check data border
		## 2.switch zone by system clock
		# time = bar.time
		# if not isinstance(time, datetime.datetime):
		# 	time = parse(time)
		# 	bar.time = time

		self.bucket.put_data(kline)
	
	def sythesize(self, kline:KLine,mode):
		self.bucket.sythesize( kline,mode)
		
__all__ = ["SharedDataManager"]




