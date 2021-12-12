#coding:utf-8

"""
https://blog.csdn.net/AMDS123/article/details/80316781
https://github.com/omnisci/pymapd/blob/master/pymapd/ipc.py
https://www.cnblogs.com/52php/p/5861372.html

https://renatocunha.com/2015/11/ctypes-mmap-rwlock/

"""
from  collections import OrderedDict
import datetime,traceback
from ctypes import *
import struct
# import numpy as np
from dateutil.parser import parse
from elabs.bitpool import config
# from numba import jit
import platform
from elabs.app.core.message import KLine

libc = CDLL('librt.so')
# void* memcpy( void *dest, const void *src, size_t count );
memcpy = libc.memcpy
memcpy.restype = c_void_p
memcpy.argtypes = (c_void_p, c_void_p, c_size_t)

# void *memset(void *s, int c, size_t n);
memset = libc.memset
memset.restype = c_void_p
memset.argtypes = (c_void_p, c_void_p, c_size_t)

Columns = dict( time = 0, open= 1,high =2,low=3, close=4 ,opi=5,vol=6,latest=7)

# K_NUM_DAY =  60*24  # 1440
# Days = config.BUFF_ZONE_DAYS
# DefaultCapacity = K_NUM_DAY * Days #
DefaultValueSize = 8 # 每个值占用8个字节
DoubleSize = 8
INDEX_PIN_SIZE = 4

import pytz
from elabs.bitpool.shrwlock import RWLock

from elabs.bitpool.get_bar_time import get_bar_time
from elabs.bitpool.basetype import Bar,get_ntime
from elabs.app.core.message import KLine

def hm2num(hm):
	return hm[0]*100 + hm[1]

def num2hm(num):
	return int(num/100) , num%100

def make_datetime(num,dt=None):
	h,m = num2hm(num)
	if not dt:
		dt = datetime.datetime.now()
	return datetime.datetime(dt.year,dt.month,dt.day,h,m,0)



class DataBlock(object):
	def __init__(self, name,addr , cap,period,minutes,owner):
		self.name = name  # Au,RB,..
		self.addr = addr
		self.ptr = addr
		self.first = -1
		self.last = 0
		self.cap = cap	# 分钟数
		self.owner = owner
		# ptr = cast(addr, POINTER(KData))
		# self.struct = ptr.contents
		self.peroid = period
		self.minutes = minutes
		self.index_ptr = addr
		if period == 1:
			self.tss = []
			self.k_num = self.minutes
		else:
			self.tss = get_bar_time(self.name, self.peroid, if_night=True)
			self.k_num = len(self.tss) * self.owner.days
		self.rw_data_ptr = None
		self.rw_lock = None
	
	def get_lock(self):
		return self.rw_lock
	
	# @jit
	def fill_index(self):
		if self.peroid == 1:
			return
		# print('block fill_index:',self.name, self.peroid)
		slot = 0
		# print self.index_ptr[:1000]
		for n in range(self.owner.days):
			day = self.owner.start + datetime.timedelta(days=n)
			for ts in self.tss:
				st = make_datetime(ts[0], day)
				if ts[1] == 0:
					et = make_datetime(ts[1], day + datetime.timedelta(days=1))
				else:
					et = make_datetime(ts[1], day)
				
				days = (st - self.owner.start).days
				m1 = days * 24 * 60 + st.hour * 60 + st.minute
				if ts[1] == 0:
					m2 = (days+1) * 24 * 60
				else:
					m2 = days * 24 * 60 + et.hour * 60 + et.minute

				# todo speedup filling
				# bulk = struct.pack('I',slot) * (m2 - m1)
				# data = create_string_buffer(bulk)
				# memcpy(self.addr+m1,data,(m2 - m1)*4 )
				# 4s faster than below

				for m in range(m1, m2):
					self.index_ptr[m] = slot
				slot += 1
		
		
		
	# print self.index_ptr[:1000]
	
	def map_ptr(self,addr):
		self.addr = addr
		self.ptr = addr
		self.index_ptr =  cast(addr,POINTER(c_uint32* self.minutes)).contents # every value of minute-slot point to period-bar
		# self.k_num = int( (minutes+self.peroid) / self.peroid)
		
				
		addr += INDEX_PIN_SIZE *  self.minutes
		# self.links = cast(addr,POINTER(c_uint32*3)).contents
		# links : [this,prev,next]
		# addr += 4*3
		# self.time_ptr = self.index_ptr   + 4*  self.minutes
		self.time_ptr = cast(addr,POINTER(c_uint64* self.k_num)).contents
		addr += self.k_num * 8
		self.open_ptr = cast(addr,POINTER(c_double* self.k_num)).contents
		addr += self.k_num * 8
		self.high_ptr = cast(addr,POINTER(c_double* self.k_num)).contents
		addr += self.k_num * 8
		self.low_ptr = cast(addr,POINTER(c_double* self.k_num)).contents
		addr += self.k_num * 8
		self.close_ptr = cast(addr,POINTER(c_double* self.k_num)).contents
		addr += self.k_num * 8
		self.opi_ptr = cast(addr,POINTER(c_double* self.k_num)).contents
		addr += self.k_num * 8
		self.vol_ptr = cast(addr,POINTER(c_double* self.k_num)).contents
		addr += self.k_num * 8
		self.latest_ptr = cast(addr, POINTER(c_double * self.k_num)).contents  # last 1M bar time

		self.rw_data_ptr = self.ptr + self.get_index_size() + self.get_data_size()
		self.rw_lock = RWLock(self.rw_data_ptr)
		
		# print 'fill_index / rw_lock :' , self.rw_lock
		# self.rw_lock.acquire_read()
		# self.rw_lock.release()

	def compose_bar(self,start,end):
		""" 仅仅 1M 适用
			合成N分钟 到 X 周期的bar"""

		# bar = Bar
		if start < self.owner.start:
			start = self.owner.start
		if end > self.owner.end:
			end = self.owner.end

		days = (start - self.owner.start).days
		p1 = days * 60 * 24 + start.hour * 60 + start.minute
		days = (end - self.owner.start).days
		p2 = days * 60 * 24 + end.hour * 60 + end.minute
		if p2 < p1:
			return None

		bar = Bar()
		bar.low = 1e+10
		while p1 < p2:
			if not self.time_ptr[p1]:
				p1+=1
				continue

			bar.time = start
			if not bar.open:
				bar.open = self.open_ptr[p1]
			bar.high = max(bar.high,self.high_ptr[p1])
			bar.low = min(bar.low,self.low_ptr[p1])
			bar.close = self.close_ptr[p1]
			bar.opi = self.opi_ptr[p1]
			bar.vol += self.vol_ptr[p1]

			p1+=1

		if not bar.time:
			return None
		return bar

	def _sythesize_load(self,m1:KLine):
		# 并不计算周期内所有M1 记录，而是在现有K线内容上叠加

		for n, ts in enumerate(self.tss):
			st = make_datetime(ts[0], m1.time)
			if ts[1] == 0:
				et = make_datetime(ts[1], m1.time + datetime.timedelta(days=1))
			else:
				et = make_datetime(ts[1], m1.time)
			if m1.time >= st and m1.time < et:
				break

		# ------------------------------------------------

		dt = m1.time
		days = (dt - self.owner.start).days
		p = days * 60 * 24 + dt.hour * 60 + dt.minute
		slot = self.index_ptr[p]

		if m1.time.minute == st.minute and m1.time.hour == st.hour:
			self.vol_ptr[slot] = 0

		if not self.time_ptr[slot]:
			self.open_ptr[slot] = m1.open
			self.low_ptr[slot] = 1e+10
		self.high_ptr[slot] = max(self.high_ptr[slot], m1.high)
		self.low_ptr[slot] = min(self.low_ptr[slot], m1.low)
		self.close_ptr[slot] = m1.close
		self.vol_ptr[slot] += m1.vol
		self.opi_ptr[slot] = m1.opi
		ntime = get_ntime(st)
		self.time_ptr[slot] = ntime
		ntime = get_ntime(m1.time)
		self.latest_ptr[slot] = ntime

	def sythesize(self,m1:KLine,mode):
		"""
		1M 到达 进行当前周期K线合成
		mode:
		  load : from csv
		  update : from server or pull db
		"""
		if self.peroid == 1:
			return
		# ------------------------------------
		if mode == 'load':	# 历史记录加载
			return self._sythesize_load(m1)
		#------------------------------------

		# 每次M1到达，重新计算一遍周期内所有M1记录生成当前 N周期的K线
		sdm = m1.sdm # 1M 's sdm
		block = sdm.bucket.blocks[self.name] # M1 缓冲区

		bar = None
		for n,ts in enumerate(self.tss):
			st = make_datetime(ts[0],m1.time)
			if ts[1] == 0:
				et = make_datetime(ts[1], m1.time + datetime.timedelta(days=1))
			else:
				et = make_datetime(ts[1], m1.time)
			if m1.time >= st and m1.time < et:
				bar = block.compose_bar( st,et)	# 合成 m1 所在 周期的k线
				#-----------^^^^^^^^^-----------
				break
		if not bar:
			return

		dt = bar.time
		days = (dt - self.owner.start).days
		p = days * 60 * 24 + dt.hour * 60 + dt.minute
		slot = self.index_ptr[p]

		self.open_ptr[slot] = bar.open
		self.high_ptr[slot] = bar.high
		self.low_ptr[slot] = bar.low
		self.close_ptr[slot] = bar.close
		self.vol_ptr[slot] = bar.vol
		self.opi_ptr[slot] = bar.opi
		# time and itime
		ntime = get_ntime(bar.time)
		self.time_ptr[slot] = ntime
		ntime = get_ntime(m1.time)	# 最近一根M1 记录
		self.latest_ptr[slot] = ntime

	def get_index_size(self):

		index_size = self.owner.days * 60 * 24 * INDEX_PIN_SIZE  # 1m 索引space
		return index_size

	def get_data_size(self):
		# data_size = len(self.tss) * len(Columns.keys()) * DefaultValueSize * self.owner.days  # bar space
		data_size = self.k_num * len(Columns.keys()) * DefaultValueSize # * self.owner.days  # bar space
		return  data_size

	def get_rwlock_size(self):
		return RWLock.RWLOCK_DATA_SIZE

	def get_size(self):
		"""
		  index: [0,1,2,3,4,..,last]
		  bar: [0,1,2,..,last]
		:return:
		"""
		size =  self.get_index_size() + self.get_data_size() + self.get_rwlock_size()
		return size

	def put_data(self,kline:KLine):
		# raise "This operation deprecated!"
		# bar = m1
		#time = datetime.datetime.fromtimestamp(kline.datetime,pytz.UTC)
		# time = datetime.datetime.fromtimestamp(kline.datetime,pytz.UTC)
		time = datetime.datetime.fromtimestamp(kline.datetime)
		# if not isinstance(time,datetime.datetime):
		# 	time = parse(time)

		days = (time - self.owner.start).days
		if days < 0:
			return
		# if days >= 2:
		# 	return
		index = days * 60 * 24 + time.hour*60 + time.minute

		d = index 	# M1 不使用index
		ntime = get_ntime(time)

		self.time_ptr[d] = ntime
		self.open_ptr[d] = kline.open
		self.high_ptr[d] = kline.high
		self.low_ptr[d]  = kline.low
		self.close_ptr[d] = kline.close
		self.opi_ptr[d] =  kline.opi
		self.vol_ptr[d] = kline.vol


	def get_lastest_data(self,end,num=10):
		"""查询返回最近的 N kline"""
		result = dict(itime=[], time=[], open=[], high=[], low=[], close=[], opi=[], vol=[],latest=[])
		if num <=0 :
			return result
		
		if end >= self.owner.start + datetime.timedelta(days=self.owner.days):
			return result
		days = (end - self.owner.start).days
		p2 = days * 60 * 24 + end.hour * 60 + end.minute

		slot = -1
		# print self.index_ptr[:9000]
		p2 -= 1
		while True :
			if p2<=0:
				break
			if self.peroid != 1:
				p = self.index_ptr[p2]
				if not p:	# 跳过 0 索引
					p2 -= 1
					continue
			else:
				p = p2

			slot = p
			break

		if slot == -1:
			return result
		
		p = slot
		while num :
			if p<0:
				break
			v = self.time_ptr[p]
			if not v:
				p -= 1
				continue
			
			
			y = int(v / 100000000)
			m = int((v - y * 100000000) / 1000000)
			d = int((v - y * 100000000 - m * 1000000) / 10000)
			h = int((v - y * 100000000 - m * 1000000 - d * 10000) / 100)
			M = v % 100
			time = datetime.datetime(year=y, month=m, day=d, hour=h, minute=M, second=0)

			result['itime'].insert(0,self.time_ptr[p])
			result['time'].insert(0,time)
			result['open'].insert(0,self.open_ptr[p])
			result['high'].insert(0,self.high_ptr[p])
			result['low'].insert(0,self.low_ptr[p])
			result['close'].insert(0,self.close_ptr[p])
			result['opi'].insert(0,self.opi_ptr[p])
			result['vol'].insert(0,self.vol_ptr[p])
			result['latest'].insert(0,self.latest_ptr[p])
			p -= 1
			num-=1
		return result

	def clear_data(self, st, et):
		# 仅清除 xxx_ptr 数据区域，保留 index_ptr , rwlock
		if st < self.owner.start :
			st = self.owner.start

		days = (st - self.owner.start).days
		if et >= self.owner.end:
			et  = self.owner.end

		m1 = days * 24 * 60 + st.hour * 60 + st.minute
		days = (et - self.owner.start).days
		m2 = days * 24 * 60 + et.hour * 60 + et.minute
		# 依次清除 多个 xxx_ptr 数据
		if self.peroid !=1:
			m1 = self.index_ptr[m1]
			m2 = self.index_ptr[m2]
		s = m1 * DefaultValueSize
		e = m2 * DefaultValueSize
		clear_size = e - s

		data_ptr = self.ptr + self.get_index_size() # skip index
		# time_ptr
		ptr = data_ptr + self.k_num * DoubleSize * 0
		memset(ptr + s , 0 , clear_size )
		ptr = data_ptr + self.k_num * DoubleSize * 1
		# open_ptr
		memset(ptr + s, 0, clear_size)
		ptr = data_ptr + self.k_num * DoubleSize * 2
		# high_ptr
		memset(ptr + s, 0, clear_size)
		ptr = data_ptr + self.k_num * DoubleSize * 3
		# low_ptr
		memset(ptr + s, 0, clear_size)
		ptr = data_ptr + self.k_num * DoubleSize * 4
		# close_ptr
		memset(ptr + s, 0, clear_size)
		ptr = data_ptr + self.k_num * DoubleSize * 5
		# opi_ptr
		memset(ptr + s, 0, clear_size)
		ptr = data_ptr + self.k_num * DoubleSize * 6
		# vol_ptr
		memset(ptr + s, 0, clear_size)
		ptr = data_ptr + self.k_num * DoubleSize * 7
		# latest_ptr
		memset(ptr + s, 0, clear_size)
		ptr = data_ptr + self.k_num * DoubleSize * 8



	def get_data(self,start=None,end=None,num=1):
		"""输入查询开始和结尾的时间，返回区间分钟K 序列
			if  not start :  end = now
		"""
		if not start and not end:
			return self.get_lastest_data(datetime.datetime.now(),num)

		result = dict(itime=[], time=[], open=[], high=[], low=[], close=[], opi=[], vol=[],latest=[])
		if not start and end :
			return self.get_lastest_data(end,num)

		if not start:
			return result
		if start < self.owner.start:
			start = self.owner.start


		if end > self.owner.start + datetime.timedelta(days=self.owner.days):
			end = self.owner.start + datetime.timedelta(days=self.owner.days)
		print(start,end)
		days  = (start - self.owner.start).days
		p1 = days * 60 * 24 + start.hour * 60 + start.minute
		days = (end - self.owner.start).days
		p2 = days * 60 * 24 + end.hour * 60 + end.minute
		if p2 < p1:
			return result


		if self.peroid == 1: # 1M 周期数据 并不通过index_ptr寻址
			s = p1
			e = p2
		else:
			while True:
				if p2 < p1:
					return result
				s = self.index_ptr[p1]
				if not s :
					p1+=1
				else:
					break

			while True:
				if p2 < p1:
					return result
				e = self.index_ptr[p2]
				if not e :
					p2-=1
				else:
					break

			if p2 < p1:
				return result
			s = self.index_ptr[p1]
			e = self.index_ptr[p2]

		# print p1,p2
		# 中间可能存在空洞，所以依次遍历检查
		for p in range(s,e):
			if not self.time_ptr[p]:
				continue
			v = self.time_ptr[p]
			y = int(v / 100000000)
			m = int((v - y * 100000000) / 1000000)
			d = int((v - y * 100000000 - m * 1000000) / 10000)
			h = int((v - y * 100000000 - m * 1000000 - d * 10000) / 100)
			M = v % 100
			time = datetime.datetime(year=y, month=m, day=d, hour=h, minute=M, second=0)

			result['itime'].append(self.time_ptr[p])
			result['time'].append(time)
			result['open'].append(self.open_ptr[p])
			result['high'] .append(self.high_ptr[p])
			result['low'] .append( self.low_ptr[p])
			result['close'].append( self.close_ptr[p])
			result['opi'].append(self.opi_ptr[p])
			result['vol'].append(self.vol_ptr[p])
			result['latest'].append(self.latest_ptr[p])

		return result

class DataBucket(object):
	""""""
	def __init__(self,sdm,start,days,symbols,period,time_offset):
		self.sdm = sdm
		self.start = start
		self.days = days
		self.end = start + datetime.timedelta(days= days)

		self.symbols = symbols
		self.ptr = 0
		self.mapped = False
		self.prev_ = None
		self.next_ = None
		self.data_inited = False
		self.blocks = {}
		self.period = period
		self.time_offset = time_offset

	
	def id(self):
		return str(self.start.date()) + '({})'.format(self.days)
	
	def init(self,**kwargs):
		return self

	def get_size(self):
		size = 0
		mem_start = self.ptr
		minutes = self.days * 60 * 24
		for n, name in enumerate(self.symbols):
			sym = DataBlock(name, None, 0, self.period, minutes, self)
			size += sym.get_size()

		return size


	def map_ptr(self,ptr,mem_zero = False):
		if self.mapped:
			return self
		
		self.ptr = ptr
		mem_start = self.ptr
		addr = mem_start
		minutes = self.days * 60 * 24
		for n, name in enumerate(self.symbols):
			# print 'init bucket:', self.period,  name
			sym = DataBlock(name, addr, 0,self.period,minutes, self)
			sym.map_ptr(addr)
			addr += sym.get_size()
			self.blocks[name] = sym

		if mem_zero:
			self.empty_data()
		self.mapped = True
		return self
	
	def fill_index(self,symbol=''):
		if symbol:
			block = self.blocks[symbol]
			return block.fill_index()
		
		for symbol,block in self.blocks.items():
			block.fill_index()
			

	def empty_data(self):
		print("-- empty_date size:",self.get_size())
		memset(self.ptr,0,self.get_size())
		return self

	def clear_data(self, symbol, st, et):
		block = self.blocks[symbol]
		if not block:
			return
		block.clear_data(st,et)

	def get_data(self,symbol,start,end,num=1):
		block:DataBlock = self.blocks[symbol]
		if not block:
			return []
		
		block.get_lock().acquire_read()
		try:
			data = block.get_data(start,end,num)
		except:
			traceback.print_exc()
			data = []
		block.get_lock().release()
		return data

	def put_data(self,kline):
		# symbol = kline.symbol.upper().replace('/','').replace('_','')
		symbol = kline.symbol

		block = self.blocks.get(symbol)
		if not block:
			return
		
		block.get_lock().acquire_read()
		try:
			block.put_data(kline)
		except:
			traceback.print_exc()
		block.get_lock().release()

	def sythesize(self,m1:KLine,mode):
		block:DataBlock = self.blocks.get(m1.symbol)
		if not block:
			return
		block.sythesize(m1,mode)
	
		
__all__ = ["DataBucket"]




