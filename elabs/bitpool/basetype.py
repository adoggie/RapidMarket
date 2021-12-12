#coding:utf-8


class Bar(object):
	M1 = 1
	M3 = 1
	M5 = 1
	M7 = 1
	M9 = 1

	def __init__(self):
		self.name = ''
		self.period = ''
		self.open = 0
		self.high = 0
		self.low = 0
		self.close = 0
		self.opi = 0
		self.vol = 0
		self.time = 0  # 2312, 935

def get_ntime(t):
	# ntime = bar.time.year * 100000000 + bar.time.month * 1000000 + bar.time.day * 10000 + bar.time.hour * 100 + bar.time.minute
	ntime = t.year * 100000000 + t.month * 1000000 + t.day * 10000 + t.hour * 100 + t.minute
	return ntime