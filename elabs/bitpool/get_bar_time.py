#coding:utf-8

import datetime
import fire

def get_trade_time(name, if_night=False):
  return [[0, 0], [24, 0]]


def get_timestamp_ab(start=[9, 0], end=None, period=30):
  delt = datetime.timedelta(minutes=period)
  
  s = datetime.datetime(1, 1, 1, start[0], start[1], 0)
  if end[0] >= 24:
    e = datetime.datetime(1, 1, 2, 0, 0, 0)
  else:
    e = datetime.datetime(1, 1, 1, end[0], end[1], 0)
  if start == [0, 0]:
    ts = []
  else:
    ts = [datetime.datetime(1, 1, 1, 0, 0, 0).time()]
  while s < e:
    
    ts.append(s.time())
    s = s + delt
  ts.append(e.time())
  return ts


def get_bar_time(comm, period, if_night, start=None, end=None):
  begin, over = get_trade_time(name=comm, if_night=if_night)
  if start is None:
    start = begin
  if end is None:
    end = over
  comm = comm.upper()
  tt = []
  
  ts = get_timestamp_ab(start=start, end=end, period=period)
  
  for i in range(1, len(ts)):
    tt.append([ts[i - 1].hour * 100 + ts[i - 1].minute, ts[i].hour * 100 + ts[i].minute])
  return tt

def test():
  return get_bar_time('SL',5,1,[10,0],[24,0])

if __name__ == '__main__':
  fire.Fire()