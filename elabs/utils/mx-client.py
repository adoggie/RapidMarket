#coding:utf-8

import os,os.path,sys,time,datetime,traceback,json
import zmq
import fire

MX_PUB_ADDR="tcp://127.0.0.1:15555"
MX_SUB_ADDR="tcp://127.0.0.1:15556"
def do_sub(sub_addr=MX_SUB_ADDR):
    ctx = zmq.Context()
    sock = ctx.socket(zmq.SUB)

    sock.setsockopt(zmq.SUBSCRIBE, b'')  # 订阅所有品种
    sock.connect(sub_addr)
    while True:
        m = sock.recv()
        print(m)

def do_pub(text,pub_addr=MX_PUB_ADDR):
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUB)
    sock.connect(pub_addr)

    for n in range(100):
        t = f"{n}.{text}"
        sock.send(t.encode())
        print("msg sent:",t)
        # sock.close()
        time.sleep(1)

    sock.close()

if __name__ == '__main__':
    fire.Fire()