# coding:utf-8

import os,os.path
import json
import time
from threading import Thread
from collections import OrderedDict
import datetime
from flask import Flask
from app import app

from wsgiref.simple_server import make_server
# import config
import views

import api
import fire

PWD = os.path.dirname(os.path.abspath(__file__))
FN = os.path.join(PWD,  'settings.json')
cfgs = json.loads( open(FN).read() )

def run():
    server = make_server( cfgs['host'] , cfgs['port'], app)
    print("HttpServer Started .. [{}:{}]".format(cfgs['host'] , cfgs['port']))
    server.serve_forever()

def debug():
    app.run(host=cfgs['host'],port=cfgs['port'],debug=True)
"""
export FLASK_APP=main.py
flask run 
"""
if __name__ == '__main__':
    fire.Fire()
