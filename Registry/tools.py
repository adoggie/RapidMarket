#coding:utf-8
import datetime
import os,os.path
import json
from dateutil.parser import parse
import flask
from flask import Flask,send_file
from flask import Response,request,redirect,url_for
from flask import render_template
from flask import Flask
# from elabs.fundamental.application import  instance
import pandas as pd
from bson import BSON
from bson.objectid import ObjectId
import base64
import config
from app import app,dbconn

from flask import make_response
from functools import wraps, update_wrapper
# from datetime import datetime
import pymongo
from dateutil.parser import parse
import fire
from app import app,dbconn

from flask import make_response
from functools import wraps, update_wrapper
# from datetime import datetime
import redis


def create_index():
    db = dbconn['EL_Monitor']
    names = db.list_collection_names()
    for name in names:
        coll = db[name]
        coll.create_index([('datetime',1),('update_time',1)])


def test():
    db = dbconn['EL_Monitor']
    coll = db['43_240_124_22-31028']
    rs = coll.find({'datetime':
                        {'$gte':parse('2021-6-9 14:0') ,'$lt':parse('2021-6-10 2:0')}
                        }).sort('datetime',-1)

                        
    rs = list( rs )
    for r in rs :
        print r['datetime'], r['mem']['Mem:']['used'],'M'
    

if __name__=='__main__':
    fire.Fire()

