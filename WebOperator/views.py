#coding:utf-8
import datetime
import os,os.path
import json
import traceback
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
from app import  app

from flask import make_response,render_template
from functools import wraps, update_wrapper
from elabs.fundamental.http.webapi import CallReturn,ErrorReturn
import pymongo


PWD = os.path.dirname(os.path.abspath(__file__))

def nocache(view):
    @wraps(view)
    def no_cache(*args, **kwargs):
        response = make_response(view(*args, **kwargs))
        response.headers['Last-Modified'] = datetime.datetime.now()
        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, post-check=0, pre-check=0, max-age=0'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '-1'
        return response

    return update_wrapper(no_cache, view)



@app.route('/',methods=['GET'])
@nocache
def main():
    return render_template("main.html")

