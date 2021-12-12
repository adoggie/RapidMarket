# coding:utf-8


import json
import time
from threading import Thread
from collections import OrderedDict
import datetime
import pymongo
# import config

from flask import Flask
from jinja2 import Environment, PackageLoader, select_autoescape
# env = Environment(
#     loader=PackageLoader('yourapplication', 'templates'),
#     autoescape=select_autoescape(['html', 'xml'])
# )
app = Flask(__name__)
app.config['TEMPLATES_AUTO_RELOAD'] = True
# dbconn = pymongo.MongoClient(**config.MONGODB)
