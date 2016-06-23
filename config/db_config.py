# _*_ coding:utf-8 _*_
from pymongo import MongoClient
import datetime
import sys
import time
import math
from bson.objectid import ObjectId
from collections import OrderedDict, Counter
import pandas as pd
import xlsxwriter
import pickle
import re
import numpy as np
import glob
import base64
import multiprocessing
import threading

mongo = MongoClient(host='10.8.8.111', port=27017, connect=False)

db = mongo['eventsV35']  # 埋点数据库
cb = mongo['onions']  # 课程结构数据库
onions = mongo['onions']
cache = mongo['cache']


events = db['eventV35']  # 3.5上线后的数据
events_33 = db['eventV32']  # 3.5上线后的老版本数据
refunds = db['refundrequests']  # 退款问卷
finishState = db['practiceFinishState']  # 注册用户练习完成状态

users = onions['users']  # 用户
questionnaires = onions['questionnaires']  # 其他问卷
coupons = onions['trialcoupons']  # 体验券
schools = onions['schools']  # 学校
rooms = onions['rooms']  # 班级

chapters = cb['chapters']  # 章节
topics = cb['topics']  # 知识点
videos = cb['hypervideos']  # 视频
problems = cb['problems']  # 题目

deviceAttr = cache['deviceAttr']
userAttr = cache['userAttr']
webDevice = cache['webDevice']


events_old = mongo['koalaBackupOnline']['events']  # 2015.12.19-2016.05.04的数据


now = datetime.datetime.now()

TODAY = datetime.datetime(now.year, now.month, now.day) - datetime.timedelta(hours=8)

A_WEEK_AGO = TODAY - datetime.timedelta(weeks=1)

ONLINE_DATE_35 = datetime.datetime(2016, 5, 4, 16)

ONLINE_DATE_36 = datetime.datetime(2016, 6, 15, 16)

PAY_ON_DATE = datetime.datetime(2016, 5, 10, 5)

ALLOWED_DATE_ON = datetime.datetime(2016, 5, 13, 11)

FIX_COUPON_DATE = datetime.datetime(2016, 5, 11, 4)

ONLINE_DATE = datetime.datetime(2015, 2, 7)

ONLINE_DATE_30 = datetime.datetime(2015, 12, 18, 16)


NUM_OF_PROCESS = 6
NUM_OF_WORKERS = 20


# THIS_SATURDAY =
# LAST_SARURDAY =

type_payable = ["B", "C", "D", "E"]

inner_ip = "111.202.79.124"

inner_names = ['twdb01', 'twdb02', 'twdb03', 'twdb04', 'twdb05', 'twdb06', 'twdb07', 'twdb08', 'twdb09', 'twdb10', 'twdb11',
               'twdb12', 'twdb13', 'twdb14', 'twdb15', 'twdb16', 'twdb17', 'twdb18', 'twdb19', 'twdb20', 'twdb21', 'twdb22',
               'twdb23', 'twdb24', 'twdb25', 'twdb26', 'twdb27', 'twdb28', 'twdb29', 'twdb30', 'twdb31', 'twdb32', 'twdb33',
               'twdb34', 'twdb35', 'twdb36', 'twdb37', 'twdb38', 'twdb39', 'twdb40', 'twdb41', 'twdb42', 'twdb43', 'twdb44',
               'twdb45', 'twdb46', 'twdb47', 'twdb48', 'twdb49', 'twdb50', 'twdb51', 'twdb52', 'twdb53', 'twdb54', 'twdb55',
               'twdb56', 'twdb57', 'twdb58', 'twdb59', 'twdb60', 'twdb61', 'twdb62', 'twdb63', 'twdb64', 'twdb65', 'twdb66',
               'twdb67', 'twdb68', 'twdb69', 'twdb70', 'twdb71', 'twdb72', 'twdb73', 'twdb74', 'twdb75', 'twdb76', 'twdb77',
               'twdb78', 'twdb79', 'twdb80', 'twdb81', 'twdb82', 'twdb83', 'twdb84', 'twdb85', 'twdb86', 'twdb87', 'twdb88',
               'twdb89', 'twdb90', 'twdb91', 'twdb92', 'twdb93', 'twdb94', 'twdb95', 'twdb96', 'twdb97', 'twdb98', 'twdb99',
               'twdb100', '18801118840', '18514270725', 'yaqi@yaqi.com', '13910971604', 'summerz0501@126.com', 'teacher',
               'cyj159@sina.com', '13521369305', '18211144099', 'loriachen@hotmail.com', 'diggzhang@gmail.com', 'tao2@test.com',
               '18211144088', 'loidco@hotmail.com', '18800102923', 'summerz0501@126.com', '18267913166', '757097678@qq.com',
               'danyangt', 'xiaoyangcong', 'cb@yangcong345.com', '13800138000', 'yanjin02@s.com', 'linxi05@s.com','3.5@new789.com', 'erwa@qq.com',
               "danyang@guanghe.tv"
               ]
inner_users = users.distinct('_id', {"name": {"$in": inner_names}})

path = ''

