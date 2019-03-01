#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import datetime
import pandas as pd
import numpy as np
from surprise import SVDpp
from surprise import Dataset, Reader
from surprise import evaluate, print_perf
from surprise.model_selection import cross_validate
from pandas.tseries.offsets import Day
from joblib import Parallel, delayed
from process import *
import pickle
import redis
import json
import time
import logging

pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db = 10)   #实现一个连接池
r = redis.Redis(connection_pool=pool)
#顶
pool_lk = redis.ConnectionPool(host='127.0.0.1', port=6379, db = 4)   #实现一个连接池
r_lk = redis.Redis(connection_pool=pool_lk)
#have read list
pool_read = redis.ConnectionPool(host='127.0.0.1', port=6379, db = 15)   #实现一个连接池
r_read = redis.Redis(connection_pool=pool_read)

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s', datefmt='%Y/%m/%d %H:%M:%S', filename='log/voice.log', filemode='w')


if __name__ == '__main__':
    while True:
        logging.warning("start read data")
        start_time = time.time()
    
        dbconn = connect_mysql()
        item = get_item(dbconn)
        like_dis = get_like(dbconn)
        comment = get_comment(dbconn)
        score = get_data(item, like_dis, comment)
        user_uni = score[['user']].drop_duplicates().reset_index(drop=True)
        score.to_csv('score.csv')
        read_time = "Read time: "+str(time.time() - start_time)+"s"
        logging.warning(read_time)
    
    
        logging.warning("start svd++")
        reader = Reader(rating_scale=(-1,5))   #-1-5的评分等级
        data = Dataset.load_from_df(score[['user','item','score']], reader)
        algo = SVDpp()
        train_time = time.time()
        algo.fit(data.build_full_trainset())
        train_time = "Train time: "+str(time.time() - start_time)+"s"
        logging.warning(train_time)
    
        logging.warning("序列化")
        with open('person.pkl', 'wb') as f:
            pickle.dump(algo, f)
            f.close()
    
        logging.warning("删除上一个svd推荐列表")
        users = re.findall(r"\d+\.?\d*",str(r.keys("svd:*")))
        for user in users:
            r.delete('svd:' + user)
    
        logging.warning("页码置0")
        user_page = re.findall(r"\d+\.?\d*",str(r.keys("P:*")))
        for user in user_page:
            r.set("P:" + user,0)
    
        #hot random
        logging.warning("start write hot data")
        lk_item = dict()
        sort_item = dict()
        items = get_one_day_item(dbconn)
        for voice in items['id']:
            lk = r_lk.hget('voice:'+str(voice)+':cnt','lk')
            cmt = r_lk.hget('voice:'+str(voice)+':cnt','cmt')
            if lk and cmt:
                if int(lk) < 100 and int(cmt) < 100:
                    lk_item[voice] = int(lk)
    
        if len(lk_item) > 2400:
            sort_item = list(dict(sorted(lk_item.items(), key = lambda x:x[1], reverse=True)).keys())[0:2400]
        else: sort_item = list(dict(sorted(lk_item.items(), key = lambda x:x[1], reverse=True)).keys())
    
        r.set("Hot:voice",json.dumps(sort_item,separators=(',',':')))
        end_elapsed =  "End elapsed: "+str(time.time() - start_time)+"s"
        logging.warning(end_elapsed)
        logging.warning(str(datetime.datetime.now()))
        time.sleep(60*40)
