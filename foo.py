#!/usr/bin/env python
# -*- coding: utf-8 -*-

import multiprocessing
import pandas as pd
import pickle
import sys
import json
import redis
import time
import datetime
import numpy as np
from pandas.tseries.offsets import Day
 

r = redis.Redis(host='127.0.0.1',port=6379,  decode_responses=True,db = 10)

with open('person.pkl', 'rb') as f:
    algo = pickle.load(f)
def func(item, user_id):
    return algo.predict(user_id, item).est


if __name__=='__main__':
    #两天的score表、两天内有行为的用户、一天内的item
    start = time.time()
    now_time =datetime.datetime.now()
    yes_time = (now_time -1*Day()).strftime('%Y-%m-%d %H:%M:%S')#格式化
    score = pd.read_csv('score.csv')
    score = score[score.created_at != 'False']
    score = score[score.created_at != False]
    score['created_at'] = pd.to_datetime(score['created_at'])
    score = score.set_index('created_at') #将时间作为索引
    score = score[yes_time:now_time.strftime('%Y-%m-%d %H:%M:%S')]
    item_uni = score[['item']].drop_duplicates().reset_index(drop=True)
   
    #获取user_id 
    user_id = int(sys.argv[1])

    #获取algo
    with open('person.pkl', 'rb') as f:
        algo = pickle.load(f)

    #定义进程池
    pool = multiprocessing.Pool(processes=7)
    
    
    #start = time.time()
    rec_pool = []
    for item in item_uni['item']:
        rec_pool.append(pool.apply_async(func, (item, user_id)))
    pool.close()
    pool.join()
    rec = list()
    for pre in rec_pool:
        rec.append(pre.get()) 
    rec = np.array(rec)
    sortedResult =  rec.argsort()[::-1]
    rec_list = list()
    for i in sortedResult:
        rec_list.append(int(item_uni.iloc[i]['item']))
    
    #初次去重，去掉产生过行为的表态
    rec_view = list()
    for item in score[score['user'] == int(user_id)]['item']:
        rec_view.append(item)
    rec_de_dup = [item for item in rec_list if item not in rec_view]

    #按12个切段
    rec_result = [rec_de_dup[i:i+15] for i in range(0,len(rec_de_dup), 15)] 
    #写入结果
    r.set("svd:"+str(user_id),json.dumps(rec_result,separators=(',',':')))


