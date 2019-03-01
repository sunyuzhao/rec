#!/usr/bin/env python
# -*- coding: utf-8 -*-
import redis,json,time
import re
from threading import Timer

pool = redis.ConnectionPool(host='127.0.0.1', port=6379, decode_responses=True,db = 6)
r = redis.Redis(connection_pool=pool)


def clean():
    users = re.findall(r"\d+\.?\d*",str(r.keys("*recent:feeds")))
    for user_id in users:
        max_score = time.time() - 24*60*60
        min_score = 0
        key = 'u:'+user_id+':recent:feeds'
        r.zremrangebyscore(key, min_score, max_score)
    Timer(60*10,  clean).start()
    print('stop')


Timer(60*10,  clean).start()
