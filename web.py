#!/usr/bin/env python
# -*- coding: utf-8 -*-
from flask import Flask, abort, request, jsonify,Response
import pandas as pd
import numpy as np
import json
import random
import redis
import logging
from  gevent.pywsgi import WSGIServer
from gevent import monkey
from process import *
from surprise import SVD,SVDpp
from flask_sqlalchemy import SQLAlchemy
import pickle
import time
import datetime
import copy
import multiprocessing
import os
import subprocess

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = "mysql://admin:password@127.0.0.1:3306/table?charset=utf8"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True
db = SQLAlchemy(app)
#monkey.patch_all()

pool = redis.ConnectionPool(host='127.0.0.1', port=6379, decode_responses=True,db = 10)
r = redis.Redis(connection_pool=pool)
pool_rel = redis.ConnectionPool(host='127.0.0.1', port=6379, db = 6)   #实现一个连接池
r_rel = redis.Redis(connection_pool=pool_rel)


def get_random_result(sort_item):
        result = random.sample(sort_item, 15)
        return result


#过滤层
def filter_layer(user_id, rec_list, db, r, r_rel):
    #加入被随机分配的表态
    if r.exists('R:' + user_id):
        rel_rel = r.lrange('R:' + user_id,0,-1)
        rel_rel = [int(i) for i in rel_rel]
        if len(rel_rel)>5:
            rel_rel = rel_rel[0:5]
        rec_list = rel_rel + rec_list
    r.delete('R:' + user_id)

    #过滤拉黑
    rela_view = select_relation(user_id,'view',db)
    rela_show = select_relation(user_id,'show',db)
    rela_follow = select_relation(user_id,'else',db)
    rela_view.append(int(user_id))
    rela_view = rela_view + rela_follow
    rec_copy = copy.copy(rec_list)
    for voice in rec_list:
        if get_publisher(voice,db) in rela_view + rela_show:
            rec_copy.remove(voice)
    #过滤重复已读
    voices = r_rel.zrange("u:"+user_id+":recent:feeds",start=0,end=-1,desc=False)
    have_read = list()
    for voice in voices:
        have_read = have_read + json.loads(voice)
    rec = [voice for voice in rec_copy if voice not in have_read]
    return rec



def get_publisher(voice,db):
        sql = 'select publisher_id from voice where id ={iid};'.format(iid=voice)
        uid = db.session.execute(sql)
        db.session.close()
        for u in uid:
                return u[0]

def select_relation(user_id,rela_type,db):
        if rela_type == 'view':
                sql = "select target_id from user_relation where self_id = {uid} and `view` = 1;".format(uid=user_id)
        elif rela_type == 'show':
                sql = "select self_id from user_relation where target_id = {uid} and `show` = 1;".format(uid=user_id)
        elif rela_type == 'follow':
                sql = "select self_id from user_relation where target_id = {uid} and `follow` = 1;".format(uid=user_id)     
        else:
                sql = "select target_id from user_relation where self_id = {uid} and `follow` = 1;".format(uid=user_id)  

        result = db.session.execute(sql)
        relation = list()
        for get_id in result:
            relation.append(get_id[0])
        db.session.close()
        return relation

def get_rec(user_id, score, r,  old):
    if r.exists("svd:"+user_id):
        rec_list = json.loads(r.get("svd:"+user_id))
    else:
        order='python3 foo.py %d' % (int(user_id))
        try:
            retcode = subprocess.call(order, shell=True)
            if retcode < 0:
                print("Child was terminated by signal")
            else:
                print("Child returned")
        except:
            print("Execution failed:")       
        rec_list = json.loads(r.get("svd:"+user_id))

    if old:
        rec_list = rec_list[random.randint(0,len(rec_list)-1)]
    else:
        #按页码取15条
        if r.exists('P:' + user_id):
            page = r.get('P:' + user_id)
            r.set('P:' + user_id, int(page)+1)
            rec_list = rec_list[int(page)]
        else:
            r.set('P:' + user_id, 1)
            rec_list = rec_list[0]    

    return rec_list

#add_task函数
def write_relation(rel_list, prob, voice):
    if rel_list:
        if random.uniform(0,10) <= prob:
            for user in rel_list:
                    r.rpush("R:"+str(user), voice)

def handle_relation(r_bre,publisher,voice):
    rel_10 = r_bre.zrevrangebyscore(name='u:'+str(publisher)+':brel',max=9,min=0)
    rel_15 = r_bre.zrevrangebyscore(name='u:'+str(publisher)+':brel',max=19,min=10)
    rel_50 = r_bre.zrevrangebyscore(name='u:'+str(publisher)+':brel',max=29,min=20)
    rel_10 = [int(i) for i in rel_10]
    rel_15 = [int(i) for i in rel_15]
    rel_50 = [int(i) for i in rel_50]
    write_relation(rel_10, 1.0, voice)
    write_relation(rel_15, 1.5, voice)
    write_relation(rel_50, 5.0, voice)

#新表态
@app.route('/add_task/', methods=['POST'])
def add_task():
    data = request.json['log']
    score = pd.read_csv('score.csv')
    user_uni = score[['user']].drop_duplicates().reset_index(drop=True)
    sample_user = user_uni.sample(n=100, frac=None, replace=False, weights=None, random_state=None, axis=None)
    #data = txt_wrap_by(line)
    if data:
        if data['action'] == "voice":
            publisher = data['publisher']
            handle_relation(r_rel, publisher, data['voiceId'])

            #广场   RB:广场的推荐列表
            rel_ground = r_rel.zrevrangebyscore(name='u:'+str(publisher)+':brel',max="+inf",min=30)
            rel_ground = [int(i) for i in rel_ground]
            if rel_ground:
                for user in rel_ground:
                    r.rpush("RB:"+str(user), data['voiceId'])

            #随机分发100个用户，剔除有关系分的用户
            rel = r_rel.zrevrangebyscore(name='u:'+str(publisher)+':brel',max="+inf",min=0)
            rel = [int(i) for i in rel]
            for user in sample_user:
                if user not in rel:
                    r.rpush("R:"+str(user), data['voiceId'])

            #广场：被关心的用户
            rela_list = select_relation(publisher,'follow',db)
            for user in rela_list:
                r.rpush("RB:"+str(user), data['voiceId'])
 

    print(str(datetime.datetime.now()) + '{new voice}:'+str(data))    
    return json.dumps({'Accept':"success"})


@app.route('/get_task/', methods=['GET'])
def get_task():
    if not request.args or 'id' not in request.args:
        abort(400)
    user_id = request.args['id']
    score = pd.read_csv('score.csv')

    #推荐活跃用户
    if int(user_id) in score['user'].tolist():
        rec_list = get_rec(user_id, score, r, False)
        rec_list = filter_layer(user_id,rec_list,db,r,r_rel) 
        print(str(datetime.datetime.now()) + '{svdpp rec}:' + str(rec_list))
        return json.dumps({'rec':rec_list},separators=(',',':'))
    #两天未活跃用户
    else:
        rel = r_rel.zrevrangebyscore(name='u:'+user_id+':brel',max="+inf",min=0)
        rel = [int(i) for i in rel]
        user_uni = score[['user']].drop_duplicates().reset_index(drop=True)
        if rel:
            user_list = list()
            for u in rel:
                if u in user_uni:
                    user_list.append(u)
            if len(user_list):
                rec_list = get_rec(str(user_list[0]), score, r, True)
                rec_list = filter_layer(user_id,rec_list,db,r,r_rel)
                print(str(datetime.datetime.now()) + '{Old user}:' + str(rec_list))
                return json.dumps({'rec':rec_list},separators=(',',':'))

        # 推荐热门
        sort_item = json.loads(r.get("Hot:voice"))
        rec_list = get_random_result(sort_item)
        rec_list = filter_layer(user_id,rec_list,db,r,r_rel)
        print(str(datetime.datetime.now()) + '{Hot rec}:' + str(rec_list))
        return json.dumps({'rec':rec_list},separators=(',',':'))

@app.route('/get_ground/', methods=['GET'])
def get_ground():
    if not request.args or 'id' not in request.args:
        abort(400)
    user_id = request.args['id']
    rec_list = list()
    if r.exists('RB:' + user_id):
        rel_rel = r.lrange('RB:' + user_id,0,-1)
        rel_rel = [int(i) for i in rel_rel]
        rec_list = rel_rel + rec_list
    r.delete('RB:' + user_id)
    print(str(datetime.datetime.now()) + '{Ground}:' + str(rec_list))
    return json.dumps({'rec':rec_list},separators=(',',':'))
    
     

if __name__ == "__main__":
    #app.run(host="0.0.0.0",port=8383, debug=True)
    app.run()
    #from werkzeug.debug import DebuggedApplication
    #dapp = DebuggedApplication(app, evalex= True)
    #server = WSGIServer(('0.0.0.0',8383), dapp)
    #server.serve_forever()
