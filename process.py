#!/usr/bin/env python
# -*- coding: utf-8 -

import pandas as pd
import numpy as np
import re
import pymysql

def connect_mysql():
	dbconn=pymysql.connect(
	    host="127.0.0.1",
	    database="table",
	    user="admin",
	    password="password",
	    port=3306,
	    charset='utf8'
	)
	return dbconn

def get_user(dbconn):
	user = "select id from user;"
	user = pd.read_sql(user,dbconn)
	return user

def get_item(dbconn):
	item = "select * from voice where date_sub(curdate(), interval 2 day) <= date(created_at) and `state` = 0;"
	item = pd.read_sql(item, dbconn)
	return item

def get_one_day_item(dbconn):
	item = "select * from voice where date_sub(curdate(), interval 1 day) <= date(created_at) and `state` = 0;"
	item = pd.read_sql(item, dbconn)
	return item


def get_like(dbconn):
	voice_like = "select  * from voice_like where date_sub(curdate(), interval 2 day) <= date(created_at);"
	like_dis = pd.read_sql(voice_like, dbconn)
	return like_dis

def get_comment(dbconn):
	comment = "select  * from comment where date_sub(curdate(), interval 2 day) <= date(created_at);"
	comment = pd.read_sql(comment, dbconn)
	return comment

def get_view(r):
	users = re.findall(r"\d+\.?\d*",str(r.keys("*recent:feed")))
	result = dict()
	index = 0
	view =pd.DataFrame(columns=('user','item'))
	for user in users:
		for feed in r.smembers('u:' + user +':recent:feed'):
			view.loc[index] = user,int(feed)
			index += 1
	view['view'] = True
	view['user'] = view['user'].apply(int)
	view['item'] = view['item'].apply(int)
	
	return view


def get_data(item, like_dis, comment):
	item = item[['publisher_id', 'id','created_at']]
	item.rename(columns={'id':'item', 'publisher_id':'user'}, inplace = True)
	item['publish'] = True
	item = item.drop_duplicates()

	like = like_dis.loc[like_dis['value'] == 1]
	like = like[['liker_id','voice_id']]
	like.rename(columns={'voice_id':'item', 'liker_id':'user'}, inplace = True)
	like['like'] = True
	like = like.drop_duplicates()

	dislike = like_dis.loc[like_dis['value'] == 2]
	dislike = dislike[['liker_id','voice_id']]
	dislike.rename(columns={'voice_id':'item', 'liker_id':'user'}, inplace = True)
	dislike['dislike'] = True
	dislike = dislike.drop_duplicates()

	comment = comment[['commenter_id', 'voice_id']]
	comment.rename(columns={'voice_id':'item', 'commenter_id':'user'}, inplace = True)
	comment['comment'] = True
	comment = comment.drop_duplicates()

	score = pd.merge(pd.merge(pd.merge(item,like,"outer"),dislike,"outer"),comment,"outer").fillna(False)
	#score = pd.merge(pd.merge(pd.merge(pd.merge(item,view,"outer"),like,"outer"),dislike,"outer"),comment,"outer")
	#score['score'] = 0
	score.to_csv('score_test.csv')
	print(score.head(20))
	def get_score(df):
		if df['dislike']:
			return -1
		elif df['publish']:
			return 5
		elif df['comment'] and df['like']:
			return 4
		elif df['comment']:
			return 3
		elif df['like']:
			return 2
		else: return 0

	score_column = score.apply(get_score,axis=1)
	score['score'] = score_column
	data = score[['user','item','score','created_at']]
	data = data.drop_duplicates()
	return data

def get_mat(score):
	user = score.user.unique()
	item = score.item.unique()
	uid_iid_mat = np.zeros((user.shape[0],item.shape[0]), dtype=np.int8)
	uid_iid_mat = pd.DataFrame(uid_iid_mat, index=user, columns=item)
	for line in score.itertuples():
		uid_iid_mat.loc[line[1], line[2]] = line[3]
	return uid_iid_mat


