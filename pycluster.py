#coding=utf-8
# -------------------------------------
# function: clustering for RMF model
# auther: chengf
# time: 2013-04-03
# changing log:
# -------------------------------------
import os,sys
import traceback
import psycopg2
import datetime
import time
from lib import Log
from numpy import array
from scipy.cluster.vq import vq, kmeans, whiten
import numpy
import numpy as np

def get_connect():
    conn = psycopg2.connect(host='192.168.171.104',port=5432,user='esundw',password='esundw',database='esun')
    cur = conn.cursor()
    return cur,conn

def load_connect():
    conn = psycopg2.connect(host='192.168.171.109',port=5432,user='gp4web_read',password='gp4web_read',database='esun')
    cur = conn.cursor()
    return cur,conn

def save(point_set,clust_res,tableName):
    print time.strftime("%Y-%m-%d %X",time.localtime()) 
    print 'save cluster result to database !'
    insert_data = np.hstack((point_set, np.atleast_2d(clust_res).T))
    set_size = insert_data.__len__()
    dict = ({'user_id':int(insert_data[0][0]),'recency':float(insert_data[0][1]),'frequency':float(insert_data[0][2]),'monetary':float(insert_data[0][3]),'cluster_type':int(insert_data[0][4])},)  
    for i in range(set_size):
        #print insert_data
        #print int(insert_data[i][0]),insert_data[i][1],insert_data[i][2],insert_data[i][3]
        if(i>0):
            point = ({'user_id':int(insert_data[i][0]),'recency':float(insert_data[i][1]),'frequency':float(insert_data[i][2]),'monetary':float(insert_data[i][3]),'cluster_type':int(insert_data[i][4])},)
            dict = dict + point
    [cur,conn] = get_connect() 
    #print insert_data
    sql_truncate = ''' truncate table {tableName} ''' #temp_chengf_test_cluster_type  for test
    sql_insert = ''' insert into {tableName} values (%(user_id)s, %(recency)s,%(frequency)s,%(monetary)s,%(cluster_type)s ) '''
    sql_truncate = sql_truncate.replace('{tableName}',tableName)
    sql_insert = sql_insert.replace('{tableName}',tableName)
    #print sql_insert
    print time.strftime("%Y-%m-%d %X",time.localtime()) 
    print 'insert begin ! '
    try:
        print 'truncate table first'
        log.info('%s  clusteing,using %d s\n' % (__file__,(time.time()-begin)))
        cur.execute(sql_truncate)
        log.info('%s trancate table,using %d s\n' % (__file__,(time.time()-begin)))
        cur.executemany(sql_insert,dict)
        log.info('%s insert table,using %d s\n' % (__file__,(time.time()-begin)))
    except Exception,e:
        print e
        #cur.close()
        traceback.print_exc()
    conn.commit()
    conn.close()    
    #print dict
    
def k_mean(all_set,train_set,cluster_num):
    cluster_num = int(cluster_num)
    print time.strftime("%Y-%m-%d %X",time.localtime()) 
    print '''let's find the center ''' + str(cluster_num) + ' point'
    kcenter = kmeans(train_set,cluster_num)[0]
    print time.strftime("%Y-%m-%d %X",time.localtime()) 
    print '''let's calculate the cluster '''
    result = vq(all_set,kcenter)[0]
    #print result
    clust_res = result
    return clust_res
    
def get_data(tableName):
    sql_all = '''select user_id,recency,frequency,monetary from {tableName} ''' 
    sql_train = '''select user_id,recency,frequency,monetary from {tableName}
        where monetary < 600000 and frequency< 10000 '''
    sql_all = sql_all.replace('{tableName}',tableName)
    sql_train = sql_train.replace('{tableName}',tableName) 
    #print sql_train
    all_set = array([0,0,0]) #初始化对象
    train_set = array([0,0,0]) #初始化对象
    point_set = array([0,0,0,0]) #初始化对象
    [cur,conn] = get_connect()
    try:
        cur.execute(sql_all)
        result = cur.fetchall()
        for temp in result:                       
                data =array([float(temp[1]),float(temp[2]),float(temp[3])])
                #print data
                all_set = numpy.vstack([all_set,data])
                point = array([int(temp[0]),float(temp[1]),float(temp[2]),float(temp[3])])
                point_set = numpy.vstack([point_set,point])
        cur.execute(sql_train)
        train_result = cur.fetchall()
        print time.strftime("%Y-%m-%d %X",time.localtime()) 
        print '''get all train data '''
        for temp in train_result: 
                data =array([float(temp[1]),float(temp[2]),float(temp[3])])
                #print data
                train_set = numpy.vstack([train_set,data])
    except Exception,e:
        print e
        #cur.close()
        traceback.print_exc()
    #finally:
    conn.close()
    all_set = np.delete(all_set, 0, 0)
    train_set = np.delete(train_set, 0, 0)
    point_set = np.delete(point_set, 0, 0)  
    print time.strftime("%Y-%m-%d %X",time.localtime()) 
    print '\n get all data'
    #print return_set
    return all_set,train_set,point_set
    
if __name__=='__main__':
    begin=time.time()
    date=str(datetime.datetime.now()-datetime.timedelta(days=1))[:10]
    cluster_num = 4
    tableName_input = 'temp_chengf_test_cluster' # temp_chengf_test_cluster  dm_rfm_module_20130327 
    tableName_output = 'temp_chengf_test_cluster_type' # dm_rmf_cluster_result
    log = Log.getLog2('/home/dm/dm_job/cluster.log')
    if len(sys.argv)==4:
        cluster_num=sys.argv[1] 
        tableName_input=sys.argv[2] 
        tableName_output =sys.argv[3] 
    if len(sys.argv)==2:
        cluster_num=sys.argv[1]
    print 'excute python pycluster.py ' + str(cluster_num) + ' '  + tableName_input + ' ' + tableName_output
    log.info('%s begin '%__file__)
    print __file__
    try:
       print time.strftime("%Y-%m-%d %X",time.localtime()) 
       print 'begin extract data !'
       log.info('%s begin extract data,Using %d s\n' % (__file__,(time.time()-begin)))
       all_set,train_set,point_set = get_data(tableName_input)
       print time.strftime("%Y-%m-%d %X",time.localtime())
       print 'begin cluster !'
       log.info('%s begin kmean cluster,train Using %d s\n' % (__file__,(time.time()-begin)))
       clust_res = k_mean(all_set,train_set,cluster_num)
       print time.strftime("%Y-%m-%d %X",time.localtime())
       print 'begin save cluster into DB !'
       log.info('%s begin save cluster result,all data Using %d s\n' % (__file__,(time.time()-begin)))
       save(point_set,clust_res,tableName_output)
       print time.strftime("%Y-%m-%d %X",time.localtime())
       print 'cluster  over!'
    except Exception,e:
       print e
       traceback.print_exc()
       log.error('执行失败: \n%s' % Log.getTraceString())
       #failed = 1
    log.info('%s finish ,Using %d s\n' % (__file__,(time.time()-begin)))
