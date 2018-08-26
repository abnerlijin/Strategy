import numpy as np
import pandas as pd
import os
from sqlalchemy import create_engine #既支持原生SQL，又支持ORM
#import pymysql #纯python实现，速度慢
import logging

conn = create_engine('mysql+pymysql://lijin:hellolj@2@localhost/testdb?charset=utf8')
ROOTDIR='D:\MyProgram\Data\商品5分钟csv'

def importDIR(path):
    if os.path.isfile(path):
        importMICSV(path)
    else:
        flist = os.listdir(path)
        for i in range(0,len(flist)):
            importDIR(os.path.join(path,flist[i]))

def importMICSV(path):
    basename=os.path.basename(path).upper()
    index=basename.find('MI.CSV')
    if index>-1:
        assetname=basename[0:index]
        importCSV(path,assetname)

def importCSV(path,assetname):
    try:
        data = pd.read_csv(path,header=None,names=['trantime','time','popen','phigh','plow','pclose','vol','amount'],dtype={'popen':np.double,'phigh':np.double,'plow':np.double,'pclose':np.double,'vol':np.double,'amount':np.double})
        data['trantime']=data['trantime']+' '+data['time']
        data['trantime']=pd.to_datetime(data['trantime'])
        data.drop('time',axis=1, inplace=True)
        data['asset']=assetname
        pd.io.sql.to_sql(data,'data_5m', conn, schema='testdb', if_exists='append',index=False,chunksize=1000)
    except Exception as e:
        print(path)
        print(e.message)

#检查是否品种的交易是否时间上连续，要补齐
def ContinueProc(col):
    interval = (df.iloc[i]['trantime'] - df.iloc[i - 1]['trantime']).seconds
    if not ((interval == 300) or (((interval - 65100) % 86400) == 0)):
        df.iloc[i]['continue'] = False


f = lambda i: isContinue(i)
df[1:].apply(func=f, axis=1)
importDIR(ROOTDIR)

