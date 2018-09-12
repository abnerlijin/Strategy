import numpy as np
import pandas as pd
import os
from sqlalchemy import create_engine #既支持原生SQL，又支持ORM
from matplotlib.pylab import date2num,num2date
import matplotlib.pyplot as plt
import mpl_finance as mpl
import datetime
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
        pd.io.sql.to_sql(data,'data_5m', conn, schema='testdb', if_exists='append',index=False,chunksize=10000)
    except Exception as e:
        print(path)
        print(e.message)

#检查是否品种的交易是否时间上连续，要补齐
def CheckTimeContinue(data):
    def isContinue(interval):
        if (interval == 300) or (interval == 7500) or (interval == 1200) or (((interval - 65100) % 86400) == 0):
            return True
        else:
            return False

    tempS=data['trantime']
    tempS=tempS.drop(0).append(pd.Series(data['trantime'][0]) ,ignore_index=True) #也可以用data.shift() data.diff()
    data['shifttime']=tempS
    # data['isContinue']=pd.Series((data['shifttime'].values-data['trantime'].values)) #这样要用.seconds
    data['isContinue'] = pd.Series((data['shifttime'].values - data['trantime'].values) / np.timedelta64(1, 's')) #直接通过np.datetime64算出seconds
    data['isContinue']=data.apply(lambda x: isContinue(x.isContinue), axis = 1) #有点慢

def alignData():
    data = pd.read_sql('select distinct trantime from data_5m',conn)
    assettype=pd.read_sql('select distinct asset from data_5m',conn)
    for idx in assettype.index:
        timeseries=pd.read_sql("select * from data_5m where asset='%s' order by trantime"%(assettype.loc[idx]['asset']),conn)
        timeseries['trantime']=pd.to_datetime(timeseries['trantime'])
        standardTime=data[(data['trantime']>=timeseries.iloc[0]['trantime']) & (data['trantime']<=timeseries.iloc[-1]['trantime'])]['trantime'].to_frame()
        if len(timeseries)==len(standardTime):
            continue
        result = pd.merge(standardTime, timeseries, how='left', on=['trantime'])
        result.fillna(value={'realdata': 0,'vol':0,'amount':0}, inplace=True)
        result.fillna(method='ffill',inplace=True)
        result = result[result['realdata']==0]
        result['popen']=result['phigh']=result['plow']=result['pclose']
        pd.io.sql.to_sql(result,'data_5m', conn, schema='testdb', if_exists='append',index=False,chunksize=10000)

#date_time = datetime.datetime.strptime('2017-12-30 12:20:12', '%Y-%m-%d %H:%M:%S')
import matplotlib.dates as mdates
import matplotlib.ticker as ticker
timeseries=pd.read_sql("select asset,trantime,popen,pclose,phigh,plow,vol from data_5m where asset='SQAL' order by trantime",conn)
timeseries['trantime']=pd.to_datetime(timeseries['trantime'])
#timeseries['trantime']=np.arange(len(timeseries))
data=timeseries.values
data[:,1]=date2num(data[:,1])
mdata = data[3000:3040,1:]
def format_date(x, pos=None):
    print(x,' ',type(x))
    return x #mdata[thisind] #.strftime('%Y-%m-%d')

fig = plt.figure(figsize=(10, 5))
ax = fig.add_axes([0.1, 0.2, 0.85, 0.7])
ax.spines['right'].set_color('none')
ax.spines['top'].set_color('none')
ax.xaxis.set_ticks_position('bottom')
ax.yaxis.set_ticks_position('left')
ax.tick_params(axis='both', direction='out', width=2, length=8,
               labelsize=12, pad=8)
ax.spines['left'].set_linewidth(2)
ax.spines['bottom'].set_linewidth(2)
#ax.set_xticks(data[3000:3200,1])
plt.grid(True)
# 设置日期刻度旋转的角度
plt.xticks(rotation=90)
plt.title('merchant price')
plt.xlabel('Date')
plt.ylabel('Price')
#ax.xaxis.set_major_formatter(ticker.FuncFormatter(format_date))
#ax.xaxis.set_major_formatter(mdates.DateFormatter('%y-%m-%d %H:%M:%S'))
mpl._candlestick(ax,mdata , width=0.001, colorup='g', colordown='r', alpha=1.0)
plt.show()
