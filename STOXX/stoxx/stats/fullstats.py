import pandas as pd
import numpy as np
import datetime as dt
from stoxx.website.indexvalues import add_months


#extended version - includes 1month, ytd and full period
def calc_stats_sharpe_ext(df1, rfrate=True): #True or nothing means use EONIA from web
    import requests
    from requests.auth import HTTPBasicAuth

    if rfrate==True:
        ratecurr = 'EUR'
        with open('creds.txt') as c: #creds.txt file contains: name.surname@stoxx.com,pass
            creds = c.read()
        creds=creds.split(',')
        auth = HTTPBasicAuth(creds[0],creds[1])

        url = "http://www.stoxx.com/download/customised/dowjones/eonia_rate.txt"
        r = requests.get(url, auth=auth)

        text = r.text
        rows = text.split('\n')[1:]
        data = [(x[:10], float(x[11:len(x)-1])/100) for x in rows if x!='']

        dfrate = pd.DataFrame(data, columns=['Date', 'rate'])
        dfrate['Date']=pd.to_datetime(dfrate['Date'], format='%d.%m.%Y', dayfirst=True)

        df1 = pd.merge(df1, dfrate, how='left', on='Date')
    elif rfrate==False:
        ratecurr = ''

    df1.fillna(method='pad', inplace=True)
    dto=df1.iloc[len(df1)-1,0]
    df1.reset_index(inplace=True,drop=True)
    
    dtfull = df1.iloc[0,0]
    strdtfull = str(dtfull)[:10]
    yrstemp = ['1m', 'YTD', '1y','3y','5y', 'from ' + strdtfull]

    dtytd = dt.date(dto.year-1, 12, 31)
    dtytd = df1[df1.Date>=dtytd].iloc[0,0]

    dt1m = add_months(dto,-1)
    dt1m = df1[df1.Date>=dt1m].iloc[0,0]

    dt5 = add_months(dto,-12*5)
    dt5 = df1[df1.Date>=dt5].iloc[0,0]

    dt3 = add_months(dto,-12*3)
    dt3 = df1[df1.Date>=dt3].iloc[0,0]

    dt1 = add_months(dto,-12)
    dt1 = df1[df1.Date>=dt1].iloc[0,0]

    dt0 = add_months(dto,-0)
    dt0 = df1[df1.Date>=dt0].iloc[0,0]


    if dtytd.weekday()==5:
        dtytd=dtytd-dt.timedelta(days=1)
    elif dtytd.weekday()==6:
        dtytd=dtytd-dt.timedelta(days=2)

    if dt1m.weekday()==5:
        dt1m=dt1m-dt.timedelta(days=1)
    elif dt1m.weekday()==6:
        dt1m=dt1m-dt.timedelta(days=2)

    if dt5.weekday()==5:
        dt5=dt5-dt.timedelta(days=1)
    elif dt5.weekday()==6:
        dt5=dt5-dt.timedelta(days=2)

    if dt3.weekday()==5:
        dt3=dt3-dt.timedelta(days=1)
    elif dt3.weekday()==6:
        dt3=dt3-dt.timedelta(days=2)

    if dt1.weekday()==5:
        dt1=dt1-dt.timedelta(days=1)
    elif dt1.weekday()==6:
        dt1=dt1-dt.timedelta(days=2)

    if dt0.weekday()==5:
        dt0=dt0-dt.timedelta(days=1)
    elif dt0.weekday()==6:
        dt0=dt0-dt.timedelta(days=2)

    dtlst=[dtfull, dt5, dt3, dt1, dtytd, dt1m, dt0] #6 dates
    ctfull = len(df1[(df1.Date <= dtlst[6]) & (df1.Date > dtlst[0])]) #observation counts
    ct5y = len(df1[(df1.Date <= dtlst[6]) & (df1.Date > dtlst[1])])
    ct3y = len(df1[(df1.Date <= dtlst[6]) & (df1.Date > dtlst[2])])
    ct1y = len(df1[(df1.Date <= dtlst[6]) & (df1.Date > dtlst[3])])
    ct1m = len(df1[(df1.Date <= dtlst[6]) & (df1.Date > dtlst[4])])
    ctytd = len(df1[(df1.Date <= dtlst[6]) & (df1.Date > dtlst[5])])

    #dfvals=df1[df1.Date.isin(dtlst)==True]
    dfvals=pd.DataFrame()
    for fe in dtlst:
        dfvals=pd.concat([dfvals, pd.DataFrame(df1[df1.Date<=fe].sort('Date', ascending=False).reset_index(drop=True).iloc[0,:]).T], axis=0)
    dfvals.reset_index(inplace=True,drop=True)
    actret=np.array(dfvals.iloc[len(dfvals)-1,1:4])/np.array(dfvals.iloc[0:len(dfvals)-1,1:4])-1

    yrs = []
    for x in range(len(actret)):
        yrs.insert(0, yrstemp[x])    

    #actual returns
    dfactret=pd.DataFrame(actret)
    dfactret.columns=df1.columns[1:4]
    dfactret['years'] = yrs
    list1=np.array(dfactret)

    #annualized returns
    dfannret=dfactret
    dfannret.iloc[0,:3]=dfannret.iloc[0,:3].map(lambda x: (x+1)**(250/ctfull)-1)
    dfannret.iloc[1,:3]=dfannret.iloc[1,:3].map(lambda x: (x+1)**(250/ct5y)-1)
    dfannret.iloc[2,:3]=dfannret.iloc[2,:3].map(lambda x: (x+1)**(250/ct3y)-1)
    dfannret.iloc[3,:3]=dfannret.iloc[3,:3].map(lambda x: (x+1)**(250/ct1y)-1)
    dfannret.iloc[4,:3]=dfannret.iloc[4,:3].map(lambda x: (x+1)**(250/ct1m)-1)
    dfannret.iloc[5,:3]=dfannret.iloc[5,:3].map(lambda x: (x+1)**(250/ctytd)-1)
    list2=np.array(dfannret)
    dfannret.columns=[0,1,2,3]

    #vola
    returns=np.array(df1.iloc[1:len(df1),1:4])/np.array(df1.iloc[0:len(df1)-1,1:4])-1
    dfr=pd.DataFrame(returns)
    dfr['Date']=list(df1.loc[1:,'Date'])
    vol=[]
    vol.append(np.std(dfr[(dfr.Date <= dtlst[6]) & (dfr.Date > dtlst[0])].iloc[:,:3], ddof=1)*np.sqrt(250))
    vol.append(np.std(dfr[(dfr.Date <= dtlst[6]) & (dfr.Date > dtlst[1])].iloc[:,:3], ddof=1)*np.sqrt(250))
    vol.append(np.std(dfr[(dfr.Date <= dtlst[6]) & (dfr.Date > dtlst[2])].iloc[:,:3], ddof=1)*np.sqrt(250))
    vol.append(np.std(dfr[(dfr.Date <= dtlst[6]) & (dfr.Date > dtlst[3])].iloc[:,:3], ddof=1)*np.sqrt(250))
    vol.append(np.std(dfr[(dfr.Date <= dtlst[6]) & (dfr.Date > dtlst[4])].iloc[:,:3], ddof=1)*np.sqrt(250))
    vol.append(np.std(dfr[(dfr.Date <= dtlst[6]) & (dfr.Date > dtlst[5])].iloc[:,:3], ddof=1)*np.sqrt(250))
    dfvola=pd.DataFrame(vol)
    dfvola.columns=df1.columns[1:4]
    dfvola['years'] = yrs
    list3=np.array(dfvola)

    #dys
    dfdys=pd.DataFrame(list2)
    dfdys.iloc[:,1]=dfdys.iloc[:,1]-dfdys.iloc[:,0]
    dfdys.iloc[:,2]=dfdys.iloc[:,2]-dfdys.iloc[:,0]
    dfdys.iloc[:,0]=np.nan
    list4=np.array(dfdys)

    #sharpe ratio
    shrp=[]
    for d in range(len(dtlst[1:])):
        dft = df1[(df1.Date <= dtlst[6]) & (df1.Date > dtlst[d])]
        returns = np.array(dft.iloc[1:len(dft),1:4])/np.array(dft.iloc[0:len(dft)-1,1:4])-1
        eonia = np.array(dft.iloc[0:len(dft)-1,4])
        timedelta = np.array([(dft.iloc[i+1,0]-dft.iloc[i,0]).days for i in range(len(dft)-1)])
        drate = eonia*timedelta/365
        excessreturn = returns.T[0]-drate, returns.T[1]-drate, returns.T[2]-drate
        shrp.append([((np.mean(excessreturn[i])/np.std(excessreturn[i], ddof=1))*np.sqrt(250)) for i in range(3)])
    dft
    dfshrp = pd.DataFrame(shrp)
    dfshrp.columns=df1.columns[1:4]
    dfshrp['years']= yrs
    list5=np.array(dfshrp)

    #max drawdown
    mxdd = []
    for d in range(len(dtlst[1:])):
        dft = df1[(df1.Date <= dtlst[6]) & (df1.Date > dtlst[d])]
        pk = np.zeros((len(dft)+1,3))
        dd = np.zeros((len(dft)+1,4))
        h = np.array(dft.iloc[:,1:4])
        pk[0] = h[0]
        for i in range(len(h)):
            for j in range(3):
                pk[i+1,j] = h[i,j] if h[i,j] > pk[i,j] else pk[i,j]
                dd[i+1,j] = h[i,j] / pk[i+1,j] - 1 if h[i,j] < pk[i+1,j] else 0
        dd = dd[1:]
        mxdd.append((abs(dd[:,0].min()), abs(dd[:,1].min()), abs(dd[:,2].min()),yrs[d]))

    dfres = pd.concat([pd.DataFrame(list1), dfannret, pd.DataFrame(list3), 
                       pd.DataFrame(list4), pd.DataFrame(list5), pd.DataFrame(mxdd)], 
                       keys=['return actual', 'return ann.', 'volatility ann.', 
                             'dividend yield ann.', 'Sharpe ratio '+ ratecurr +' ann.', 'max drawdown'])
    dfres.columns=[df1.columns[1], df1.columns[2], df1.columns[3], 'period']  
    
    return dfres


def calc_stats(df1, rfrate=True): #receives a dataframe with columns: date, price, net, gross; outputs dataframe. True or nothing means use EONIA from web
    import requests
    from requests.auth import HTTPBasicAuth

    if rfrate==True:
        ratecurr = 'EUR'
        with open('creds.txt') as c: #creds.txt file contains: name.surname@stoxx.com,pass
            creds = c.read()
        creds=creds.split(',')
        auth = HTTPBasicAuth(creds[0],creds[1])

        url = "http://www.stoxx.com/download/customised/dowjones/eonia_rate.txt"
        r = requests.get(url, auth=auth)

        text = r.text
        rows = text.split('\n')[1:]
        data = [(x[:10], float(x[11:len(x)-1])/100) for x in rows if x!='']

        dfrate = pd.DataFrame(data, columns=['Date', 'rate'])
        dfrate['Date']=pd.to_datetime(dfrate['Date'], format='%d.%m.%Y', dayfirst=True)

        df1 = pd.merge(df1, dfrate, how='left', on='Date')
    elif rfrate==False:
        ratecurr = ''

    df1.fillna(method='pad', inplace=True)
    dto=df1.iloc[len(df1)-1,0]
    df1.reset_index(inplace=True,drop=True)
    
    dtfull = df1.iloc[0,0]
    strdtfull = str(dtfull)[:10]
    yrstemp = ['1m', 'YTD', '1y','3y','5y', 'as of ' + strdtfull]

    dtytd = dt.date(dto.year-1, 12, 31)
    dtytd = df1[df1.Date>=dtytd].iloc[0,0]

    dt1m = add_months(dto,-1)
    dt1m = df1[df1.Date>=dt1m].iloc[0,0]

    dt5 = add_months(dto,-12*5)
    dt5 = df1[df1.Date>=dt5].iloc[0,0]

    dt3 = add_months(dto,-12*3)
    dt3 = df1[df1.Date>=dt3].iloc[0,0]

    dt1 = add_months(dto,-12)
    dt1 = df1[df1.Date>=dt1].iloc[0,0]

    dt0 = add_months(dto,-0)
    dt0 = df1[df1.Date>=dt0].iloc[0,0]


    if dtytd.weekday()==5:
        dtytd=dtytd-dt.timedelta(days=1)
    elif dtytd.weekday()==6:
        dtytd=dtytd-dt.timedelta(days=2)

    if dt1m.weekday()==5:
        dt1m=dt1m-dt.timedelta(days=1)
    elif dt1m.weekday()==6:
        dt1m=dt1m-dt.timedelta(days=2)

    if dt5.weekday()==5:
        dt5=dt5-dt.timedelta(days=1)
    elif dt5.weekday()==6:
        dt5=dt5-dt.timedelta(days=2)

    if dt3.weekday()==5:
        dt3=dt3-dt.timedelta(days=1)
    elif dt3.weekday()==6:
        dt3=dt3-dt.timedelta(days=2)

    if dt1.weekday()==5:
        dt1=dt1-dt.timedelta(days=1)
    elif dt1.weekday()==6:
        dt1=dt1-dt.timedelta(days=2)

    if dt0.weekday()==5:
        dt0=dt0-dt.timedelta(days=1)
    elif dt0.weekday()==6:
        dt0=dt0-dt.timedelta(days=2)

    dtlst=[dtfull, dt5, dt3, dt1, dtytd, dt1m, dt0] #6 dates
    ctfull = len(df1[(df1.Date <= dtlst[6]) & (df1.Date > dtlst[0])]) #observation counts
    ct5y = len(df1[(df1.Date <= dtlst[6]) & (df1.Date > dtlst[1])])
    ct3y = len(df1[(df1.Date <= dtlst[6]) & (df1.Date > dtlst[2])])
    ct1y = len(df1[(df1.Date <= dtlst[6]) & (df1.Date > dtlst[3])])
    ct1m = len(df1[(df1.Date <= dtlst[6]) & (df1.Date > dtlst[4])])
    ctytd = len(df1[(df1.Date <= dtlst[6]) & (df1.Date > dtlst[5])])

    #dfvals=df1[df1.Date.isin(dtlst)==True]
    dfvals=pd.DataFrame()
    for fe in dtlst:
        dfvals=pd.concat([dfvals, pd.DataFrame(df1[df1.Date<=fe].sort('Date', ascending=False).reset_index(drop=True).iloc[0,:]).T], axis=0)
    dfvals.reset_index(inplace=True,drop=True)
    actret=np.array(dfvals.iloc[len(dfvals)-1,1:4])/np.array(dfvals.iloc[0:len(dfvals)-1,1:4])-1

    yrs = []
    for x in range(len(actret)):
        yrs.insert(0, yrstemp[x])    

    #actual returns
    dfactret=pd.DataFrame(actret)
    dfactret.columns=df1.columns[1:4]
    dfactret['years'] = yrs
    list1=np.array(dfactret)

    #annualized returns
    dfannret=dfactret
    dfannret.iloc[0,:3]=dfannret.iloc[0,:3].map(lambda x: (x+1)**(250/ctfull)-1)
    dfannret.iloc[1,:3]=dfannret.iloc[1,:3].map(lambda x: (x+1)**(250/ct5y)-1)
    dfannret.iloc[2,:3]=dfannret.iloc[2,:3].map(lambda x: (x+1)**(250/ct3y)-1)
    dfannret.iloc[3,:3]=dfannret.iloc[3,:3].map(lambda x: (x+1)**(250/ct1y)-1)
    dfannret.iloc[4,:3]=dfannret.iloc[4,:3].map(lambda x: (x+1)**(250/ct1m)-1)
    dfannret.iloc[5,:3]=dfannret.iloc[5,:3].map(lambda x: (x+1)**(250/ctytd)-1)
    list2=np.array(dfannret)
    dfannret.columns=[0,1,2,3]

    #vola
    returns=np.array(df1.iloc[1:len(df1),1:4])/np.array(df1.iloc[0:len(df1)-1,1:4])-1
    dfr=pd.DataFrame(returns)
    dfr['Date']=list(df1.loc[1:,'Date'])
    vol=[]
    vol.append(np.std(dfr[(dfr.Date <= dtlst[6]) & (dfr.Date > dtlst[0])].iloc[:,:3], ddof=1)*np.sqrt(250))
    vol.append(np.std(dfr[(dfr.Date <= dtlst[6]) & (dfr.Date > dtlst[1])].iloc[:,:3], ddof=1)*np.sqrt(250))
    vol.append(np.std(dfr[(dfr.Date <= dtlst[6]) & (dfr.Date > dtlst[2])].iloc[:,:3], ddof=1)*np.sqrt(250))
    vol.append(np.std(dfr[(dfr.Date <= dtlst[6]) & (dfr.Date > dtlst[3])].iloc[:,:3], ddof=1)*np.sqrt(250))
    vol.append(np.std(dfr[(dfr.Date <= dtlst[6]) & (dfr.Date > dtlst[4])].iloc[:,:3], ddof=1)*np.sqrt(250))
    vol.append(np.std(dfr[(dfr.Date <= dtlst[6]) & (dfr.Date > dtlst[5])].iloc[:,:3], ddof=1)*np.sqrt(250))
    dfvola=pd.DataFrame(vol)
    dfvola.columns=df1.columns[1:4]
    dfvola['years'] = yrs
    list3=np.array(dfvola)

    #dys
    dfdys=pd.DataFrame(list2)
    dfdys.iloc[:,1]=dfdys.iloc[:,1]-dfdys.iloc[:,0]
    dfdys.iloc[:,2]=dfdys.iloc[:,2]-dfdys.iloc[:,0]
    dfdys.iloc[:,0]=np.nan
    list4=np.array(dfdys)

    #sharpe ratio
    shrp=[]
    for d in range(len(dtlst[1:])):
        dft = df1[(df1.Date <= dtlst[6]) & (df1.Date > dtlst[d])]
        returns = np.array(dft.iloc[1:len(dft),1:4])/np.array(dft.iloc[0:len(dft)-1,1:4])-1
        eonia = np.array(dft.iloc[0:len(dft)-1,4])
        timedelta = np.array([(dft.iloc[i+1,0]-dft.iloc[i,0]).days for i in range(len(dft)-1)])
        drate = eonia*timedelta/365
        excessreturn = returns.T[0]-drate, returns.T[1]-drate, returns.T[2]-drate
        shrp.append([((np.mean(excessreturn[i])/np.std(excessreturn[i], ddof=1))*np.sqrt(250)) for i in range(3)])
    dft
    dfshrp = pd.DataFrame(shrp)
    dfshrp.columns=df1.columns[1:4]
    dfshrp['years']= yrs
    list5=np.array(dfshrp)

    #max drawdown
    mxdd = []
    for d in range(len(dtlst[1:])):
        dft = df1[(df1.Date <= dtlst[6]) & (df1.Date > dtlst[d])]
        pk = np.zeros((len(dft)+1,3))
        dd = np.zeros((len(dft)+1,4))
        h = np.array(dft.iloc[:,1:4])
        pk[0] = h[0]
        for i in range(len(h)):
            for j in range(3):
                pk[i+1,j] = h[i,j] if h[i,j] > pk[i,j] else pk[i,j]
                dd[i+1,j] = h[i,j] / pk[i+1,j] - 1 if h[i,j] < pk[i+1,j] else 0
        dd = dd[1:]
        mxdd.append((abs(dd[:,0].min()), abs(dd[:,1].min()), abs(dd[:,2].min()),yrs[d]))

    dfres = pd.concat([pd.DataFrame(list1), dfannret, pd.DataFrame(list3), 
                       pd.DataFrame(list4), pd.DataFrame(list5), pd.DataFrame(mxdd)], 
                       keys=['return actual', 'return ann.', 'volatility ann.', 
                             'dividend yield ann.', 'Sharpe ratio '+ ratecurr +' ann.', 'max drawdown'])
    dfres.columns=[df1.columns[1], df1.columns[2], df1.columns[3], 'period']  
    
    return dfres