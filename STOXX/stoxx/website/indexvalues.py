import requests
import pandas as pd
import numpy as np
import datetime as dt

def get_web_h(idxlist, usedates=False, dfrom='1.1.1980', dto='1.1.2050', special=False):
    from requests.auth import HTTPBasicAuth
    with open('creds.txt') as c: #creds.txt file contains: name.surname@stoxx.com,pass
        creds = c.read()
    creds=creds.split(',')
    auth = HTTPBasicAuth(creds[0],creds[1])
    proxyDict = { 
                  "https"  : 'https://webproxy-fra.deutsche-boerse.de:8080'
                }
    if usedates==True:
        dfrom=dfrom
        dto=dto

    for f in idxlist:
        url='https://www.stoxx.com/download/historical_data/h_'+f.lower()+'.txt'
        r = requests.get(url, auth=auth, proxies=proxyDict)
        text = r.text
        rows = text.split('\n')[1:]
        if special==False:
            try:
                data = [x.split(';')[:-1] for x in rows if x!='']
                df = pd.DataFrame(data, columns=['Date','Symbol','Indexvalue'])
            except:
                data = [x.split(';') for x in rows if x!='']
                df = pd.DataFrame(data, columns=['Date','Symbol','Indexvalue'])
            del df['Symbol']
        elif special==True:
            data = [x.split(';') for x in rows if x!='']
            df = pd.DataFrame(data, columns=['Date','Indexvalue'])
            df = df.loc[1:,:]
        df['Indexvalue'] = df['Indexvalue'].map(lambda x: float(x))
        df = df.rename(columns={'Indexvalue':f})
        if idxlist.index(f)==0:
            dfres=df
        else:
            dfres=pd.merge(dfres, df, how='outer', on='Date') 
    dfres['Date']=pd.to_datetime(dfres['Date'], format='%d.%m.%Y', dayfirst=True)
    if usedates:
        dfres=filterdts(dfrom, dto, dfres)
        dfres=dfres.sort('Date', ascending=True)

    return dfres


def get_h(idxlist, floc, usedates=False, dfrom='1.1.1980', dto='1.1.2050', special=False):

    for idx in idxlist:
        print('http://www.stoxx.com/download/historical_data/h_'+idx.lower()+'.txt')
        #pass

    if usedates==True:
        dfrom=dfrom
        dto=dto

    for f in idxlist:

        df = pd.read_csv(floc + 'h_'+f.lower()+'.txt', sep=';')
        df['Indexvalue'] = df['Indexvalue'].map(lambda x: float(x))
        df = df.rename(columns={'Indexvalue':f})

        df = df[['Date', f]]
        if idxlist.index(f)==0:
            dfres=df
        else:
            dfres=pd.merge(dfres, df, how='outer', on='Date') 
    dfres['Date']=pd.to_datetime(dfres['Date'], format='%d.%m.%Y', dayfirst=True)
    if usedates:
        dfres=filterdts(dfrom, dto, dfres)

    return dfres

def filterdts(dfrom,dto,df):
    dfreturn=df[df['Date']>=dfrom][df['Date']<=dto]
    return dfreturn

def add_months(date, months):
    import calendar
    month = int(date.month - 1 + months)
    year = int(date.year + month / 12)
    month = int(month % 12 + 1)
    day = min(date.day, calendar.monthrange(year, month)[1])
    return dt.date(year, month, day)