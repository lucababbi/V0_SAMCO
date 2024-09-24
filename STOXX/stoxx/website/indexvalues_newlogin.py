import requests
import pandas as pd
import numpy as np
import datetime as dt
import os
import keyring 

url='http://www.stoxx.com/download/historical_data/h_'+f.lower()+'.txt'
login_url = 'http://www.stoxx.com//mystoxx/user_profile.html'

username = 'nicola.palumbo@stoxx.com'
password = keyring.get_password('stoxx_website', username)
credentials = username, password

save_in_dir = os.path.dirname(os.path.realpath(__file__))

def get_web_h(idxlist, usedates=False, dfrom='1.1.1980', dto='1.1.2050', special=False):

    user_pass = dict(username=username, password=password)
    r = requests.post(login_url,stream=True, data=user_pass)
     

    if usedates==True:
        dfrom=dfrom
        dto=dto

    for f in idxlist:
        r = requests.get(url.format(**params),stream=True, auth=credentials)
        text = r.text
        rows = text.split('\n')[1:]
        if special==False:
            data = [x.split(';')[:-1] for x in rows if x!='']
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