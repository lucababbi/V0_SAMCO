#index definition table with jobIDs, and correct index symbols 
#VCS file with non unique Full Names. Following columns are needed: 'Curr','Type','Full Name','Symbol'
#loch: history folder location with 'Index_Definition.xlsx' and 'vcs.csv'
#locif = index factory histories

import pandas as pd
import numpy as np
import datetime as dt
import os
from stoxx.qad import datastream as q
import stoxx as stx
import sys
import time
import requests
from requests.auth import HTTPBasicAuth
import csv
from io import StringIO
from multiprocessing import Pool
import pickle
import lzma

class MyDialect(csv.Dialect):
        strict = True
        skipinitialspace = False
        quoting = csv.QUOTE_ALL
        delimiter = ';'
        quotechar = '"'
        lineterminator = '\n'

def getjobIDdates(jobID, type_):
    
    """Arguments:
    type_: 'eom', 'review' or 'all'
    """
    df_ = getIndexTick([jobID])
    if type_=='eom':
        dfdates = myf.monthend(pd.DatetimeIndex(df_.index))
        datesEOM = dfdates[dfdates.columns[0]].tolist()
        return datesEOM
    
    elif type_=='review':
        df_.index = pd.to_datetime(df_.index, format='%Y-%m-%d', dayfirst=True)
        datesRev = stx.get_datelist(df_.index[0].date(), dt.date.today(),months=[3,6,9,12])
        return datesRev
    
    elif type_=='all':
        df_.index = pd.to_datetime(df_.index, format='%Y-%m-%d', dayfirst=True)
        return df_.index.tolist()


def getIndexTick(jobID_list):
    p = Pool(1)
    dates=[]

    df1 = pd.DataFrame()
    df2 = pd.DataFrame()
    for jobID in jobID_list:

        pathTS = "http://10.249.12.252:8088/downloadindexticks?jobId="+ str(jobID) + "&currency=EUR&baseValueCurrency="

        r = requests.get(pathTS, auth=HTTPBasicAuth('user', 'user'),
                         headers={"Cookie": "tntnet.RM=a7e1c450a7ac1fc09ccad83c42fcf0d1"})
        text = r.text
        rows = text.split('\n')
        data = [x.split(';') for x in rows if x!='']

        df2 = pd.DataFrame(data,columns=['Date',jobID])
        df2.index = df2.Date
        del df2['Date']

        df1 = pd.concat([df1,df2], axis=1)
    df1.index.name = None
    return df1


def getCompositions(jobID, dtlst):

    df1 = pd.DataFrame()
    df2 = pd.DataFrame()

    for d in dtlst:
        print(d)
        path = "http://10.249.12.252:8088/downloadindexreport?reportId=" + str(jobID) + "&day=" + d.strftime('%Y-%m-%d') + "&currency=EUR"
        r = requests.get(path, auth=HTTPBasicAuth('christoph_gackstatter', 'christoph_gackstatter'),headers={"Cookie": "tntnet.RM=a7e1c450a7ac1fc09ccad83c42fcf0d1"})

        text = r.text
        rows = text.split('\n')
        data = [x.split(';') for x in rows if x!='']

        df2 = pd.DataFrame(data[1:], columns = data[0][:-1])
        df1 = pd.concat([df1,df2], axis=0)

        return df1
    

def get_hist_and_divs(loc_h, loc_if, rebdate, rebvalue=100):
    df = get_hist_curr(loc_h, loc_if)
    df = do_rebase(df, rebdate, rebvalue)
    df.to_csv(loc_h + 'history_summary_' + str(dt.date.today()).replace('-','') + '.csv', index=True, sep=';')
    get_h_output(df, loc_h)
    get_divisor_output(df, loc_h)
    
def get_h_output(df, loc_h):
    df['Date'] = (df.index).map(lambda x: str(x)[8:10] + '.'+ str(x)[5:7] + '.' + str(x)[:4])
    for col in df.columns[:-1]:
        dfout = pd.DataFrame(df[col])
        dfout['Date'] = df['Date']
        dfout['Symbol'] = col
        dfout['Indexvalue'] = dfout[col]
        dfout[''] = np.nan
        del dfout[col]
        dfout.to_csv(loc_h + col.lower() + '.txt', float_format='%.2f', sep=';', index=False)

def get_divisor_output(df, loc_h):
    loc_prod = 'S:/Stoxx/Production/FinalSheets/'
    dfprod = pd.read_excel(loc_prod + 'stoxx_index_divisors_internal_global.xls')
    dfprod['Symbol'] = dfprod['Symbol'].map(lambda x: x.strip())
    dfprod = dfprod[dfprod.Symbol.isin(list(df.columns))][['ISIN','Symbol','New_Close','New_Mcap','New_Divisor']] #change the slicing from 11 to 9
    dfprod['New_Mcap'] = dfprod['New_Mcap'].map(lambda x: float(x)*1000000)
    dfclose = df.iloc[-1:,:].T
    dfclose.columns = ['close_']
    dfprod = pd.merge(dfprod, dfclose, how='left', left_on='Symbol', right_index=True)  
    dfprod['Divisor'] = np.around((dfprod['New_Mcap'] / dfprod['close_']).astype(np.double), 0)
    #dfprod['Divisor'] = (dfprod['New_Mcap'] / dfprod['close_']).astype(np.double)
    x = str(dt.date.today())
    dfprod['date'] = str(x)[8:10] + '.'+ str(x)[5:7] + '.' + str(x)[:4]
    df = dfprod[['date','ISIN','Symbol','Divisor']]
    df.to_excel(loc_h + 'Divisors_' + str(dt.date.today()).replace('-','') +'.xlsx', index=False)
    
def do_rebase(df, rebdate, rebvalue=100):
    basedtindex = len(df[df.index<rebdate])
    df = (df.iloc[:,:]/df.iloc[basedtindex,:])*rebvalue
    #df = np.around(df.astype(np.double), decimals=2)
    return df

def get_hist_curr(loc_h, loc_if, firstline=False, rebdate='2000-01-01'):

    dfdef = pd.read_excel(loc_h + 'Index_Definition.xlsx')
    dfdef = dfdef[['jobID','symbol<quote>','type<quote>']]

    dflist = pd.read_csv(loc_h + 'vcs.csv', sep=';', encoding='iso-8859-1')
    dflist = dflist[['Curr','Type','Full Name','Symbol']]

    df = pd.merge(dflist, dfdef, how='left', left_on='Symbol', right_on='symbol<quote>')

    dfres = pd.DataFrame()
    for index, row in df[-df['symbol<quote>'].isnull()].iterrows():
        for filename in os.listdir(loc_if):
            if filename[:5]==str(int(row.jobID)):
                f = filename
        dfif = pd.read_csv(loc_if + f, sep=';', names=['Date', row['symbol<quote>']])

        if firstline==True:
            df100 = pd.DataFrame(data={'Date':rebdate, row['symbol<quote>']: 100.00}, index=[0])
            dfif = pd.concat([dfif, df100], axis=0)

        dfif['Date'] = pd.to_datetime(dfif['Date'], format='%Y-%m-%d', dayfirst=True)
        dfif = dfif.sort_values('Date',ascending=True).reset_index(drop=True)
        dfif = dfif.set_index(['Date'], drop=True)
        dfif.index.name= None

        dfcur = df[(df['Full Name']==row['Full Name']) & (df['Type']==row['type<quote>']) & (df['Curr']!='EUR')]
        for index2, row2 in dfcur.iterrows():
            dffx = q.get_curr_rate('EUR', row2.Curr, dfif.index[0], dfif.index[len(dfif)-1])
            dfif = pd.merge(dfif, dffx, how='left', left_index=True, right_index=True)
            if row2.Curr=='JPY':
                dfif.loc[dfif[dfif.index=='2015-05-25'].index,'value_'] = 133.8804
            elif row2.Curr=='USD':
                dfif.loc[dfif[dfif.index=='2015-05-25'].index,'value_'] = 1.10235
            elif row2.Curr=='AUD':
                dfif.loc[dfif[dfif.index=='2015-05-25'].index,'value_'] = 1.40875
            elif row2.Curr=='CNY':
                dfif.loc[dfif[dfif.index=='2015-05-25'].index,'value_'] = 6.83195 
            dfif = dfif.fillna(method='pad')

            dfif[row2.Symbol] = dfif.value_ * dfif[row['symbol<quote>']]
            del dfif['curr1']
            del dfif['curr2']
            del dfif['value_']

            #print(row2.Symbol, row2['Full Name'], row2['Curr'], row2['Type'])

        dfres = pd.concat([dfres, dfif], axis=1)
    return dfres
	
def get_hist_curr_direct(loc_h, firstline=False, rebdate='2000-01-01'):

    dfdef = pd.read_excel(loc_h + 'Index_Definition.xlsx')
    dfdef = dfdef[['jobID','symbol<quote>','type<quote>']]

    dflist = pd.read_csv(loc_h + 'vcs.csv', sep=';', encoding='iso-8859-1')
    dflist = dflist[['Curr','Type','Full Name','Symbol']]

    df = pd.merge(dflist, dfdef, how='left', left_on='Symbol', right_on='symbol<quote>')

    dfres = pd.DataFrame()
    for index, row in df[-df['symbol<quote>'].isnull()].iterrows():

        jobID = str(int(row.jobID))
        dfif = getIndexTick([jobID])

        dfif.columns = [row.Symbol]
        dfif[row.Symbol] = dfif[row.Symbol].map(lambda x: float(x))
        dfif['Date'] = dfif.index
        cols = dfif.columns
        dfif = dfif[cols[-1:] + cols[:-1]]
        dfif = dfif.reset_index(drop=True)

        if firstline==True:
            df100 = pd.DataFrame(data={'Date':rebdate, row['symbol<quote>']: 100.00}, index=[0])
            dfif = pd.concat([dfif, df100], axis=0)

        dfif['Date'] = pd.to_datetime(dfif['Date'], format='%Y-%m-%d', dayfirst=True)
        dfif = dfif.sort_values('Date',ascending=True).reset_index(drop=True)
        dfif = dfif.set_index(['Date'], drop=True)
        dfif.index.name= None

        dfcur = df[(df['Full Name']==row['Full Name']) & (df['Type']==row['type<quote>']) & (df['Curr']!='EUR')]
        for index2, row2 in dfcur.iterrows():
            dffx = q.get_curr_rate('EUR', row2.Curr, dfif.index[0], dfif.index[len(dfif)-1])
            dfif = pd.merge(dfif, dffx, how='left', left_index=True, right_index=True)
            if row2.Curr=='JPY':
                dfif.loc[dfif[dfif.index=='2015-05-25'].index,'value_'] = 133.8804
            elif row2.Curr=='USD':
                dfif.loc[dfif[dfif.index=='2015-05-25'].index,'value_'] = 1.10235
            elif row2.Curr=='AUD':
                dfif.loc[dfif[dfif.index=='2015-05-25'].index,'value_'] = 1.40875
            elif row2.Curr=='CNY':
                dfif.loc[dfif[dfif.index=='2015-05-25'].index,'value_'] = 6.83195 
            dfif = dfif.fillna(method='pad')

            dfif[row2.Symbol] = dfif.value_ * dfif[row['symbol<quote>']]
            del dfif['curr1']
            del dfif['curr2']
            del dfif['value_']

            #print(row2.Symbol, row2['Full Name'], row2['Curr'], row2['Type'])

        dfres = pd.concat([dfres, dfif], axis=1)

    return dfres

def do_checks(df, checkloc, checkfile): #df in the format of get_hist_curr (columns with index symbols, date as index)
    
    #format index factory file and link closed form internal divisor file
    
    bafile = checkloc + checkfile
    dfcheck = df.tail(2).T
    dfcheck['return'] = dfcheck.iloc[:,1] / dfcheck.iloc[:,0] - 1

    loc_prod = 'S:/Stoxx/Production/FinalSheets/'
    dfprod = pd.read_excel(loc_prod + 'stoxx_index_divisors_internal_global.xls')
    dfprod['Symbol'] = dfprod['Symbol'].map(lambda x: x.strip())
    dfprod = dfprod[dfprod.Symbol.isin(list(df.columns))][['Symbol','New_Close']]

    dfprod.index = dfprod.Symbol
    dfprod.index.name = None
    del dfprod['Symbol']
    dfprod.columns = ['close_divisors_file']

    dfcheck_ = pd.merge(dfcheck, dfprod, how='left', left_index=True, right_index=True)
    
    #format BA check file - h_ tabs
    import xlrd  
    book = xlrd.open_workbook(bafile)
    dftest_ = pd.DataFrame()
    ct = 0
    for sheet in book.sheets():
        if sheet.name[:2] == 'h_':
            print(ct, sheet.name)
            dftest = pd.read_excel(bafile, sheetname=sheet.name)
            dftest = dftest.tail(2).T
            dftest.columns = dftest.iloc[0,:]
            idx = dftest.iloc[1,0]
            dftest = dftest.tail(1)
            dftest.index = [idx]
            dftest_ = pd.concat([dftest_,dftest])
            ct+=1
            
    dftest_.columns=['BA_'+ dftest_.columns[0], 'BA_'+ dftest_.columns[1]]
    dftest_['BA_return'] = dftest_.iloc[:,1] / dftest_.iloc[:,0] - 1
    
    #merge all
    dfres = pd.merge(dfcheck_, dftest_, how='left', left_index=True, right_index=True)
    dfres['idx_pts_dif'] = dfres.iloc[:,1] - dfres.iloc[:,4]
    dfres['idx_pts_dif_round'] = np.around(dfres.iloc[:,1],2) - dfres.iloc[:,4]
    dfres['ret_dif'] = dfres['return'] - dfres['BA_return']
    
    dfres.to_excel(checkloc + 'PD_check_IF_' + str(dt.date.today()).replace('-','') +'.xlsx', index=True)
    print('done')
	
def do_checks(df, checkloc, checkfile): #df in the format of get_hist_curr (columns with index symbols, date as index)
    
    #format index factory file and link closed form internal divisor file
    
    bafile = checkloc + checkfile
    dfcheck = df.tail(2).T
    dfcheck['return'] = dfcheck.iloc[:,1] / dfcheck.iloc[:,0] - 1
    
    #format BA check file - h_ tabs
    import xlrd  
    book = xlrd.open_workbook(bafile)
    dftest_ = pd.DataFrame()
    ct = 0
    for sheet in book.sheets():
        if sheet.name[:2] == 'h_':
            print(ct, sheet.name)
            dftest = pd.read_excel(bafile, sheetname=sheet.name)
            dftest = dftest.tail(2).T
            dftest.columns = dftest.iloc[0,:]
            idx = dftest.iloc[1,0]
            dftest = dftest.tail(1)
            dftest.index = [idx]
            dftest_ = pd.concat([dftest_,dftest])
            ct+=1
            
    dftest_.columns=['BA_'+ dftest_.columns[0], 'BA_'+ dftest_.columns[1]]
    dftest_['BA_return'] = dftest_.iloc[:,1] / dftest_.iloc[:,0] - 1
    
    #merge all
    dfres = pd.merge(dfcheck, dftest_, how='left', left_index=True, right_index=True)
    dfres['idx_pts_dif'] = dfres.iloc[:,1] - dfres.iloc[:,4] 
    dfres['ret_dif'] = dfres['return'] - dfres['BA_return']
    
    dfres.to_excel(checkloc + 'PD_check_IF_' + str(dt.date.today()).replace('-','') +'.xlsx', index=True)
    print('done')