import os
import re
import pandas as pd
from pandas.tseries.offsets import BDay #pandas has holiday functionality (tbd)
import datetime as dt
from stoxx.qad.datastream import get_timeseries_gross_return
from stoxx.qad.datastream import get_timeseries_price_return

def calculate(folderloc, indexsymbol, currency, returnversion='GR', indexformula='dax'):
    '''Return indexvalue history
    
    filenames have to be of the format '{indexsymbol}_{YYYYMMDD}.csv',
    the date in the filename is the effective date, i.e. weights are
    open-weights; the files need to be comma-separated and include at
    least the columns 'SEDOL' and 'Weight'    
    
    Keyword arguments:
    folderloc -- path to folder that contains composition files (string)
    indexsymbol -- indexsymbol of index to be calculated
    returnversion -- GR: gross return (default), PR: price return
    indexformula -- dax: dividend invested in single stock, stoxx: dividend invested in portfolio
    '''
    basevalue = 100
    data = []
    
    datelist = sorted([pd.to_datetime(re.split('[\_\.]',x)[1],format='%Y%m%d').date() for x in os.listdir(folderloc) if x.startswith(indexsymbol+'_')])
    
    log = []
    
    for filedate in datelist:
        # define dates
        try:
            nextfiledate = [x for x in datelist if x > filedate][0]
        except:
            nextfiledate = dt.date(2079,1,1)

        # read composition to dataframe
        fileloc = folderloc + '/' + indexsymbol + '_' + dt.datetime.strftime(filedate,'%Y%m%d') + '.csv'
        comp = pd.read_csv(fileloc, dtype={'SEDOL':str})
        
        if returnversion == 'GR':
            # load total return indices from QAD DataStream tables
            prices = get_timeseries_gross_return(list(comp['SEDOL']),(filedate - BDay(5)).date(),(nextfiledate - BDay(1)).date(),currency,filedate).fillna(method='ffill')
            prices = prices.ix[filedate - BDay(1):]
        elif returnversion == 'PR':
            # load price return indices from QAD DataStream tables
            prices = get_timeseries_price_return(list(comp['SEDOL']),(filedate - BDay(5)).date(),(nextfiledate - BDay(1)).date(),currency,filedate).fillna(method='ffill')
            prices = prices.ix[filedate - BDay(1):]
        # standardize and apply weights
        for index, row in comp.iterrows():
            prices[row['SEDOL']] = prices[row['SEDOL']] * row['Weight'] / prices[row['SEDOL']].iloc[0]
        log.append([filedate, len(list(comp['SEDOL'])), sum(prices.iloc[0].fillna(0))])
        # sum up to get index timeseries
        index = prices.sum(axis=1)
        # scale to match previous indexvalue
        index = index / index.iloc[0] * basevalue
        # patch together
        data.append(index)
        basevalue = index[-1]

    index = pd.DataFrame(pd.concat(data)).drop_duplicates()
    index.columns = [indexsymbol+'_'+currency+'_'+returnversion]
    log = pd.DataFrame(columns=['FileDate','Components','Weight_Found'], data=log)
    return index, log