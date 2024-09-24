#from website.composition import get_composition_website
import pandas as pd
import zipfile
import datetime as dt
from pandas.tseries.offsets import BDay

def get_composition(indexsymbol, date, type_):
    """Return index composition
    
    Keyword arguments:
    indexsymbol -- STOXX index symbol (string)
    date -- composition effective date (datetime.date(year, month, day))
    type_ -- 'open' or 'close' (string)
    """
    try:
        return get_composition_website(indexsymbol, date, type_)
    except:
        pass
    try:
        return _get_composition_lepton(indexsymbol, date, type_)
    except:
        pass
    try:
        return _get_composition_globalarchive(indexsymbol, date, type_)
    except:
        pass
    try:
        return _get_composition_indexfactory(indexsymbol, date, type_)
    except:
        raise

def _get_composition_lepton(indexsymbol, date, type_):
    """Return index composition if available on "//STOXX_Reports"
    
    Keyword arguments:
    indexsymbol -- STOXX index symbol (string)
    date -- composition effective date (datetime.date(year, month, day))
    type_ -- 'open' or 'close' (string)
    """
    try:
        loc = '//frpnas06/stoxx_composition_files/STOXX/'+indexsymbol.lower()+'/'
        filename = type_.lower()+'_'+indexsymbol.lower()+'_'+dt.date.strftime(date,'%Y%m%d')+'.csv'
        return pd.read_csv(loc + filename, sep=';',index_col=False,dtype={'SEDOL':str,'ICB':str,'Internal_Number':str})
    except:
        loc = 'S:/Stoxx/Stoxx_Reports/stoxx_composition_files/STOXX/'+indexsymbol.lower()+'/'
        filename = type_.lower()+'_'+indexsymbol.lower()+'_'+dt.date.strftime(date,'%Y%m%d')+'.csv'
        return pd.read_csv(loc + filename, sep=';',index_col=False,dtype={'SEDOL':str,'ICB':str,'Internal_Number':str})       
    
def _get_composition_globalarchive(indexsymbol, date, type_):
    """Return index composition if available in global_archive file in
        "//Production/FinalSheets/s6/archive/"
    
    Keyword arguments:
    indexsymbol -- STOXX index symbol (string)
    date -- composition effective date (datetime.date(year, month, day))
    type_ -- 'open' or 'close' (string)
    """
    loc = 'S:/Stoxx/Production/FinalSheets/s6/archive/'
    if type_ == 'close':
        zipfilename = 'global_archive_' + dt.date.strftime(date,'%Y%m%d') + '.zip'
        sheetname = 'Close'
        datelabel = 'Date'
    elif type_ == 'open':
        zipfilename = 'global_archive_' + dt.date.strftime((date - BDay(1)).date(),'%Y%m%d') + '.zip'
        sheetname = 'Opening'
        datelabel = 'Next_Trading_Day'
    else:
        return pd.DataFrame()
    z = zipfile.ZipFile(loc + zipfilename,'r')
    filename = 'change_file_' + indexsymbol.lower() + '.xls'
    f = z.open(filename)
    df = pd.read_excel(f,sheetname=sheetname,skiprows=range(0,7),skip_footer=1,converters={'SEDOL':str,'Subsector':str})
    df.insert(0,datelabel,str(date))
    df = df.rename(columns = {'Wgt.':'Weight'})
    df = df.rename(columns = {'Forex':'Currency'})
    df = df.rename(columns = {'Capfac.   ':'Capfactor'})
    df = df.rename(columns = {'Float':'Free_Float'})
    df['Mcap_Units_Index_Currency'] = df['Mcap(EUR)']
    df['ICB'] = df['Subsector'].apply(lambda x: x.zfill(4))
    return df
    
def _get_composition_indexfactory(indexsymbol, date, type_):
    """Return index composition if converted from Index Factory composition and
        saved in "//vpzhnap05e/Team/stoxx/STOXX/Product Development & Research/
        Projects/Index Factory/IF file converter"
    
    Keyword arguments:
    indexsymbol -- STOXX index symbol (string)
    date -- composition effective date (datetime.date(year, month, day))
    type_ -- 'open' or 'close' (string)
    """
    
    filename = type_.lower()+'_'+indexsymbol.lower()+'_'+dt.date.strftime(date,'%Y%m%d')+'.csv'
    try:
        loc = '//frpnas06/Stoxx/Product Development and Research/Projects/Index Factory/IF file converter/'+indexsymbol.lower()+'/'
        return pd.read_csv(loc + filename, sep=';',index_col=False,dtype={'SEDOL':str,'ICB':str})
    except:
        loc = 'S:/Stoxx/Product Development and Research/Projects/Index Factory/IF file converter/'+indexsymbol.lower()+'/'
        return pd.read_csv(loc + filename, sep=';',index_col=False,dtype={'SEDOL':str,'ICB':str})