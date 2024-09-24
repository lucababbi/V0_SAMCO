import pandas as pd
import numpy as np
import os
from zipfile import ZipFile
import datetime as dt
from stoxx.qad.identifier import _get_sedol_from_ticker
from stoxx.qad.identifier import _get_isin_from_ticker

def ifconverter(zipfile, output, symbol, name, isin, type_, curr,
                from_date=dt.date(1,1,1), to_date=dt.date(2079,1,1), skiprows=0):
    """Convert Index Factory compositions to stoxx format open and
    close files
    
    Keyword arguments:
    zipfile -- path to Index Factory zip file (string)
    output -- path to output folder (string)
    symbol -- index symbol (string)
    name -- index short name, replace 'euro symbol' with 'EUR ' (string)
    isin -- index isin (string)
    type_ -- return type: 'Price', 'Net. Return', 'Gross Return' (string)
    curr -- currency (string)
    """    
    
    loc = '//frpnas06/Stoxx-Product Development and Research/Python/stoxx/tools/indexfactory/'
    
    close_cols = pd.read_csv(loc + 'close_.csv',sep=';').columns
    open_cols = pd.read_csv(loc + 'open_.csv',sep=';').columns
    country_if2stx = pd.read_csv(loc + 'country_if2stx.csv',sep=';')
    
    if not os.path.exists(output + '/' + symbol.lower()):
        os.makedirs(output + '/' + symbol.lower())
    
    z = ZipFile(zipfile,'r')
    
    for filepath in [x for x in z.namelist() if '.csv' in x]:
        try:
            date = dt.datetime.strptime(filepath[-14:-4],'%Y-%m-%d').date()
        except:
            continue
        if ((date < from_date) | (date > to_date)): continue
        f = z.open(filepath)
        df_if = pd.read_csv(f, sep=';', dtype={'SEDOL':str}, decimal='.',skiprows=skiprows)
        
        if len(df_if) == 0: continue
        
        df_if = pd.merge(df_if,country_if2stx, how='left', left_on='Country', right_on='Country_IF')
        
        df = pd.DataFrame(columns=set(open_cols).union(close_cols))
        
        df['Date'] = df_if['Date']
        df['Next_Trading_Day'] = df_if['Date']
        df['Index_Symbol'] = symbol.upper()
        df['Index_Name'] = name
        df['Index_ISIN'] = isin
        df['Index_Type'] = type_
        df['Index_Currency'] = curr
        df['Index_Component_Count'] = len(df)
        df['ISIN'] = df_if['ISIN']
        df['SEDOL'] = df_if['SEDOL']
        df['RIC'] = df_if['RIC']
        df['Currency'] = df_if['Currency']
        df['Country'] = df_if['Country_STX']
        df['ICB'] = df_if['ICB Subsector'].apply(str).str.zfill(4)
        
        if len(df['SEDOL'].dropna()) != len(df):
            for index, row in df.iterrows():
                if pd.isnull(row['SEDOL']):
                    try:
                        ticker = row['RIC'].split('.')[0]
                        country = row['Country']
                        df.loc[index,'SEDOL'] = _get_sedol_from_ticker(ticker, country, date)
                    except:
                        pass
        
        if len(df['ISIN'].dropna()) != len(df):
            for index, row in df.iterrows():
                if pd.isnull(row['ISIN']):
                    try:
                        ticker = row['RIC'].split('.')[0]
                        country = row['Country']
                        df.loc[index,'ISIN'] = _get_isin_from_ticker(ticker, country, date)
                    except:
                        pass
        
        df_close = df
        df_close['Weight'] = df_if['Weight in %']
        df_close['Mcap_Units_Index_Currency'] = df_if['Close (Euro)'] * df_if['Correction factor (CI)'] * df_if['Weight factor (QI)']
        df_close = df_close.sort_values('Weight',ascending=False)
        
        df_open = df
        df_open['Weight'] = df_if['Weight in %'] / df_if['Close (Euro)'] * df_if['Open (Euro)']
        df_open['Weight'] = np.round(df_open['Weight'] / df_open['Weight'].sum() * 100,7)
        df_open['Mcap_Units_Index_Currency'] = df_if['Open (Euro)'] * df_if['Correction factor (CI)'] * df_if['Weight factor (QI)']
        df_open = df_open.sort_values('Weight',ascending=False)
            
        df_close[close_cols].to_csv(output + '/' + symbol.lower() + '/close_'+symbol.lower()+'_'+dt.datetime.strftime(date,'%Y%m%d')+'.csv',
                        sep=';',
                        index=False,
                        float_format='%.5f')
        df_open[open_cols].to_csv(output + '/' + symbol.lower() + '/open_'+symbol.lower()+'_'+dt.datetime.strftime(date,'%Y%m%d')+'.csv',
                        sep=';',
                        index=False,
                        float_format='%.5f')