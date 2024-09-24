import pandas as pd
import numpy as np
import datetime as dt
import os

#input dataframe needs a 'weight' column and a internal_symbol_col column

def get_4d_comp_file(df, indexsymbol, internal_symbol_col): #date in format: 20151013
    df = get_4d_wf_fields(df, indexsymbol)
    df = df[['valid_from', 'valid_to', 'index_symbol',  internal_symbol_col, 'size', 'description', 'not_rep_before']]
    df.columns = ['valid_from', 'valid_to', 'index_symbol', 'dj_id', 'size', 'description', 'not_rep_before']
    return df

def get_4d_wf_file(df, cutdt, indexsymbol, internal_symbol_col): #date in format: 20151013
    dfprod = get_prod_close_EUR(cutdt)
    df = pd.merge(df, dfprod[['ISIN','internal_key','close_eur']], left_on=internal_symbol_col, right_on='internal_key', how='left')
    df = get_4d_wf_fields(df, indexsymbol)
    df['weightfactor'] = np.around(100000000000*df['weight'] / df['close_eur'],0)
    df['weightfactor'] = df['weightfactor'].map(lambda x: int(x))
    df = df[['valid_from', 'valid_to', 'index_symbol', internal_symbol_col, 'weightfactor', 'capfactor', 'description', 'not_rep_before']]
    df.columns = ['valid_from', 'valid_to', 'index_symbol', 'dj_id', 'weightfactor', 'capfactor', 'description', 'not_rep_before']
    return df

def get_4d_wf_and_comp_file_if(loc_h, loc_if, target_loc):
    '''qets 4d wf upload files using index factory weightfactors and ci factors based on the 
    IndexDefinition file
    '''
    today = dt.date.today() 
    if today.weekday() == 0:
        yesterday = str(dt.date.today() - dt.timedelta(days=3)).replace('-','')
    else:
        yesterday = str(dt.date.today() - dt.timedelta(days=1)).replace('-','')
		
    dfdef = pd.read_excel(loc_h + 'Index_Definition.xlsx')
    dfdef = dfdef[dfdef['type<quote>']=='Price'][['jobID','symbol<quote>','type<quote>']]

    for index, row in dfdef.iterrows():
        for filename in os.listdir(loc_if):
            if filename[-9:][:-4]==str(int(row.jobID)):
                f = filename
                df = pd.read_csv(loc_if + f, sep=';')
                print(len(df), 'components in ', row['symbol<quote>'])
                df = df[['Date','ISIN','Organisation name','SEDOL','Correction factor (CI)','Weight factor (QI)']].copy()
                df.columns = ['date','ISIN','name','sedol','corrfact','wgtfact']
                df['wf'] = np.around(df['corrfact'] * df['wgtfact'] * 100, 0)
                df['wf'] = df['wf'].map(lambda x: int(x))

                dfprod = get_prod_close_EUR(yesterday)
                df = pd.merge(df, dfprod[['ISIN','internal_key']], on='ISIN', how='left')
                df = get_4d_wf_fields(df, row['symbol<quote>'])
                df['weightfactor'] = df['wf']
                
                #component files
                dfcomp = df[['valid_from', 'valid_to', 'index_symbol',  'internal_key', 'size', 'description', 'not_rep_before']]
                dfcomp.columns = ['valid_from', 'valid_to', 'index_symbol', 'dj_id', 'size', 'description', 'not_rep_before']
                
                dfcomp.to_csv(target_loc + row['symbol<quote>'] + '_prod_composition_'+ 
                            str(dt.date.today()).replace('-','') +'.csv' ,sep=';',index=False)
                
                #wf files
                dfwf = df[['valid_from', 'valid_to', 'index_symbol', 'internal_key', 'weightfactor', 'capfactor', 'description', 'not_rep_before']]
                dfwf.columns = ['valid_from', 'valid_to', 'index_symbol', 'dj_id', 'weightfactor', 'capfactor', 'description', 'not_rep_before']
                
                dfwf.to_csv(target_loc + row['symbol<quote>'] + '_prod_weightfactor_'+ 
                            str(dt.date.today()).replace('-','') +'.csv' ,sep=';',index=False)
	
def get_prod_close_EUR(dt): #date in format: 20151013
    df = pd.read_csv('S:/Stoxx/Production/FinalSheets/s6/archive/stoxx_global_'+ dt +'.txt', sep=';')
    df=trim_rows_cols(df)
    return df

def get_4d_wf_fields(df, indexsymbol): #date is the cut-off for wf calculation
    tomorro = str(dt.date.today()+dt.timedelta(days=1)).replace('-','')
    df['valid_from'] = tomorro
    df['valid_to'] = 99991231
    df['index_symbol'] = indexsymbol
    df['size'] = 'Y'
    df['description'] = np.nan
    df['not_rep_before'] = tomorro
    df['weightfactor'] = 1
    df['capfactor'] = 1
    return df

def trim_rows_cols(df):
    cols=df.columns
    cols=cols.map(lambda x: x.strip())
    df.columns=cols
    for c in df.columns:
        try: 
            df[c] = df[c].map(lambda x: x.strip())
        except:
            a=1
    return df