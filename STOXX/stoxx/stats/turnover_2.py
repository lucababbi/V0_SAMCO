import pandas as pd
import numpy as np
import stoxx.dates.keydates as kd

#df is a dataframe of the 'IndexReport_RuleModeler' csv file. freq is 'y', 'q', 'm', or a list of months
#Changed map_id='SEDOL' to map_id='ISIN'
def calc_turnover_df(df, freq, map_id='ISIN'):
    rd=kd.reviewdates()
    
    if freq == 'y':
        mnthlist=[9]
    elif freq == 'q':
        mnthlist=[3,6,9,12]
    elif freq == 'm':
        mnthlist=[1,2,3,4,5,6,7,8,9,10,11,12]
    else:
        mnthlist = freq
    
    dta=[]
    for index, rows in rd[rd.mth.isin(mnthlist)].iterrows():

        impdt =  str(pd.to_datetime(rows.impdt, format='%d.%m.%Y', dayfirst=True))[:10]
        effdt =  str(pd.to_datetime(rows.effdt, format='%d.%m.%Y', dayfirst=True))[:10]

        i = df[df.Date==impdt]
        i = i.rename(columns={'Weight in %':'wgtold'})
        e = df[df.Date==effdt]
        e = e.rename(columns={'Weight in %':'wgtnew'})
        x = pd.merge(i[[map_id,'wgtold']], e[[map_id,'wgtnew']], how='outer', on=map_id)
        x['wgtold'] = x['wgtold'].map(lambda x: float(x))
        x['wgtnew'] = x['wgtnew'].map(lambda x: float(x))
        count_in = len(x[x.wgtold.isnull()])
        count_out = len(x[x.wgtnew.isnull()])
        count_total = len(x[-x.wgtnew.isnull()])
        x.iloc[:,-2:] = x.iloc[:,-2:].fillna(0)
        to = (np.absolute(x['wgtold'] - x['wgtnew'])).sum() / 200
        dta.append([rows.yr, rows.mth, to, count_out, count_in, count_total])

    return pd.DataFrame(dta, columns=['year','month','turnover','count_out','count_in','count_total'])

	
def calc_turnover_prod(idx, freq):

    rd=kd.reviewdates()
    stoxx_reports = 'S:/Stoxx/Stoxx_Reports/stoxx_composition_files/STOXX/'

    if freq == 'y':
        mnthlist=[9]
    elif freq == 'q':
        mnthlist=[3,6,9,12]
    elif freq == 'm':
        mnthlist=[1,2,3,4,5,6,7,8,9,10,11,12]
    else:
        mnthlist = freq

    dta=[]
    for index, rows in rd[rd.mth.isin(mnthlist)].iterrows():

        impdt =  str(pd.to_datetime(rows.impdt, format='%d.%m.%Y', dayfirst=True))[:10]
        effdt =  str(pd.to_datetime(rows.effdt, format='%d.%m.%Y', dayfirst=True))[:10]

        fi = stoxx_reports + idx.lower() + '/' + 'close_' + idx.lower() + '_' + impdt.replace('-','') + '.csv'
        fe = stoxx_reports + idx.lower() + '/' + 'close_' + idx.lower() + '_' + effdt.replace('-','') + '.csv'
        try:
            i = pd.read_csv(fi, sep=';', dtype={'Internal_Number': object})[['Internal_Number','Weight']]
        except:
            i = pd.DataFrame(columns=['Internal_Number','Weight'])
        try:
            e = pd.read_csv(fe, sep=';', dtype={'Internal_Number': object})[['Internal_Number','Weight']]
        except:
            e = pd.DataFrame(columns=['Internal_Number','Weight'])

        i = i.rename(columns={'Weight':'wgtold'})
        e = e.rename(columns={'Weight':'wgtnew'})
        x = pd.merge(i[['Internal_Number','wgtold']], e[['Internal_Number','wgtnew']], how='outer', on='Internal_Number')
        count_in = len(x[x.wgtold.isnull()])
        count_out = len(x[x.wgtnew.isnull()])
        count_total = len(x[-x.wgtnew.isnull()])
        x.iloc[:,-2:] = x.iloc[:,-2:].fillna(0)
        to = (np.absolute(x['wgtold'] - x['wgtnew'])).sum() / 200
        dta.append([rows.yr, rows.mth, to, count_out, count_in, count_total])

    return pd.DataFrame(dta, columns=['year','month','turnover','count_out','count_in','count_total'])