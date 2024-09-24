import pandas as pd
import sys
import numpy as np
import datetime as dt
sys.path.append('S:/Stoxx/Product Development and Research/Python')
import stoxx.dates.keydates as kd

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
	
	
def calc_roll_dy(df, freq, window):
    """Returns the rolling dividend yields actual and annualized with 250 observations for a year
    
    Keyword arguments:
    df -- dataframe with columns: date, price, net, gross
    freq -- 'y', 'q', 'm' (yearly, quarterly, monthly)
    window -- window in years (1, 2, 3 ..)
    """
    dates=pd.DatetimeIndex(df.iloc[:,0])

    if freq=='y':
        stp = 1
        isyearend=(dates.month[0:len(dates)-1]>dates.month[1:len(dates)])
        period_end = dates[numpy.append(isyearend, True)]
    elif freq=='q':
        stp = 4
        datesq=dates[dates.map(lambda x: x.month==3 or x.month==6 or x.month==9 or x.month==12)]
        isquarterend =(datesq.day[0:len(datesq)-1]>datesq.day[1:len(datesq)])
        period_end = datesq[np.append(isquarterend, True)]
    else:
        stp = 12
        ismonthend=(dates.day[0:len(dates)-1]>dates.day[1:len(dates)])
        period_end = dates[np.append(ismonthend, True)]

    dflastday=pd.DataFrame(period_end)
    dflastday=pd.concat([dflastday, df.iloc[[0,len(df)-1],0]], axis=0).sort(0, ascending=True).drop_duplicates().reset_index(drop=True)
    df1 = pd.merge(dflastday, df, left_on=dflastday.columns[0], right_on=df.columns[0])

    del df1[df1.columns[0]]
    df1.index=df1[df1.columns[0]]
    df1.index.name = None
    del df1[df1.columns[0]]

    per = []; returns = []; annreturns = []

    for x in range(1,len(df1)):
        stp1 = stp * window
        if x-stp1>=0:
            per.append((str(df1.index[x-stp1])[:10]+' / '+ str(df1.index[x])[:10]))
            ct = len(df[(df.Date>df1.index[x-stp1]) & (df.Date<=df1.index[x])])
            rets = np.array(df1.iloc[x,:]) / np.array(df1.iloc[x-stp1,:])
            returns.append(list(rets-1))
            annreturns.append(list((rets)**(250/ct)-1))
        else:
            per.append(np.nan)
            returns.append([np.nan,np.nan,np.nan])
            annreturns.append([np.nan,np.nan,np.nan])

    dyact = pd.DataFrame([np.array(returns)[:,1]-np.array(returns)[:,0], np.array(returns)[:,2]-np.array(returns)[:,0]])
    dyann = pd.DataFrame([np.array(annreturns)[:,1]-np.array(annreturns)[:,0], np.array(annreturns)[:,2]-np.array(annreturns)[:,0]])

    dfdy = pd.concat([pd.DataFrame(per), dyact.T, dyann.T], axis=1)
    dfdy.columns=['period', 'DY_net_act','DY_gross_act','DY_net_ann','DY_gross_ann']
    dfdy = dfdy[-dfdy.period.isnull()]
    
    return dfdy
	

	
	