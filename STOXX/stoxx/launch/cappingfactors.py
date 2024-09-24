import pandas as pd
import numpy as np

#received a df with -> column0:weight ;column1=cap; returns df with additional column2:capfactor; colum3:cappedwgt
#reindexes the df starting with 1
def calccapfacs(df_comp): 
    df_comp = df_comp.sort(df_comp.columns[0],ascending=False)
    df_comp.index = range(1,len(df_comp)+1)
    df_comp['capfactor']=1
    if sum(df_comp.iloc[:,1])<=1.:   
        df_comp['cappedwgt'] = 1. / len(df_comp) #equal weight
    else:
        df_comp['cappedwgt'] = df_comp.iloc[:,0]
        while len(df_comp[np.round(df_comp.cappedwgt, 7) > np.round(df_comp.iloc[:,1], 7)]) > 0:
            dblToCap = df_comp[df_comp.cappedwgt >= df_comp.iloc[:,1]].cap.sum()
            weightsnocap = df_comp[df_comp.cappedwgt < df_comp.iloc[:,1]].cappedwgt.sum()
            dblDistFactor = weightsnocap / (1 - dblToCap)
            for index, row in df_comp.iterrows():
                if row['cappedwgt'] >= row[1]: 
                    df_comp.loc[index,'cappedwgt'] = dblDistFactor * row[1]
            dblcappedsum = df_comp.cappedwgt.sum()
            df_comp['cappedwgt'] = df_comp['cappedwgt'] / dblcappedsum
    df_comp['capfactor']=(df_comp['cappedwgt']/df_comp.iloc[:,0])/max(df_comp['cappedwgt']/df_comp.iloc[:,0])
    return df_comp

#calculates weights and capping factors minimizing the squared sum of the deviation with the intended weights
#received a df with column0:weight (used as initial guess) ;column1=cap; 
#returns df with additional column2:capfactor; colum3:cappedwgt
#method met_ =1: TNC (Truncated Newton's algorithm); met_ =2: L-BFGS-B (limited memory BFGS)
#reindexes the df starting with 1
def cap_with_min_devs(dfx, met_=1):
    from scipy.optimize import minimize
    dfx['capfactor']=1.
    options={1 : 'TNC',
         2 : 'L-BFGS-B'} #for methods 1 and 2
    def wgtfun(x):
        return sum(x-wt)**2
    x = np.array(dfx.iloc[:,0])
    wt = x
    b = [(0.,dfx.iloc[i,1]) for i in range(len(dfx))]
    c = ({'type':'eq', 'fun': lambda x: sum(x)-1. })  #methods TNC and L-BFGS-B cannot handle constraints
    
    res=minimize(wgtfun, x , method=options[met_], bounds=b, constraints=c)
    dfx['cappedwgt']=res.x
    dfx['capfactor']=(dfx['cappedwgt']/dfx.iloc[:,0])/max(dfx['cappedwgt']/dfx.iloc[:,0])
    return dfx