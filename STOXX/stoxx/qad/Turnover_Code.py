import sys
sys.path.append('C:/Users/ec806/PycharmProjects/stoxx-pd-python/')
import pandas as pd
import numpy as np
import datetime as dt
import stoxx as stx
import matplotlib.pyplot as plt
import matplotlib
from pandas.tseries.offsets import BDay
from pandas.tseries.offsets import BMonthEnd
import os

from stoxx.calculator.dates import get_datelist
from stoxx.calculator.composition import get_composition
from stoxx.qad import con
from stoxx.qad.datastream import get_vencode
from stoxx.qad.identifier import get_vencodes
# from stoxx.qad.datastream import get_currencies
from stoxx.qad.identifier import get_infocode
from stoxx.qad.datastream import get_currency


def get_turnover_ratio(sedols,InfoCode, startdate, enddate, sedoldate=None):
    """Returns Turnover Ratio
    For each trading day, the trading volume is divided by its free-float shares.
    The Turnover Ratio is then defined as the median of this time series over the
    past twelve months, annualized by multiplying by 252.

    Keyword arguments:
    sedols -- list of SEDOLs ([string])
    enddate -- enddate of calculation period (pd.Timestamp)
    sedoldate -- efective date if SEDOLs (pd.Timestamp)
    """
    if sedoldate == None:
        sedoldate = enddate
    
    # startdate = enddate - pd.DateOffset(years=1)

    
    lst = {'Sedol': sedols, 'VenCode': InfoCode}
    infoCodes = pd.DataFrame(lst)
    infoCodes = infoCodes.set_index('Sedol',drop = False)
    infoCodes['VenCode'] = infoCodes.VenCode.apply(int)
    
    # Load InfoCode and map SEDOLs
    # infoCodes = get_vencodes(sedols, sedoldate, 33)
    # infoCodes['Sedol'] = infoCodes.index
    # infoCodes['VenCode'] = infoCodes.VenCode.apply(int)
    try:
        # Pull daily volumes, shares outstanding, free-float factors (forward-fill so and ff)
        sql_pqp = """
        SELECT InfoCode, MarketDate, Volume
        FROM DS2PrimQtPrc
        WHERE InfoCode IN ('%s')
            AND MarketDate >= '%s'
            AND MarketDate <= '%s'
        """ % ("','".join(infoCodes.loc[:, 'VenCode'].apply(str)),
               startdate.strftime('%Y-%m-%d'), enddate.strftime('%Y-%m-%d'))
        res_pqp = pd.read_sql(sql_pqp, con).replace([None], [np.nan])
        
        sql_ns = """
        SELECT InfoCode, EventDate AS MarketDate, NumShrs * 1000 AS NumShrs
        FROM Ds2NumShares
        WHERE InfoCode IN ('%s')
            AND EventDate <= '%s'
        """ % ("','".join(infoCodes.loc[:, 'VenCode'].apply(str)), enddate.strftime('%Y-%m-%d'))
        res_ns = pd.read_sql(sql_ns, con).replace([None], [np.nan])
        
        sql_sh = """
        SELECT InfoCode, ValDate AS MarketDate, FreeFloatPct / 100 AS FreeFloatPct
        FROM Ds2ShareHldgs
        WHERE InfoCode IN ('%s')
            AND ValDate <= '%s'
        """ % ("','".join(infoCodes.loc[:, 'VenCode'].apply(str)), enddate.strftime('%Y-%m-%d'))
        res_sh = pd.read_sql(sql_sh, con).replace([None], [np.nan])
        
        df = pd.merge(res_pqp, res_ns, how='outer', on=['MarketDate','InfoCode'])
        df = pd.merge(df, res_sh, how='outer', on=['MarketDate','InfoCode'])
        df = df.sort_values(['InfoCode','MarketDate'])
        df = pd.merge(df, infoCodes, how='left', left_on='InfoCode', right_on='VenCode')
        
        df[['NumShrs','FreeFloatPct']] = df.groupby('InfoCode').fillna(method='ffill')[['NumShrs','FreeFloatPct']]
        df['FreeFloatPct'] = df['FreeFloatPct'].fillna(1)
        
        # Calculate median turnover ratio over last twelve months
        df = df.loc[df.MarketDate >= startdate].dropna(subset=['Volume'])
        df['Turnover_Ratio'] = df.Volume / (df.NumShrs * df.FreeFloatPct) * 252
        df_toratio = df.groupby('Sedol').agg(
            {'InfoCode':'median','Turnover_Ratio':'median','Volume':'count'}).rename(columns={'Volume':'VolumeCount'})
        
        return df_toratio
    except:
        df = pd.DataFrame(columns =['InfoCode','Turnover_Ratio','VolumeCount'])
        df.index.name = 'Sedol'
        return df


def get_company_code(sedols, sedoldate):
    # Load InfoCode and map SEDOLs
    infoCodes = get_vencodes(sedols, sedoldate, 33)
    infoCodes['Sedol'] = infoCodes.index
    infoCodes['VenCode'] = infoCodes.VenCode.apply(int)
    
    # Pull DsCompyCode
    sql = """
    SELECT PrimQtInfoCode AS InfoCode, DsCmpyCode
    FROM Ds2Security
    WHERE PrimQtInfoCode IN ('%s')
    """ % ("','".join(infoCodes.loc[:, 'VenCode'].apply(str)))
    res = pd.read_sql(sql, con).replace([None], [np.nan])
    
    df = pd.merge(res, infoCodes, how='left', left_on='InfoCode', right_on='VenCode')
    
    return df


def get_adtv_DR_Index(sedol, startdate, enddate, currency,ExchIntCode):
    """Return average daily traded value
    The maximum turnover over all available exchanges is calculated    
    
    Keyword arguments:
    sedol --List of 6 digit SEDOL of a security (string)
    startdate -- startdate of calculation period (datetime.date)
    enddate -- enddate of calculation period (datetime.date)
    currency -- target currency iso code (string)
        LOC: local currency
    ExchIntCode -- List of Exchange code
    """
    infoCode = get_vencodes(sedol, enddate, 33)
    infoCode = infoCode.astype(str)
    seccurr = 'USD'   #get_currency(sedol[0], enddate)''
    

    sqlprim = """
    SELECT p.InfoCode,AVG(p.Close_ * p.Volume / fxr.MidRate) AS ADTV 
    FROM DS2PrimQtPrc p, DS2FXCode fxc, DS2FXRate fxr
    WHERE p.ISOCurrCode = fxc.FromCurrCode
        AND fxc.ToCurrCode = '%s'
        AND fxc.RateTypeCode = 'SPOT'
        AND fxr.ExRateIntCode = fxc.ExRateIntCode
        AND fxr.ExRateDate = p.MarketDate
        AND p.InfoCode in %s
        AND p.MarketDate >= '%s'
        AND p.MarketDate <= '%s'
        Group by  p.InfoCode
    """ % (currency, (str(tuple(x[0] for x in infoCode[[infoCode.columns[0]]].values))), dt.date.strftime(startdate,'%Y-%m-%d'), dt.date.strftime(enddate,'%Y-%m-%d'))
    try:
        resprim = pd.io.sql.read_sql(sqlprim, con).replace([None],[np.nan])
    except:
        resprim = np.nan
    sqlscd = """
    SELECT p.InfoCode,AVG(p.Close_ * p.Volume / fxr.MidRate) as ADTV
    FROM DS2ScdQtPrc p, DS2FXCode fxc, DS2FXRate fxr
    WHERE p.ISOCurrCode = fxc.FromCurrCode
        AND fxc.ToCurrCode = '%s'
        AND fxc.RateTypeCode = 'SPOT'
        AND fxr.ExRateIntCode = fxc.ExRateIntCode
        AND fxr.ExRateDate = p.MarketDate
        AND p.InfoCode in %s
        AND p.MarketDate >= '%s'
        AND p.MarketDate <= '%s'
        AND ExchIntCode in %s
    GROUP BY ExchIntCode,p.InfoCode
    """ % (currency, (str(tuple(x[0] for x in infoCode[[infoCode.columns[0]]].values))), dt.date.strftime(startdate,'%Y-%m-%d'), dt.date.strftime(enddate,'%Y-%m-%d'),tuple(ExchIntCode))
    try:
        resscd = pd.io.sql.read_sql(sqlscd,con)
    except:
        resscd = np.nan
    try:
        res = resprim.append(resscd)
        res = res.groupby('InfoCode').max().reset_index()
        res = res.dropna()                
    except:
        return np.nan
    if seccurr == 'GBP':
        res['ADTV'] = res['ADTV']/ 100
    try:
        return res
    except:
        return np.nan
    else:
        return res