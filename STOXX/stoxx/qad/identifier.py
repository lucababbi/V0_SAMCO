from stoxx.qad import con
from pandas.io.sql import read_sql
import numpy as np
import datetime as dt
import pandas as pd

def get_vencode(sedol, date, venType):
    """Return the vendor code corresponding to a specified vendor
    
    Keyword arguments:
    sedol -- 6 digit SEDOL of a security (string)
    date -- date the SEDOL was effective (datetime.date(year, month, day))
    venType -- ventype of vendor code to be returned (int)
        33: DS2 (DataStream InfoCode)
        35: WSPIT (World Scope Point In Time)
        full list in table SecVenType in QAD
    """

    sql = """
    SELECT VenCode
    FROM %sSecSdl%sChg%s ssc, %sSecMapX smx
    WHERE ssc.Sedol = ?
        AND ssc.StartDate <= ?
        AND ssc.EndDate >= ?
        AND ssc.SecCode = smx.SecCode
        AND smx.Rank = ?
        AND smx.VenType = ?
    """
    d = dt.date.strftime(date,'%Y-%m-%d')
    params = [sedol,str(d),str(d), 1, str(venType)]
    vc = read_sql(sql % ('','','X',''), con=con, params=params)
    if len(vc)== 0:
        vc = read_sql(sql % ('G','','','G'),con=con, params=params)

    params = [sedol, str(d), str(d), 2, str(venType)]
    if len(vc)== 0:
        vc = read_sql(sql % ('','2','X',''),con=con, params=params)
    if len(vc) == 0:
        return np.nan
    else:
        return vc.loc[0,'VenCode']


def get_vencodes(sedols, date, venType):
    """Return the vendor code corresponding to a specified vendor

    Keyword arguments:
    sedol -- 6 digit SEDOL of a security (string)
    date -- date the SEDOL was effective (datetime.date(year, month, day))
    venType -- ventype of vendor code to be returned (int)
        33: DS2 (DataStream InfoCode)
        35: WSPIT (World Scope Point In Time)
        full list in table SecVenType in QAD
    """

    sql = """
    SELECT Sedol, VenCode
    FROM %sSecSdl%sChg%s ssc, %sSecMapX smx
    WHERE ssc.Sedol in %s
        AND ssc.StartDate <= '%s'
        AND ssc.EndDate >= '%s'
        AND ssc.SecCode = smx.SecCode
        AND smx.Rank = %s
        AND smx.VenType = %s
    """
    d = dt.date.strftime(date, '%Y-%m-%d')
    params = []
    vc = read_sql(sql % ('', '', 'X', '', "('"+"','".join([str(x)[:6] for x in sedols])+"')", d, d, 1, venType), con, index_col='Sedol')
    if len(vc) != len(sedols):
        vc_g = read_sql(sql % ('G', '', '', 'G', "('"+"','".join([str(x)[:6] for x in sedols])+"')", d, d, 1, venType), con, index_col='Sedol')
        if len(vc_g) != len(sedols):
            vc_1 = pd.concat((vc, vc_g))

            if len(vc_1) != len(sedols):
                vc_2 = read_sql(sql % ('', '2', 'X', '', "('"+"','".join([str(x)[:6] for x in sedols])+"')", d, d, 2, venType), con, index_col='Sedol')
                return pd.concat((vc_1, vc_2))
            else:
                return vc_1
        else:
            return vc_g
    else:
        return vc



def get_wspit_primary_vencodes(sedols, date):
    """Return the primary vendor code corresponding to a specified vendor

    Keyword arguments:
    sedol -- 6 digit SEDOL of a security (string)
    date -- date the SEDOL was effective (datetime.date(year, month, day))
        full list in table SecVenType in QAD
    """

    sql = """
    SELECT ssc.Sedol, prim.venCode
    FROM (SELECT *, 1 as typ_, 1 as rank_ 
        from SecSdlChgX union
        SELECT *, 1 as typ_, 2 as rank_
        from SecSdl2ChgX union
        SELECT *, 6 as typ_, 1 as rank_
        from GSecSdlChg) ssc
    join vw_WsPITCompanyMapping prim
    ON prim.typ = ssc.typ_ and prim.seccode = ssc.SecCode
    WHERE ssc.Sedol in (%s)
        AND ssc.StartDate <= ?
        AND ssc.EndDate >= ?
    """
    d = dt.date.strftime(date, '%Y-%m-%d')

    try:
        placeholder = ','.join('?' for i in range(len(sedols)))
        params = [str(x)[:6] for x in sedols] + [d, d]
        table = read_sql(sql % (placeholder), con=con, params=params)
        dftable = pd.DataFrame(columns=['Sedol', 'VenCode'])
        temp = pd.DataFrame({'Sedol': [np.nan], 'VenCode': [np.nan]})
        for sedol in sedols:
            temp.loc[0, 'Sedol'] = sedol[0:6]
            try:
                temp.loc[0, 'VenCode'] = table.loc[table.Sedol == sedol[0:6]]['venCode'].values[0]
                temp['VenCode'] = temp['VenCode'].astype(int)
            except:
                temp.loc[0, 'VenCode'] = np.nan
            dftable = dftable.append(temp)
        dftable = dftable.set_index('Sedol')
        return dftable
    except:
        return np.nan

def _get_sedol_from_ticker(ticker, country, date):
    """Return the SEDOL
    
    Keyword arguments:
    ticker -- Ticker (string)
    country -- Country code (string)
    date -- date the SEDOL was effective (datetime.date(year, month, day))
    """
    sql = """
    SELECT *
    FROM Ds2MnemChg mc, DS2SEDOLChg sc, DS2CtryQtInfo cqi
    WHERE mc.Ticker = '%s'
        AND mc.StartDate <= '%s'
        AND mc.EndDate >= '%s'
        AND mc.InfoCode = sc.InfoCode
        AND sc.StartDate <= '%s'
        AND sc.EndDate >= '%s'
        AND sc.InfoCode = cqi.InfoCode
        AND cqi.Region = '%s'
    """
    d = dt.date.strftime(date,'%Y-%m-%d')
    table = pd.io.sql.read_sql(sql % (ticker, d, d, d, d, country), con)
    if len(table) == 0:
        return np.nan
    else:
        return table.loc[0,'Sedol'] + table.loc[0,'SedolChk']

def _get_isin_from_ticker(ticker, country, date):
    """Return the ISIN
    
    Keyword arguments:
    ticker -- Ticker (string)
    country -- Country code (string)
    date -- date the SEDOL was effective (datetime.date(year, month, day))
    """
    sql = """
    SELECT *
    FROM Ds2MnemChg mc, DS2CtryQtInfo cqi, DS2IsinChg ic
    WHERE mc.Ticker = '%s'
        AND mc.StartDate <= '%s'
        AND mc.EndDate >= '%s'
        AND mc.InfoCode = cqi.InfoCode
        AND cqi.Region = '%s'
        AND cqi.DsSecCode = ic.DsSecCode
        AND ic.StartDate <= '%s'
        AND ic.EndDate >= '%s'
    """
    d = dt.date.strftime(date,'%Y-%m-%d')
    table = pd.io.sql.read_sql(sql % (ticker, d, d, country, d, d), con)
    if len(table) == 0:
        return np.nan
    else:
        return table.loc[0,'ISIN']

def get_infocode(identifier):
    #Legacy
    sql = """
    SELECT VenCode
    FROM %sSecMstrX AS t1, %sSecMapX AS t2, DS2CtryQtInfo AS t3
    WHERE t1.SecCode = t2.SecCode
        AND t2.VenType = 33
        AND t2.VenCode = t3.InfoCode
        AND t3.IsPrimQt = 1
        AND %s = '%s'
    """
    if len(identifier) > 7:
        identifier_type = 'Isin'
        flag = ''
    else:
        identifier = str(identifier)[0:6]
        identifier_type = 'Sedol'
        flag = '--'
    sql_us = sql % ('','',identifier_type,str(identifier))
    res = pd.io.sql.read_sql(sql_us,con).values
    if len(res) > 0:
        return res[0][0]
    else:
        sql_g = sql % ('G','G',identifier_type,str(identifier))
        res = pd.io.sql.read_sql(sql_g,con).values
        if len(res) > 0:
            return res[0][0]
        else:
            if identifier_type == 'Sedol':
                sql_ch = """
                SELECT t1.Infocode
                FROM DS2SedolChg t1, DS2CtryQtInfo t2
                WHERE t1.Sedol = '%s'
                    AND IsPrimQt = 1
                """ % (str(identifier))
            elif identifier_type == 'Isin':
                sql_ch = """
                SELECT Infocode
                FROM DS2IsinChg t1, DS2CtryQtInfo t2
                WHERE t1.Isin = '%s'
                    AND t1.DsSecCode = t2.DsSecCode
                    AND IsPrimQt = 1
                """ % (str(identifier))
            res = pd.io.sql.read_sql(sql_ch,con).values
            if len(res) > 0:
                return res[0][0]
            else:
                return np.nan

