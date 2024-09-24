from stoxx.qad.identifier import get_vencode
from stoxx.qad.identifier import get_infocode
from stoxx.qad import con

import pandas as pd
import numpy as np
import datetime as dt

def get_sharesout(sedol, date):
    """Return the number of shares outstanding
    
    Keyword arguments:
    sedol -- 6 digit SEDOL of a security (string)
    date -- date the SEDOL was effective (datetime.date)
    """
    
    infoCode = get_vencode(sedol, date, 33)
    if np.isnan(infoCode):
        return np.nan
    else:
        sql = """
        SELECT NumShrs
        FROM DS2NumShares
        WHERE InfoCode = '%s'
            AND EventDate <= '%s'
        ORDER BY EventDate DESC
        """ % (infoCode, dt.date.strftime(date, '%Y-%m-%d'))
        res = pd.io.sql.read_sql(sql, con).values
        if len(res) > 0:
            return res[0][0] * 1000
        else:
            return np.nan

def get_freefloat(sedol, date):
    """Return the free-float percentage, 1 if not available
    
    Keyword arguments:
    sedol -- 6 digit SEDOL of a security (string)
    date -- date the SEDOL was effective (datetime.date)
    """
    
    infoCode = get_vencode(sedol, date, 33)
    if np.isnan(infoCode):
        return np.nan
    else:
        sql = """
        SELECT FreeFloatPct
        FROM DS2ShareHldgs
        WHERE InfoCode = '%s'
            AND ValDate <= '%s'
        ORDER BY ValDate DESC
        """ % (infoCode, dt.date.strftime(date, '%Y-%m-%d'))
        res = pd.io.sql.read_sql(sql, con).values
        if len(res) > 0:
            return res[0][0] / 100.0
        else:
            return 1.0

def get_divinfo(sedol, date):
    """Return dividend information
    lists dividends that are effective after 'date'
    
    Keywords arguments:
    sedol -- 6 digit SEDOL of a security (string)
    date -- date the SEDOL was effective (datetime.date)
    """
    infoCode = get_vencode(sedol, date, 33)
    if np.isnan(infoCode):
        return pd.DataFrame()
    else:
        sql = """
        SELECT *
        FROM DS2Div
        WHERE InfoCode = '%s'
            AND EffectiveDate >= '%s'
        """ % (infoCode, dt.date.strftime(date, '%Y-%m-%d'))
        res = pd.io.sql.read_sql(sql, con)
        if len(res) > 0:
            return res
        else:
            return pd.DataFrame()

def get_currency(sedol, date):
    """Return currency of security
    
    Keyword arguments:
    sedol -- 6 digit SEDOL of a security (string)
    date -- date the SEDOL was effective (datetime.date)
    """
    infoCode = get_vencode(sedol, date, 33)
    if np.isnan(infoCode):
        return np.nan
    else:
        sql = """
        SELECT PrimISOCurrCode
        FROM Ds2CtryQtInfo
        WHERE Infocode = '%s'
        """ % (str(infoCode))
        res = pd.io.sql.read_sql(sql, con).values
        if len(res) > 0:
            return res[0][0]
        else:
            return np.nan

def get_currency_ic(infoCode, date):
    """Return currency of security
    
    Keyword arguments:
    sedol -- 6 digit SEDOL of a security (string)
    date -- date the SEDOL was effective (datetime.date)
    """
    sql = """
    SELECT PrimISOCurrCode
    FROM Ds2CtryQtInfo
    WHERE Infocode = %s
    """ % (str(int(infoCode)))
    res = pd.io.sql.read_sql(sql, con).values
    if len(res) > 0:
        return res[0][0]
    else:
        return np.nan

def get_name(sedol, date):
    """Return name of security
    
    Keyword arguments:
    sedol -- 6 digit SEDOL of a security (string)
    date -- date the SEDOL was effective (datetime.date)
    """
    infoCode = get_vencode(sedol, date, 33)
    if np.isnan(infoCode):
        return np.nan
    else:
        sql = """
        SELECT DsQtName
        FROM Ds2CtryQtInfo
        WHERE Infocode = '%s'
        """ % (str(infoCode))
        res = pd.io.sql.read_sql(sql, con).values
        if len(res) > 0:
            return res[0][0]
        else:
            return np.nan

# returns local closing price of identifier on date
#def get_localclose(identifier, date):
#    ic = get_infocode(identifier)
#    if np.isnan(ic):
#        return np.nan
#    else:
#        sql = """
#        SELECT Close_
#        FROM DS2PrimQtPrc
#        WHERE InfoCode = '%s'
#        AND MarketDate <= '%s'
#        ORDER BY MarketDate DESC
#        """ % (str(ic), str(date))
#        res = pd.io.sql.read_sql(sql,con).values
#        if len(res) > 0:
#            return res[0][0]
#        else:
#            return np.nan

# download timeseries of field of list of identifiers from startdate to enddate, returns dataframe
# field in ['adj close', 'close', 'open', 'high', 'low', 'volume', 'bid', 'ask', 'total return']
#def get_timeseries(identifiers, field, startdate, enddate, currency):
#    if type(identifiers) == str:
#        identifiers = [identifiers]
#    if field.lower() not in ['adj close', 'close', 'open', 'high', 'low', 'volume', 'bid', 'ask', 'total return']:
#        print('Unknown field name ' + field)
#        return np.nan
#    elif field.lower() == 'adj close':
#        return get_timeseries_adj_close(identifiers, startdate, enddate, currency)
#    elif field.lower() == 'total return':
#        return get_timeseries_total_return(identifiers, startdate, enddate, currency)	
#    else:
#        if field.lower() == 'close' or field.lower() == 'open':
#            field = field + '_'		
#        data = []
#        for i in identifiers:
#            ic = get_infocode(i)
#            if np.isnan(ic):
#                df = pd.DataFrame(np.nan,index=[],columns=[i])
#            else:
#                sql = """
#                SELECT MarketDate, %s
#                FROM DS2PrimQtPrc
#                WHERE InfoCode = %s      
#                AND MarketDate >= '%s'
#                AND MarketDate <= '%s'
#                """ % (field, str(ic), str(startdate), str(enddate))     
#                df = pd.io.sql.read_sql(sql,con, index_col='MarketDate')
#                df.columns = [i]
#            data.append(df)
#        return data[0].join(data[1:], how='outer')
		
def get_timeseries_price_return(sedols, startdate, enddate, currency, sedoldate):
    """Return price return timeseries
    
    Keyword arguments:
    sedols -- SEDOL or list of SEDOLs (string / [string])
    startdate -- timeseries start date (datetime.date(year, month, day))
    enddate -- timeseries end date (datetime.date(year, month, day))
    currency -- timeseries currency iso code (str)
        'LOC' = local currency (tbd)
    """
    if type(sedols) == str:
        sedols = [sedols]
    data = []
    for sedol in sedols:
        infoCode = get_vencode(sedol[0:6], sedoldate, 33)
        seccurr = get_currency(sedol[0:6], sedoldate)
        if np.isnan(infoCode):
            df = pd.DataFrame(np.nan, index=pd.bdate_range(startdate, enddate), columns=[sedol])
        else:
            if (currency == seccurr) or (currency.lower() == 'loc'):
                sql = """
                SELECT pqp.MarketDate, pqp.Close_ * a.CumAdjFactor AS close_adjusted_loc
                FROM DS2PrimQtPrc pqp, DS2Adj a
                WHERE pqp.InfoCode = a.InfoCode
                    AND pqp.MarketDate between a.AdjDate and isnull(a.EndAdjdate, '20790101')
                    AND a.AdjType = 2
                    AND pqp.InfoCode = %s      
                    AND pqp.MarketDate >= '%s'
                    AND pqp.MarketDate <= '%s'
                """ % (str(infoCode), dt.date.strftime(startdate,'%Y-%m-%d'), dt.date.strftime(enddate,'%Y-%m-%d'))     
                df = pd.io.sql.read_sql(sql, con, index_col='MarketDate')
                df.columns = [sedol]
            else:
                sql = """
                SELECT pqp.MarketDate, pqp.Close_ * a.CumAdjFactor / fxr.MidRate AS close_adjusted
                FROM DS2PrimQtPrc pqp, DS2Adj a, DS2FXCode fxc, DS2FXRate fxr
                WHERE fxc.FromCurrCode = '%s'
                    AND fxc.ToCurrCode = '%s'
                    AND fxc.RateTypeCode = 'SPOT'
                    AND fxr.ExRateIntCode = fxc.ExRateIntCode
                    AND fxr.ExRateDate = pqp.MarketDate
                    AND pqp.InfoCode = a.InfoCode
                    AND pqp.MarketDate between a.AdjDate and isnull(a.EndAdjdate, '20790101')
                    AND a.AdjType = 2
                    AND pqp.InfoCode = '%s'      
                    AND pqp.MarketDate >= '%s'
                    AND pqp.MarketDate <= '%s'
                """ % (seccurr, currency, str(infoCode), dt.date.strftime(startdate,'%Y-%m-%d'), dt.date.strftime(enddate,'%Y-%m-%d'))
                df = pd.io.sql.read_sql(sql, con, index_col='MarketDate')
                df.columns = [sedol]
        if seccurr == 'GBP':
            df = df / 100.
        data.append(df)
    return pd.concat(data, axis=1, join='outer')

def get_timeseries_gross_return(sedols, startdate, enddate, currency, sedoldate):
    """Return total return timeseries
    
    Keyword arguments:
    sedols -- SEDOL or list of SEDOLs (string / [string])
    startdate -- timeseries start date (datetime.date(year, month, day))
    enddate -- timeseries end date (datetime.date(year, month, day))
    currency -- timeseries currency iso code (string)
        LOC: local currency (tbd)
    """
    if type(sedols) == str:
        sedols = [sedols]
    data = []
    for sedol in sedols:
        infoCode = get_vencode(sedol[0:6], sedoldate, 33)
        seccurr = get_currency(sedol[0:6], sedoldate)
        if np.isnan(infoCode):
            df = pd.DataFrame(np.nan, index=pd.bdate_range(startdate, enddate), columns=[sedol])
        else:
            if (currency == seccurr) or (currency.lower() == 'loc'):
                sql = """
                SELECT MarketDate, RI
                FROM DS2PrimQtRI
                WHERE InfoCode = '%s'
                    AND MarketDate >= '%s'
                    AND MarketDate <= '%s'
                """ % (str(infoCode), dt.date.strftime(startdate,'%Y-%m-%d'), dt.date.strftime(enddate,'%Y-%m-%d'))     
                df = pd.io.sql.read_sql(sql, con, index_col='MarketDate')
                df.columns = [sedol]
            else:
                sql = """
                SELECT r.MarketDate, r.RI / fxr.MidRate
                FROM DS2PrimQtRI r, DS2FXCode fxc, DS2FXRate fxr
                WHERE fxc.FromCurrCode = '%s'
                    AND fxc.ToCurrCode = '%s'
                    AND fxc.RateTypeCode = 'SPOT'
                    AND fxr.ExRateIntCode = fxc.ExRateIntCode
                    AND fxr.ExRateDate = r.MarketDate
                    AND r.InfoCode = '%s'
                    AND r.MarketDate >= '%s'
                    AND r.MarketDate <= '%s'
                """ % (seccurr, currency, str(infoCode), dt.date.strftime(startdate,'%Y-%m-%d'), dt.date.strftime(enddate,'%Y-%m-%d'))
                df = pd.io.sql.read_sql(sql, con, index_col='MarketDate')
                df.columns = [sedol]
        data.append(df)
    return pd.concat(data, axis=1, join='outer')

def get_adtv(sedol, startdate, enddate, currency):
    """Return average daily traded value
    The maximum turnover over all available exchanges is calculated    
    
    Keyword arguments:
    sedol -- 6 digit SEDOL of a security (string)
    startdate -- startdate of calculation period (datetime.date)
    enddate -- enddate of calculation period (datetime.date)
    currency -- target currency iso code (string)
        LOC: local currency
    """
    infoCode = get_vencode(sedol, enddate, 33)
    if np.isnan(infoCode):
        return np.nan
    else:
        seccurr = get_currency(sedol, enddate)
        if (currency.lower() == seccurr.lower()) or (currency.lower() == 'loc'):
            sqlprim = """
            SELECT AVG(Close_ * Volume) AS adtv
            FROM DS2PrimQtPrc
            WHERE InfoCode = '%s'
                AND MarketDate >= '%s'
                AND MarketDate <= '%s'
            """ % (str(infoCode), dt.date.strftime(startdate,'%Y-%m-%d'), dt.date.strftime(enddate,'%Y-%m-%d'))
            try:
                resprim = pd.io.sql.read_sql(sqlprim, con).replace([None],[np.nan]).loc[0,'adtv']
            except:
                resprim = np.nan
            sqlscd = """
            SELECT MAX(t.adtv) AS adtv
            FROM (SELECT AVG(Close_ * Volume) AS adtv
            FROM DS2ScdQtPrc
            WHERE InfoCode = '%s'
                AND MarketDate >= '%s'
                AND MarketDate <= '%s'
            GROUP BY ExchIntCode) as t
            """ % (str(infoCode), dt.date.strftime(startdate,'%Y-%m-%d'), dt.date.strftime(enddate,'%Y-%m-%d'))
            try:
                resscd = pd.io.sql.read_sql(sqlscd, con).replace([None],[np.nan]).loc[0,'adtv']
            except:
                resscd = np.nan
            try:
                res = np.nanmax([resprim,resscd])
            except:
                return np.nan
        else:
            sqlprim = """
            SELECT AVG(p.Close_ * p.Volume / fxr.MidRate) AS adtv
            FROM DS2PrimQtPrc p, DS2FXCode fxc, DS2FXRate fxr
            WHERE p.ISOCurrCode = fxc.FromCurrCode
                AND fxc.ToCurrCode = '%s'
                AND fxc.RateTypeCode = 'SPOT'
                AND fxr.ExRateIntCode = fxc.ExRateIntCode
                AND fxr.ExRateDate = p.MarketDate
                AND p.InfoCode = '%s'
                AND p.MarketDate >= '%s'
                AND p.MarketDate <= '%s'
            """ % (currency, str(infoCode), dt.date.strftime(startdate,'%Y-%m-%d'), dt.date.strftime(enddate,'%Y-%m-%d'))
            try:
                resprim = pd.io.sql.read_sql(sqlprim, con).replace([None],[np.nan]).loc[0,'adtv']
            except:
                resprim = np.nan
            sqlscd = """
            SELECT MAX(t.adtv)
            FROM(SELECT ExchIntCode, AVG(p.Close_ * p.Volume / fxr.MidRate) as adtv
            FROM DS2ScdQtPrc p, DS2FXCode fxc, DS2FXRate fxr
            WHERE p.ISOCurrCode = fxc.FromCurrCode
                AND fxc.ToCurrCode = '%s'
                AND fxc.RateTypeCode = 'SPOT'
                AND fxr.ExRateIntCode = fxc.ExRateIntCode
                AND fxr.ExRateDate = p.MarketDate
                AND p.InfoCode = '%s'
                AND p.MarketDate >= '%s'
                AND p.MarketDate <= '%s'
            GROUP BY ExchIntCode) as t
            """ % (currency, str(infoCode), dt.date.strftime(startdate,'%Y-%m-%d'), dt.date.strftime(enddate,'%Y-%m-%d'))
            try:
                resscd = pd.io.sql.read_sql(sqlscd,con).replace([None],[np.nan]).loc[0,'adtv']
            except:
                resscd = np.nan
            try:
                res = np.nanmax([resprim,resscd])
            except:
                return np.nan
        if seccurr == 'GBP':
            try:
                return res / 100.
            except:
                return np.nan
        else:
            return res

def get_adtv_ic_india(infoCode, startdate, enddate, currency):
    """Return average daily traded value
    The maximum turnover over all available exchanges is calculated    
    
    Keyword arguments:
    sedol -- 6 digit SEDOL of a security (string)
    startdate -- startdate of calculation period (datetime.date)
    enddate -- enddate of calculation period (datetime.date)
    currency -- target currency iso code (string)
        LOC: local currency
    """
    infoCode = int(infoCode)
    seccurr = get_currency_ic(infoCode, enddate)
    if (currency.lower() == seccurr.lower()) or (currency.lower() == 'loc'):
        sql = """
        SELECT AVG(Close_ * Volume) AS adtv
        FROM DS2ScdQtPrc
        WHERE InfoCode = '%s'
            AND ExchIntCode = 14
            AND MarketDate >= '%s'
            AND MarketDate <= '%s'
        """ % (str(infoCode), dt.date.strftime(startdate,'%Y-%m-%d'), dt.date.strftime(enddate,'%Y-%m-%d'))
        try:
            res = pd.io.sql.read_sql(sql, con).replace([None],[np.nan]).loc[0,'adtv']
        except:
            res = np.nan
    else:
        sql = """
        SELECT AVG(p.Close_ * p.Volume / fxr.MidRate) AS adtv
        FROM DS2ScdQtPrc p, DS2FXCode fxc, DS2FXRate fxr
        WHERE p.ISOCurrCode = fxc.FromCurrCode
            AND p.ExchIntCode = 14
            AND fxc.ToCurrCode = '%s'
            AND fxc.RateTypeCode = 'SPOT'
            AND fxr.ExRateIntCode = fxc.ExRateIntCode
            AND fxr.ExRateDate = p.MarketDate
            AND p.InfoCode = '%s'
            AND p.MarketDate >= '%s'
            AND p.MarketDate <= '%s'
        """ % (currency, str(infoCode), dt.date.strftime(startdate,'%Y-%m-%d'), dt.date.strftime(enddate,'%Y-%m-%d'))
        try:
            res = pd.io.sql.read_sql(sql, con).replace([None],[np.nan]).loc[0,'adtv']
        except:
            res = np.nan
    if seccurr == 'GBP':
        try:
            return res / 100.
        except:
            return np.nan
    else:
        return res
            
def get_fxrate(fromcurr, tocurr, date):
    """Return exchange rate
    Most recent if not available on date
    
    Keyword arguements:
    date -- (datetime.date)
    fromcurr -- (string)
    tocurr -- (string)
    """
    if fromcurr == tocurr:
        return 1
    else:
        sqlcode = """
        SELECT ExRateIntCode
        FROM DS2FXCode
        WHERE FromCurrCode = '%s'
            AND ToCurrCode = '%s'
            AND RateTypeCode = 'SPOT'
        """ % (fromcurr, tocurr)
        try:
            exrateintcode = pd.io.sql.read_sql(sqlcode,con).loc[0,'ExRateIntCode']
        except:
            return np.nan
        sqlrate = """
        SELECT MidRate
        FROM DS2FxRate
        WHERE ExRateIntCode = '%s'
            AND ExRateDate <= '%s'
            ORDER BY ExRateDate DESC
        """ % (str(exrateintcode), dt.date.strftime(date,'%Y-%m-%d'))
        try:
            return pd.io.sql.read_sql(sqlrate, con).loc[0,'MidRate']
        except:
            return np.nan

def get_curr_rate(curr1, curr2, datefrom, dateto):
    sqlstr = """
    select a.ExRateDate as date_, b.ToCurrCode as curr1, b.FromCurrCode as curr2, a.MidRate as value_
    from Ds2FxRate a 
    left join Ds2FxCode b
    on b.ExRateIntCode=a.ExRateIntCode
    where b.FromCurrCode = '%s'
    and b.ToCurrCode = '%s'
    and b.RateTypeCode='SPOT'
    and a.exratedate>='%s' 
    and a.exratedate<='%s'
    order by a.exratedate asc
    """ % (str(curr2), str(curr1), str(datefrom), str(dateto))
    if curr1==curr2:
        d = {'curr1': curr1, 'curr2': curr2, 'value_': 1}
        return pd.DataFrame(data=d, index=[datefrom, dateto])
    else:
        try:
            res= pd.io.sql.read_sql(sqlstr,con)
            res.index = res.date_
            res.index.name = None
            del res['date_']
            return res
        except:
            d = {'curr1': curr1, 'curr2': curr2, 'value_': np.nan}
            return pd.DataFrame(data=d, index=[datefrom, dateto])

## e.g. second Friday in month of the_date is nth_weekday(the_date,2,4)
#def nth_weekday(the_date, nth_week, week_day):
#    temp = the_date.replace(day=1)
#    adj = (week_day - temp.weekday()) % 7
#    temp += dt.timedelta(days=adj)
#    temp += dt.timedelta(weeks=nth_week-1)
#    return temp
#
#def add_months(date, months):
#    import calendar
#    month = int(date.month - 1 + months)
#    year = int(date.year + month / 12)
#    month = int(month % 12 + 1)
#    day = min(date.day, calendar.monthrange(year, month)[1])
#    return dt.date(year, month, day)