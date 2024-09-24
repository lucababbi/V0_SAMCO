from stoxx.qad.identifier import get_vencode, get_vencodes, get_wspit_primary_vencodes
from stoxx.qad import con

import pandas as pd
import numpy as np
import datetime as dt
import pyodbc

def get_prod_wswspit(item, sedstr, dat, finalval, ws_type, minmth, fr): 
    
    """Arguments: 
    sedstr: first 6 char of Sedol or Sedol list (text); dat is a str like '2001-01-01'; 
    finalval is 'Y' (retreieve the last valid value available), 'N' (perform aggregation);
    ws_type is the set of tables 'ws' or 'wspit'
    
    Output:
    the field 'epsReportDate' corresponds to the 'pointdate' or 'startdate' field on wspit, 
    'fiscalPeriodEndDate' is the 'Calperiodenddate' or 'enddate' on wspit
    WSPIT tables covered: WSPITFinVal, WSPITCmpIssFData, WSPITSupp
    """
    
    creds1 = 'DRIVER={SQL Server};SERVER=Zurix04.bat.ci.dom\STOXXDBDEV2,55391;DATABASE=TSTXENG03;UID=stx-txg2a;PWD=stx-txg2a' #dbag

    conex = pyodbc.connect(creds1)
    
    if ws_type=='ws':
        db_ = 'usp_get_cumulative_fundamental_generic'
        freq_ = 'A,Q,S,R'
    elif ws_type=='wspit':
        db_ = 'usp_get_cum_fundamental_wspit_fin_all'
        if fr == 'Q':
            freq_ = '8,3'
        elif fr == 'QC':
            freq_ = '9,4'
        elif fr == 'A':
            freq_ = '1,2'
        else:
            freq_ = '1,2'

    sql = """
    set nocount on
    if object_id('tempdb..#fundamentals') is not null
    begin
        drop table #fundamentals;
    end;
    create table #fundamentals(
    sedol varchar(6),
    sedol7 varchar(7),
    isin nvarchar(48), 
    dj_id nvarchar(12),
    name VARCHAR(61),
    code INT NOT NULL,
    currencyOfDocument VARCHAR(12),
    epsReportDate DATETIME,
    fiscalPeriodEndDate DATETIME,
    value FLOAT,
    year_ smallint,
    freq varchar(1) ,
    item INT NOT NULL,
    seq SMALLINT NOT NULL ,
    periodUpdateFlag VARCHAR(12),
    itemUnits VARCHAR(9),
    latest_value smallint DEFAULT 0
    );

	exec %s ?, ?, ?, ?, ?, ?;
    select * from #fundamentals
    """
    try:
        res = pd.io.sql.read_sql(sql % db_, conex, params=[item, sedstr, dat, freq_, finalval, minmth])
        return res
    except:
        return pd.DataFrame()

def get_wspit(sedol, date, wspitItem, frequency=None):
    """Return the WSPIT item
    
    Keyword arguments:
    sedol -- 6 digit SEDOL of a security (string)
    date -- date the SEDOL was effective (datetime.date(year, month, day))
    wspitItem -- WSPIT item code of item to be returned (int)
        full list in table WSPITITEM in QAD
    frequency -- necessary for some items (string)
        A: Annual
        S: Semiannual
        Q: Quarterly
    """
    wspitCode = get_vencode(sedol, date, 35)
    sql_table = """
    SELECT Desc_
    FROM WSPITDesc
    WHERE Code = (SELECT Left(TableCode,1)
                  FROM WSPITITEM
                  WHERE WSPITITEM.Item = %s)
        AND Type_ = 1
    """
    try:
        table = pd.io.sql.read_sql(sql_table % str(wspitItem),con).loc[0,'Desc_']
    except:
        return np.nan
    sql_value = """
    SELECT *
    FROM %s
    WHERE Item = %s
        AND Code = %s
    """
    try:
        value = pd.io.sql.read_sql(sql_value % (table,str(wspitItem),str(wspitCode)),con)
    except:
        return np.nan
    if 'PointDate' in value.columns:
        value = value[value.PointDate < date]
    elif 'StartDate' in value.columns:
        value = value[value.StartDate < date].sort_values('StartDate')
    else:
        pass
    if 'FreqCode' in value.columns:
        if frequency == 'A':
            value = value[(value.FreqCode == 1) | (value.FreqCode == 2)].sort_values(['FiscalPrd','PointDate'])
        elif frequency == 'Q':
            value = value[(value.FreqCode == 8) | (value.FreqCode == 3)].sort_values(['FiscalPrd','PointDate'])
        elif frequency == 'S':
            value = value[(value.FreqCode == 10) | (value.FreqCode == 5)].sort_values(['FiscalPrd','PointDate'])
        else:
            return 'Frequency needed'
    else:
        pass
    try:
        return value.iloc[-1].Value_
    except:
        return np.nan


def get_wspit_sedollist(sedols, date, lagdate, wspitItem, sumAnnual, itemType='C', pit='Y', frequency=None):
    """Return the WSPIT item

    Keyword arguments:
    sedols -- list of 6 digit SEDOL of a security (string)
    date -- date the SEDOL was effective (datetime.date(year, month, day))
    lagdate -- date to lookup the WSPIT item
    wspitItem -- WSPIT item code of item to be returned (int)
        full list in table WSPITITEM in QAD
    itemType -- company level data (C) or security level data (S)
    sumAnnual -- 'Y'/'N' annualised data
    pit -- use point in time 'Y' or report date 'N'
    frequency -- necessary for some items (string)
        A: Annual
        S: Semiannual
        Q: Quarterly
    """

    if itemType == 'C':
        wspitCodes = get_wspit_primary_vencodes(sedols, date)
    else:
        wspitCodes = get_vencodes(sedols, date, 35)
    sql_table = """
    SELECT Desc_
    FROM WSPITDesc
    WHERE Code = (SELECT Left(TableCode,1)
                  FROM WSPITITEM
                  WHERE WSPITITEM.Item = ?)
        AND Type_ = 1
    """
    try:
        params = [str(wspitItem)]
        table = pd.io.sql.read_sql(sql_table, con=con, params=params).loc[0, 'Desc_']
    except:
        return np.nan
    sql_value = """
    SELECT *
    FROM %s
    WHERE Item = ?
        AND Code in (%s)
    """
    try:
        placeholder = ','.join('?' for i in range(len(wspitCodes['VenCode'].dropna().apply(str))))
        params = [str(wspitItem)] + list(wspitCodes['VenCode'].dropna().apply(str))
        value = pd.io.sql.read_sql(sql_value % (table, placeholder), con=con, params=params)
    except:
        return np.nan
    if 'PointDate' in value.columns:
        if pit == 'Y':
            value = value[value.PointDate < lagdate]
        else:
            value = value[(value.CalPrdEndDate < lagdate)]
    elif 'StartDate' in value.columns:
        value = value[value.StartDate < lagdate].sort_values(['Code', 'StartDate'])
    else:
        pass
    if 'FreqCode' in value.columns:
        if frequency == 'A':
            value = value[(value.FreqCode == 1) | (value.FreqCode == 2)].sort_values(['Code', 'FiscalPrd', 'PointDate'])
        elif frequency == 'Q':
            value = value[(value.FreqCode == 8)
                          | (value.FreqCode == 3) | (value.FreqCode == 10)
                          | (value.FreqCode == 5) | (value.FreqCode == 1)
                          | (value.FreqCode == 2)].sort_values(['Code', 'FiscalPrd', 'PointDate'])
        elif frequency == 'S':
            value = value[(value.FreqCode == 10)
                          | (value.FreqCode == 5) | (value.FreqCode == 1)
                          | (value.FreqCode == 2)].sort_values(['Code', 'FiscalPrd', 'PointDate'])
        else:
            return 'Frequency needed'
    else:
        pass
    dftable = pd.DataFrame(columns=['sedol', 'value'])
    try:
        temp = pd.DataFrame({'sedol': [np.nan], 'value': [np.nan]})
        dictSedol = wspitCodes.to_dict()['VenCode']
        for sedol in sedols:
            temp.loc[0, 'sedol'] = sedol
            try:
                dfwspit = value[value.Code == dictSedol[sedol[0:6]]]
                if 'FreqCode' in value.columns:
                    if len(dfwspit) > 0:
                        limitdate = lagdate - dt.timedelta(weeks = 78)
                        lastdate = dfwspit.CalPrdEndDate.max()
                        if lastdate < limitdate:
                            dfwspit = dfwspit[(dfwspit.CalPrdEndDate > limitdate)]
                        else:
                            firstdate = lastdate - dt.timedelta(weeks = 48)
                            dfwspit = dfwspit[(dfwspit.CalPrdEndDate > firstdate)]
                            dfwspit = dfwspit[dfwspit.Value_ > -1e20]
                            dfwspit = dfwspit.loc[dfwspit.groupby(['FreqCode', 'CalPrdEndDate']).PointDate.idxmax(), :]

                            if len(dfwspit[(dfwspit.FreqCode == 3) | (dfwspit.FreqCode == 8)]) == 4:
                                dfwspit = dfwspit[(dfwspit.FreqCode == 8) | (dfwspit.FreqCode == 3)].sort_values(
                                    ['Code', 'FiscalPrd', 'PointDate'])
                            elif len(dfwspit[(dfwspit.FreqCode == 5) | (dfwspit.FreqCode == 10)]) == 2:
                                dfwspit = dfwspit[(dfwspit.FreqCode == 5) | (dfwspit.FreqCode == 10)].sort_values(
                                    ['Code', 'FiscalPrd', 'PointDate'])
                            else:
                                dfwspit = dfwspit[(dfwspit.FreqCode == 1) | (dfwspit.FreqCode == 2)].sort_values(
                                    ['Code', 'FiscalPrd', 'PointDate'])
                    if sumAnnual == 'Y':
                        if len(dfwspit) == 0:
                            temp.loc[0, 'value'] = np.nan
                        else:
                            temp.loc[0, 'value'] = dfwspit.Value_.sum()
                    else:
                        temp.loc[0, 'value'] = dfwspit.iloc[-1].Value_
                else:
                    temp.loc[0, 'value'] = dfwspit.iloc[-1].Value_
            except:
                temp.loc[0, 'value'] = np.nan
            dftable = dftable.append(temp)
        return dftable
    except:
        return dftable


def get_wspit_sectorname(code):
    """Return sector name
    
    Keyword arguments:
    code -- ICB code (int / string)
    """
    try:
        code = int(code)
        sql = """
        SELECT Desc_
        FROM WSPITCode
        WHERE Type_ = 7040
            AND Code = %s
        """ % (str(code))
        return pd.io.sql.read_sql(sql, con).loc[0,'Desc_']
    except:
        sql = """
        SELECT Desc_
        FROM WSPITDesc
        WHERE Type_ = 7040
            AND Code = '%s'
        """ % (code)
        return pd.io.sql.read_sql(sql, con).loc[0,'Desc_']
        
def get_wspit_geoseg(sedol, date):
    """Return geographical revenue segment data

    Keyword arguments:
    sedol -- 6 digit SEDOL of a security (string)
    date -- date the SEDOL was effective (datetime.date(year, month, day))
    """
    wspitCode = get_vencode(sedol, date, 35)
    sql_table = """
    SELECT *
    FROM WSPITSeg
    WHERE Code = %s
        AND SegTypeCode = 2
        AND (FreqCode = 1 OR FreqCode = 2)
        AND PointDate <= '%s'
    """
    try:
        table_raw = pd.io.sql.read_sql(sql_table % (wspitCode,dt.date.strftime(date,'%Y-%m-%d')),con)
        table_recent = table_raw[table_raw.CalPrdEndDate == table_raw.CalPrdEndDate.max()]
    except:
        return pd.DataFrame(columns=['Code','PointDate','FreqCode','FiscalPrd','SegTypeCode','SegNum',
                                   'CalPrdEndDate',59500,59501])
    try:
        output = pd.DataFrame(columns=['Code','PointDate','FreqCode','FiscalPrd','SegTypeCode','SegNum',
                                   'CalPrdEndDate',59500,59501],index = range(1,table_recent.SegNum.max()+1))
    except:
        return pd.DataFrame(columns=['Code','PointDate','FreqCode','FiscalPrd','SegTypeCode','SegNum',
                                   'CalPrdEndDate',59500,59501])
    output['Code'] = table_recent.iloc[0]['Code']
    output['FiscalPrd'] = table_recent.iloc[0]['FiscalPrd']
    output['SegTypeCode'] = table_recent.iloc[0]['SegTypeCode']
    output['SegNum'] = range(1,table_recent.SegNum.max()+1)
    output['CalPrdEndDate'] = table_recent.iloc[0]['CalPrdEndDate'].date()
    pointDate = table_recent['PointDate'].min()
    output_hist = []
    for index, row in table_recent.iterrows():
        if row['PointDate'] != pointDate:
            output_hist.append(output.copy())
            pointDate = row['PointDate']
        output.loc[row['SegNum'],row['Item']] = row['Value_']
        output.loc[row['SegNum'],'PointDate'] = row['PointDate'].date()
        output.loc[row['SegNum'],'FreqCode'] = row['FreqCode']
    output_hist.append(output)
    output_hist_sales_clean = [o.replace('-1e38',np.nan).dropna(subset=[59500,59501]) for o in output_hist]
    output_hist_sales_clean = [o for o in output_hist_sales_clean if len(o) > 0]
    if len(output_hist_sales_clean) == 0:
        return pd.DataFrame(columns=['Code','PointDate','FreqCode','FiscalPrd','SegTypeCode','SegNum',
                                   'CalPrdEndDate',59500,59501])
    else:
        return output_hist_sales_clean[-1]

def get_wspit_geoseg_wspitcode(wspitCode, year):
    """Return geographical revenue segment data

    Keyword arguments:
    wspitCode
    year
    """
    sql_table = """
    SELECT *
    FROM WSPITSeg
    WHERE Code = %s
        AND SegTypeCode = 2
        AND (FreqCode = 1 OR FreqCode = 2)
        AND FiscalPrd = %s
    """    
    try:
        table_raw = pd.io.sql.read_sql(sql_table % (str(wspitCode),str(year)),con)
        table_recent = table_raw[table_raw.CalPrdEndDate == table_raw.CalPrdEndDate.max()]
    except:
        return pd.DataFrame(columns=['Code','PointDate','FreqCode','FiscalPrd','SegTypeCode','SegNum',
                                   'CalPrdEndDate',59500,59501])
    try:
        output = pd.DataFrame(columns=['Code','PointDate','FreqCode','FiscalPrd','SegTypeCode','SegNum',
                                   'CalPrdEndDate',59500,59501],index = range(1,table_recent.SegNum.max()+1))
    except:
        return pd.DataFrame(columns=['Code','PointDate','FreqCode','FiscalPrd','SegTypeCode','SegNum',
                                   'CalPrdEndDate',59500,59501])
    output['Code'] = table_recent.iloc[0]['Code']
    output['FiscalPrd'] = table_recent.iloc[0]['FiscalPrd']
    output['SegTypeCode'] = table_recent.iloc[0]['SegTypeCode']
    output['SegNum'] = range(1,table_recent.SegNum.max()+1)
    output['CalPrdEndDate'] = table_recent.iloc[0]['CalPrdEndDate'].date()
    pointDate = table_recent['PointDate'].min()
    output_hist = []
    for index, row in table_recent.iterrows():
        if row['PointDate'] != pointDate:
            output_hist.append(output.copy())
            pointDate = row['PointDate']
        output.loc[row['SegNum'],row['Item']] = row['Value_']
        output.loc[row['SegNum'],'PointDate'] = row['PointDate'].date()
        output.loc[row['SegNum'],'FreqCode'] = row['FreqCode']
    output_hist.append(output)
    output_hist_sales_clean = [o.replace('-1e38',np.nan).dropna(subset=[59500,59501]) for o in output_hist]
    output_hist_sales_clean = [o for o in output_hist_sales_clean if len(o) > 0]
    if len(output_hist_sales_clean) == 0:
        return pd.DataFrame(columns=['Code','PointDate','FreqCode','FiscalPrd','SegTypeCode','SegNum',
                                   'CalPrdEndDate',59500,59501])
    else:
        return output_hist_sales_clean[-1]