import requests
import numpy as np
import pandas as pd
import datetime as dt
from datetime import date, timedelta
from pandas.tseries.offsets import BDay
from io import StringIO
import requests
from requests.auth import HTTPBasicAuth
import calendar
from dateutil.relativedelta import relativedelta
import sys
sys.path.append(r"C:\Users\et246\Desktop\V0_SAMCO\STOXX")
import stoxx
import polars as pl

def get_prod_comp(symbol, date_i, oc):
    """" Reads index composition from STOXX composition folder/ SID/ STOXX website (priority in that order) and returns dataframe with composition
    This code should be valid even if the STOXX composition folder is no longer updated/ deleted. It will look for SID comps in that case.

    Parameters
    ----------
    - symbol : str, desired index symbol
    - date : datetime, desired date in datetime format
    - oc: str, desired type, 'open' or 'close'
    """

    proxies = {'http': 'http://webproxy.deutsche-boerse.de:8080',
               'https': 'http://webproxy.deutsche-boerse.de:8080', }

    symbol = symbol.lower()

    oc = oc.lower()
    if oc.lower() == 'open':
        Parse_Date = ['Next_Trading_Day']
    elif oc.lower() == 'close':
        Parse_Date = ['Date']

    try:
        # Prod composition folder: 'S:\Stoxx\Stoxx_Reports\stoxx_composition_files\STOXX\'
        comp = pd.read_csv('S:\Stoxx\Stoxx_Reports\stoxx_composition_files\STOXX\\' + symbol + '\\' + oc + '_' + symbol + '_' + date_i.strftime('%Y%m%d') + '.csv',
                           sep=";", encoding="ISO-8859-1", parse_dates=Parse_Date, dayfirst=True,
                           dtype={'SEDOL': str, 'Internal_Number': str, 'ISIN': str, 'RIC': str, 'ICB': str, 'Weight': float,
                                  'Shares': float, 'Free_Float': float, 'Close_adjusted_local': float,
                                  'FX_local_to_Index_Currency': float, 'Mcap_Units_Index_Currency': float}, index_col=False,
                            keep_default_na=False, na_values=['', '#N/A', '#N/A N/A', '#NA', '-NaN', '-nan', 'N/A', 'NULL', 'NaN', 'n/a', 'nan', 'null'])
        print ('%s %s source: STOXX composition folder.' %(symbol.upper(), date_i.strftime("%d-%m-%Y")))
    except:
        try:
            # Prod SID (MADDOX02): 'http://maddox2.prod.ci.dom/sidwebapi/Help/Api/GET-api-Index-GetOpenCloseComposition_indexSymbol_date_type
            compsidpath = 'http://maddox2.prod.ci.dom/sidwebapi/api/Index/GetOpenCloseComposition?indexSymbol=' + symbol + '&date=' + date_i.strftime('%Y-%m-%d') + '&type=' + oc
            comp =pd.read_csv(compsidpath, sep=";", encoding="ISO-8859-1", parse_dates=Parse_Date, dayfirst=True,
                              dtype={'SEDOL': str, 'Internal_Number': str, 'ISIN': str, 'RIC': str, 'ICB': str, 'Weight': float,
                                     'Shares': float, 'Free_Float': float, 'Close_adjusted_local': float,
                                     'FX_local_to_Index_Currency': float, 'Mcap_Units_Index_Currency': float}, index_col=False,
                              keep_default_na=False, na_values=['', '#N/A', '#N/A N/A', '#NA', '-NaN', '-nan', 'N/A', 'NULL', 'NaN', 'n/a', 'nan', 'null'])
            comp.rename(columns={'CapFactor':'Capfactor'}, inplace=True)
            if comp.empty:
                proxies = {'http': 'http://webproxy.deutsche-boerse.de:8080',
                           'https': 'http://webproxy.deutsche-boerse.de:8080', }
                # url = 'http://www.stoxx.com/download/data/composition_files/' + symbol + '/' + oc + '_' + symbol + '_' + date_i.strftime('%Y%m%d') + '.csv' - OLD link. Leads to old format, this is being phased out
                # New composition files in the website have missing fields:
                # 'CUSIP', 'Cash_Dividend_Amount', 'Cash_Dividend_Currency', 'Ci-factor', 'Corporate_Action_Description',
                #  'Country', 'Exchange', 'ICB', 'Mcap_Units_local', 'RIC', 'SEDOL', 'Special_Cash_Dividend_Amount', 'Special_Cash_Dividend_Currency'
                if oc.lower() == 'open':
                    correct_date_i = date_i - BDay(1)
                elif oc.lower() == 'close':
                    correct_date_i = date_i
                url = 'https://www.stoxx.com/document/Indices/Current/Composition_Files/' + oc + 'composition_' + symbol + '_' + correct_date_i.strftime('%Y%m%d') + '.csv'
                rr = requests.get(url.format(), stream=True, auth=HTTPBasicAuth('stoxxindex@stoxx.com', 'Welcome11'), proxies=proxies)
                dataTS = rr.text
                b = StringIO(dataTS)
                comp = pd.read_csv(b, sep=";", encoding="ISO-8859-1", parse_dates=Parse_Date, dayfirst=True,
                                   dtype={'SEDOL': str, 'Internal_Key': str, 'ISIN': str, 'RIC': str, 'ICB': str, 'Weight': float,
                                          'Shares': float, 'Free_Float': float, 'Close_adjusted_local': float,
                                          'FX_local_to_Index_Currency': float, 'Mcap_Units_Index_Currency': float}, index_col=False,
                                   keep_default_na=False, na_values=['', '#N/A', '#N/A N/A', '#NA', '-NaN', '-nan', 'N/A', 'NULL', 'NaN', 'n/a', 'nan', 'null'])
                comp.rename(columns={'Internal_Key': 'Internal_Number'}, inplace=True)
                # Some of the missing fields will be fetched from SID.
                othersid = 'http://maddox2.prod.ci.dom/sidwebapi/api/Security/getSecurityTimeSeriesByIndexSymbolCSV?calendarName=STOXXCAL&indexSymbol=' + symbol + '&targetCcy=EUR&startDate=' + date_i.strftime("%Y-%m-%d") + '&endDate=' + date_i.strftime("%Y-%m-%d")
                other_i = pd.read_csv(othersid, parse_dates=['vd', 'prevVd'], dayfirst=True, sep=";",
                                      dtype={'stoxx_id': str, 'isin': str, 'sedol': str, 'ric': str, 'icb_subsector': str, 'icb2_subsector': str, 'stoxxId_primary_company': str, 'stoxxId_primary_company': str},
                                      index_col=False, encoding="ISO-8859-1", keep_default_na=False,
                                      na_values=['', '#N/A', '#N/A N/A', '#NA', '-NaN', '-nan', 'N/A', 'NULL', 'NaN', 'n/a', 'nan', 'null'])
                other_i.rename(columns={'stoxx_id': 'Internal_Number', 'sedol': 'SEDOL', 'ric': 'RIC', 'icb5_subsector': 'ICB', 'iso_country_dom': 'Country', 'exchangeName':'Exchange'}, inplace=True)
                comp = pd.merge(comp, other_i[['Internal_Number','SEDOL','RIC', 'ICB',  'Country','Exchange']], on=['Internal_Number'], how='left')
                print('%s %s source: STOXX website.' % (symbol.upper(), date_i.strftime("%d-%m-%Y")))
            else:
                print('%s %s source: SID (MADDOX02).' % (symbol.upper(), date_i.strftime("%d-%m-%Y")))
        except:
            print ('Index composition was not found for %s for %s. Please investigate.' %(symbol.upper(), date_i.strftime("%d-%m-%Y")))
    try:
        cols = comp.columns.tolist()
        cols = [x for x in cols if 'Unnamed' not in x]
        comp = comp[cols]
    except:
        pass
    return comp

def last_friday_of_previous_month(date):
    first_day_of_current_month = date.replace(day=1)
    first_day_of_previous_month = first_day_of_current_month - relativedelta(months=1)
    
    # Find the last day of the previous month
    last_day_of_previous_month = first_day_of_current_month - pd.Timedelta(days=1)
    
    # Calculate the weekday of the last day of the previous month
    last_day_weekday = last_day_of_previous_month.weekday()
    
    # Calculate the difference between the last day's weekday and Friday (5)
    difference = (last_day_weekday - calendar.FRIDAY + 7) % 7
    
    # Subtract the difference to get the last Friday of the previous month
    last_friday = last_day_of_previous_month - pd.Timedelta(days=difference)
    
    return last_friday

def previous_month_working_day(input_date):
    # Convert the input date to a pandas datetime object
    input_date = pd.to_datetime(input_date)

    # Calculate the first day of the current month
    first_day_of_month = input_date.replace(day=1)

    # Calculate the last day of the previous month
    last_day_of_previous_month = first_day_of_month - pd.Timedelta(days=1)

    # Find the previous month's working day
    previous_working_day = last_day_of_previous_month
    while previous_working_day.weekday() in [5, 6]:  # 5 is Saturday, 6 is Sunday
        previous_working_day -= pd.Timedelta(days=1)

    return previous_working_day

def get_last_business_day_of_month(date):
    # Calculate the end of the month
    EOM = pd.to_datetime(date + pd.offsets.MonthEnd())

    # Check if the end of the month is a business day
    if not pd.offsets.BDay().is_on_offset(EOM):
        # If not, find the previous business day
        while not pd.offsets.BDay().is_on_offset(EOM):
            EOM -= pd.Timedelta(days=1)

    return EOM

idx = ["STXWAGV"]
opclo = "close"

# Create DataFrame with Review and Cutoff dates
Review_Date = pd.read_csv(r"C:\Users\et246\Desktop\V0_SAMCO\Dates\Review_Date-QUARTERLY.csv", parse_dates=["Review", "Cutoff"], index_col=0)
Output = pd.DataFrame()

for index in idx:
    for date in Review_Date["Cutoff"]:
        date = pd.to_datetime(date)
        cons = get_prod_comp(index, dt.date(date.year, date.month, date.day), oc = opclo)
        cons["Date"] = date
        Output = pd.concat((Output, cons))
        print(Output)

    try:
        Output = pl.from_pandas(Output)
    except:
        Output = Output.astype(str)
        Output = pl.from_pandas(Output)

    Output = Output.select(pl.col(["Date", "Internal_Number", "Mcap_Units_Index_Currency"]))

    Output.write_parquet(rf"C:\Users\et246\Desktop\V0_SAMCO\Universe\{index}_Cutoff.parquet")
    Output = pd.DataFrame()