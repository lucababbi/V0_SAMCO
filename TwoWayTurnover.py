from requests import get
import pandas as pd
from urllib3 import exceptions, disable_warnings
import datetime
import numpy as np
import sys
sys.path.append(r"C:\Users\et246\Desktop\V0_SAMCO\STOXX")
import stoxx

disable_warnings(exceptions.InsecureRequestWarning)

# User variables
Review_Date = pd.read_csv(r"C:\Users\et246\Desktop\V0_SAMCO\Dates\Review_Date-TWT.csv", parse_dates=["Review", "Cutoff"])

header = {
    "Content-Type": "application/json",
    "iStudio-User": "lbabbi@qontigo.com"
}

server = {
    "PROD": {
        "url": "https://vmindexstudioprd01:8002/",
        "ssl": False
    }
}

returns = {
    "NR": 2,
    "PR": 1,
    "GR": 3,
}

def fetch_batch_composition(batch_ids, Review_Date, Date_Download, server_type="PROD", batch_name="Output_file", file_path=r"C:\\Users\\et246\Desktop\\Vietnam\\"):
    compositions = []
    
    for batch_id in batch_ids:
        composition = pd.DataFrame()
        
        for current_date in Review_Date[Date_Download]:
            url = "{}/api/2.0/analytics/batch/{}/composition/export/{}/".format(
                server[server_type]["url"], batch_id, current_date.strftime("%Y-%m-%d"))
            json_result = get(url=url, headers=header, verify=server[server_type]["ssl"]).json()

            if "data" in json_result.keys():
                print("Components found for batch id: {}, keyword: {} at date - {}".format(
                    batch_id, batch_id, current_date))
                composition = composition.append(pd.json_normalize(json_result["data"]["composition_export"]))

        composition.reset_index(drop=True, inplace=True)

        composition = composition[composition.index_type.isin(['Price'])]
        composition = composition[composition.index_currency.isin(['USD'])]
    
    return composition

batch_ids = [20911]

# Get composition by Review Date
Date_Download = "Review"
Composition_T = fetch_batch_composition(batch_ids, Review_Date, Date_Download)

# Get previous business day Review Date
Review_Date["Review_T-1"] = Review_Date["Review"] - pd.tseries.offsets.BDay()
Date_Download = "Review_T-1"
Composition_T_Minus_1 = fetch_batch_composition(batch_ids, Review_Date, Date_Download)


# Add "Prev_Business_Day" to each column name
new_column_names = [f"{col}_Prev_Business_Day" for col in Composition_T_Minus_1.columns]

# Rename columns
Composition_T_Minus_1.rename(columns=dict(zip(Composition_T_Minus_1.columns, new_column_names)), inplace=True)

# Merge the two DataFrames
Composition_T = Composition_T.merge(Composition_T_Minus_1, left_on = ["close_day", "internal_number"], right_on = ["next_trading_day_Prev_Business_Day", "internal_number_Prev_Business_Day"],
                                    how = "outer")

# Calculate ABS Weights difference
Composition_T["weight"] = Composition_T["weight"].fillna(0)
Composition_T["ABS_Weight_Difference"] = abs(Composition_T["weight"] - Composition_T["weight_Prev_Business_Day"])

TwoWayTurnover = Composition_T.groupby("next_trading_day_Prev_Business_Day")["ABS_Weight_Difference"].sum()
TwoWayTurnover = TwoWayTurnover.reset_index()
TwoWayTurnover = TwoWayTurnover.rename(columns={"next_trading_day_Prev_Business_Day": "Review_Date", "ABS_Weight_Difference": "Two_Ways_Turnover"})
TwoWayTurnover.to_clipboard()