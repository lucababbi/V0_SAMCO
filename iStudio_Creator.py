import polars as pl
import pandas as pd
from datetime import datetime
import os

# Capfactor from SWACALLCAP
CapFactor = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Universe\Capfactor_SWACALLCAP.parquet").with_columns(
    pl.col("Date").cast(pl.Date),
).select(pl.col(["Date", "Internal_Number", "Capfactor"])).to_pandas()

# Create the iStudio input
# Small_Index = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Output\Small_Index_Security_Level.csv", parse_dates=["Date"])
Small_Index = pd.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Output\Small_Index_Security_Level_0.85_UPDATE.csv", parse_dates=["Date"]).query("Date >= '2019-03-18'")

# Filter for needed columns
Frame = Small_Index[["Internal_Number", "SEDOL", "ISIN", "Date"]]

# Create weightFactor
Frame["weightFactor"] = 1

# Add CapFactor from SWACALLCAP
Frame = Frame.merge(CapFactor[["Date", "Internal_Number", "Capfactor"]], on=["Date", "Internal_Number"], how="left")

# Convert column Date
Frame["Date"] = Frame["Date"].dt.strftime('%Y%m%d')

# Renaming to convention
Frame = Frame.rename(columns={"Internal_Number": "STOXXID", "Date": "effectiveDate", "Capfactor": "capFactor"})

# Store the .CSV
# Get current date formatted as YYYYMMDD_HHMMSS
current_datetime = datetime.today().strftime('%Y%m%d')

# Store the .CSV with version and timestamp
Frame.to_csv(
        os.path.join(
            r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Output", 
            current_datetime + "_SMALL.csv"
        ), 
        index=False, 
        lineterminator="\n", 
        sep=";"
    )