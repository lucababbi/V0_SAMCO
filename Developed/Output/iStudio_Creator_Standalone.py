import polars as pl
import pandas as pd
from datetime import datetime
import datetime
import os

# Capfactor from SWACALLCAP
CapFactor = pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Universe\Capfactor_SWACALLCAP.csv").with_columns(
    pl.col("Date").cast(pl.Date),
).select(pl.col(["Date", "Internal_Number", "Capfactor"])).filter(pl.col("Date") < datetime.date(2024,6,24)).to_pandas()

# Capfactor adjusted for JUN-SEP 2024
FreeFloat_TMI = pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Universe\Capfactor_TMI.csv", separator=",",
                infer_schema=False).with_columns(
                pl.col("validDate").str.strptime(pl.Date, "%Y%m%d").alias("validDate"),
                pl.col("freeFloat").cast(pl.Float64).alias("Free_Float_TMI")
)

# Select columns to read from the Parquets
Columns = ["Date", "Index_Symbol", "Index_Name", "Internal_Number", "ISIN", "SEDOL", "RIC", "Instrument_Name", 
           "Country", "Currency", "Exchange", "ICB", "Free_Float", "Capfactor", "Shares", "Close_unadjusted_local", "FX_local_to_Index_Currency"]

FreeFloat_SW = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Universe\SWDACGV.parquet", columns=Columns).with_columns([
                            pl.col("Free_Float").cast(pl.Float64),
                            pl.col("Capfactor").cast(pl.Float64),
                            pl.col("Shares").cast(pl.Float64),
                            pl.col("Close_unadjusted_local").cast(pl.Float64),
                            pl.col("FX_local_to_Index_Currency").cast(pl.Float64),
                            pl.col("Date").cast(pl.Date)
                            ]).select(pl.col(["Date", "Internal_Number", "Free_Float"])).filter(pl.col("Date") >= datetime.date(2024,6,24))

CapFactor_JUNSEP = FreeFloat_SW.join(FreeFloat_TMI.select(pl.col(["validDate", "stoxxId", "Free_Float_TMI"])), left_on=["Date", "Internal_Number"],
                                    right_on=["validDate", "stoxxId"], how="left").with_columns(
                                    (pl.col("Free_Float") / pl.col("Free_Float_TMI")).alias("Capfactor")
                                     ).select(pl.col(["Date", "Internal_Number", "Capfactor"])
                                     ).to_pandas()

CapFactor = pd.concat([CapFactor, CapFactor_JUNSEP])

# Create the iStudio input
Index = pd.read_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Developed\Output\AllCap_Index_Security_Level_0.99_0.9925_20241119_NoShadow.csv", parse_dates=["Date"]).query("Date >= '2019-03-18'")

# Filter for needed columns
Frame = Index[["Internal_Number", "SEDOL", "ISIN", "Date"]]

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
from datetime import datetime
current_datetime = datetime.today().strftime('%Y%m%d')

# Store the .CSV with version and timestamp
Frame.to_csv(
        os.path.join(
            r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Developed\Output", 
            current_datetime + "_ALLCAP_ETF.csv"
        ), 
        index=False, 
        lineterminator="\n", 
        sep=";"
    )