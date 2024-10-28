import polars as pl
import datetime

Standard_Index = pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Output\Standard\Standard_Index_Security_Level_Shadows_0.85_True_20241028.csv").with_columns(
    pl.col("Date").cast(pl.Date)
)

All_Cap = pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Output\All_Country\AllCap_Index_Security_Level_0.995_ETF_Version_Coverage_Adjustment_True_20241028.csv").with_columns(
    pl.col("Date").cast(pl.Date)
)

Small_Index = All_Cap.join(Standard_Index.select(pl.col(["Date", "Internal_Number"])), on=["Date", "Internal_Number"], how="anti").filter(
    pl.col("Date") >= datetime.date(2019,3,18)
)

# Get current date formatted as YYYYMMDD_HHMMSS
from datetime import datetime
current_datetime = datetime.today().strftime('%Y%m%d')

Small_Index.write_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Output\Small\Small_Index_Security_Level_ETF_Version_Coverage_Adjustment" + current_datetime + ".csv")