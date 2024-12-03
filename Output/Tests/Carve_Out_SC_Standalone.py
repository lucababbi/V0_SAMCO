import polars as pl
import datetime
import os

Standard_Index = pl.read_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Output\Tests\90.993995 @ 20241126\Standard_Index_Security_Level_Shadows_CNTarget_0.9_20241121.csv").with_columns(
    pl.col("Date").cast(pl.Date)
)

All_Cap = pl.read_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Output\Tests\90.993995 @ 20241126\AllCap_Index_Security_Level_0.993_0.9955_20241121_NoShadow_NoChinaASmall.csv").with_columns(
    pl.col("Date").cast(pl.Date)
)

Small_Index = All_Cap.join(Standard_Index.select(pl.col(["Date", "Internal_Number"])), on=["Date", "Internal_Number"], how="anti")

# Get current date formatted as YYYYMMDD_HHMMSS
from datetime import datetime
current_datetime = datetime.today().strftime('%Y%m%d')

Small_Index.write_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Output\Tests\90.993995 @ 20241126\Small_Index_Security_Level_CNTarget_0.9_0.993_0.9955_20241121_Extended.csv")