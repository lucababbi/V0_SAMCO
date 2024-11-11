import polars as pl

Small_Index = pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Output\Tests\90-9939955\Small_Index_Security_Level_CNTarget_0.9_0.993_0.9955_20241105.csv").with_columns(
    pl.col("Date").cast(pl.Date)
).drop("Instrument_Name_right")

Small_Index = Small_Index.join(pl.read_parquet(
    r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Universe\STXWAGV_Review.parquet").with_columns(
        pl.col("Date").cast(pl.Date),
        pl.col("Mcap_Units_Index_Currency").cast(pl.Float64)
    ), on=["Date", "Internal_Number"], how="left")

Small_Index = Small_Index.with_columns(
    (pl.col("Mcap_Units_Index_Currency") / pl.col("Mcap_Units_Index_Currency").sum().over("Date")).alias("Weight")
)

Recap_Count = (
    Small_Index
    .group_by(["Country", "Date"])  # Group by Country and Date
    .agg(pl.col("Internal_Number").count().alias("Sum_Components"))  # Count "Count" column and alias it
    .sort("Date")  # Ensure sorting by Date for proper column ordering in the pivot
    .pivot(
        index="Country",  # Set Country as the row index
        on="Date",        # Create columns for each unique Date
        values="Sum_Components"  # Fill in values with Sum_Components
    )
)

Recap_Weight = (
    Small_Index
    .group_by(["Country", "Date"])  # Group by Country and Date
    .agg(pl.col("Weight").sum().alias("Weight_Components"))  # Count "Count" column and alias it
    .sort("Date")  # Ensure sorting by Date for proper column ordering in the pivot
    .pivot(
        index="Country",  # Set Country as the row index
        on="Date",        # Create columns for each unique Date
        values="Weight_Components"  # Fill in values with Sum_Components
    )
)

# Get current date formatted as YYYYMMDD_HHMMSS
from datetime import datetime
current_datetime = datetime.today().strftime('%Y%m%d')

Recap_Count.write_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Output\Tests\90-9939955\Recap_Count_Small_Index_" + current_datetime + ".csv")
Recap_Weight.write_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Output\Tests\90-9939955\Recap_Weight_Small_Index_" + current_datetime + ".csv")