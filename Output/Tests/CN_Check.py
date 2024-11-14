import polars as pl
import datetime

# Load CSVs with Date column casting
AllCap_NoShadow = pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Output\Tests\AllCap_Index_Security_Level_0.993_0.9955_20241114_NoShadow.csv").with_columns(pl.col("Date").cast(pl.Date))
AllCap_NoShadow_NoChina = pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Output\Tests\AllCap_Index_Security_Level_0.993_0.9955_20241114_NoShadow_NoChinaASmall.csv").with_columns(pl.col("Date").cast(pl.Date))
Standard = pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Output\Tests\Standard_Index_Security_Level_CNTarget_0.9_20241114.csv").with_columns(pl.col("Date").cast(pl.Date))
Standard_Shadow = pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Output\Tests\Standard_Index_Security_Level_Shadows_CNTarget_0.9_20241114.csv").with_columns(pl.col("Date").cast(pl.Date))
Small = pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Output\Tests\Small_Index_Security_Level_CNTarget_0.9_0.993_0.9955_20241114.csv").with_columns(pl.col("Date").cast(pl.Date))

# Function to create recap count and filter for Country "CN"
def create_recap(df, frame_name, country="CN"):
    return (
        df
        .group_by(["Country", "Date"])
        .agg(pl.col("Internal_Number").count().alias("Sum_Components"))
        .filter(pl.col("Country") == country)
        .with_columns(pl.lit(frame_name).alias("Frame"))  # Add frame name column
        .sort("Date")
    )

# Create filtered recaps for each frame
Recap_AllCap_NoShadow = create_recap(AllCap_NoShadow, "AllCap_NoShadow")
Recap_AllCap_NoShadow_NoChina = create_recap(AllCap_NoShadow_NoChina, "AllCap_NoShadow_NoChina")
Recap_Standard = create_recap(Standard, "Standard")
Recap_Standard_Shadow = create_recap(Standard_Shadow, "Standard_Shadow")
Recap_Small = create_recap(Small, "Small")

# Combine all recaps into a single DataFrame
Recap_All = pl.concat([
    Recap_AllCap_NoShadow,
    Recap_AllCap_NoShadow_NoChina,
    Recap_Standard,
    Recap_Standard_Shadow,
    Recap_Small
])

# Pivot the combined DataFrame to have Frame names as columns, Dates as rows, and Sum_Components as values
Recap_Pivoted = (
    Recap_All
    .pivot(
        index="Date",          # Set Date as the row index
        columns="Frame",        # Use Frame names as columns
        values="Sum_Components" # Fill values with Sum_Components
    )
    .sort("Date")              # Sort by Date for easier readability
)

print(Recap_Pivoted.filter(pl.col("Date")>=datetime.date(2019,3,18)))
Recap_Pivoted.filter(pl.col("Date")>=datetime.date(2019,3,18)).write_clipboard()
