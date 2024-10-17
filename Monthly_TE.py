import polars as pl
import numpy as np

# Load data and parse the 'Date' column
Time_Series = pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\MSCI EM Small Cap V130 Cutoff TimeSeries 15644.csv", separator=";").with_columns(
    pl.col("Date").str.strptime(pl.Date, "%m/%d/%Y")
)

# Group by month and calculate mean returns for MSCI and V130_MCAP_Cutoff
monthly_returns_MSCI = Time_Series.with_columns([
    pl.col("Date").dt.year().alias("year"),
    pl.col("Date").dt.month().alias("month")
]).group_by(["year", "month"]).agg([
    pl.col("MSCI").last()
]).sort(by=["year", "month"], descending=False)

monthly_returns_SAMCO = Time_Series.with_columns([
    pl.col("Date").dt.year().alias("year"),
    pl.col("Date").dt.month().alias("month")
]).group_by(["year", "month"]).agg([
    pl.col("V130_MCAP_Cutoff").last()
]).sort(by=["year", "month"], descending=False)

# Join MSCI and SAMCO monthly returns
monthly_returns = monthly_returns_MSCI.join(monthly_returns_SAMCO, on=["year", "month"], how="left")

# Calculate monthly returns for MSCI and V130_MCAP_Cutoff
monthly_returns = monthly_returns.with_columns(
    (pl.col("MSCI") / pl.col("MSCI").shift(1) - 1).alias("Returns_MSCI"),
    (pl.col("V130_MCAP_Cutoff") / pl.col("V130_MCAP_Cutoff").shift(1) - 1).alias("Returns_V130_MCAP_Cutoff"),
).with_columns(
    (pl.col("Returns_MSCI") - pl.col("Returns_V130_MCAP_Cutoff")).alias("Difference_V130_MCAP_Cutoff")
)

# Function to calculate tracking error for a given period (in months)
def calculate_tracking_error(data, months):
    return data.tail(months).select(pl.col("Difference_V130_MCAP_Cutoff").std()).to_numpy()[0][0] * np.sqrt(12)

# Calculate tracking errors for 1Y (12 months), 3Y (36 months), and the Max period
tracking_error_1Y = calculate_tracking_error(monthly_returns, 12)
tracking_error_3Y = calculate_tracking_error(monthly_returns, 36)
tracking_error_max = calculate_tracking_error(monthly_returns, len(monthly_returns))

# Output the results
print(f"1Y Tracking Error: {tracking_error_1Y}")
print(f"3Y Tracking Error: {tracking_error_3Y}")
print(f"Max Period Tracking Error: {tracking_error_max}")
