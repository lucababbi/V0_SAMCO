import polars as pl
from datetime import datetime
import math

# Read the Dates
dates = pl.read_csv(
    r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Dates\Review_Date-QUARTERLY.csv",
    separator=",",
    infer_schema=False
).with_columns(
    pl.col("Review").str.strptime(pl.Date, format="%m/%d/%Y")  # Parse date column
).select(pl.col("Review")).filter(
    pl.col("Review") >= datetime(2012, 6, 18)  # Filter dates from 2012-06-18 onward
)

# Time Series
TimeSeries = pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Metrics\SAMCO_EM_STD_Extended_Te.csv", separator=";", infer_schema=False)
TimeSeries = TimeSeries.with_columns(
            pl.col("Date").str.strptime(pl.Date, format="%m/%d/%Y"),
            pl.col("MSCIPriceUSD").cast(pl.Float64),
            pl.col("SAMCOPriceUSD").cast(pl.Float64)
)
# Calculate Returns
TimeSeries = TimeSeries.with_columns(
    ((pl.col("MSCIPriceUSD") / pl.col("MSCIPriceUSD").shift(1)) - 1).alias("ReturnsMSCI"),
    ((pl.col("SAMCOPriceUSD") / pl.col("SAMCOPriceUSD").shift(1)) - 1).alias("ReturnsSAMCO")
)

# Calculate the Spread
TimeSeries = TimeSeries.with_columns(
    (pl.col("ReturnsMSCI") - pl.col("ReturnsSAMCO")).alias("Spread")
)

# Get the Ranges
dates = dates.with_columns(
    pl.col("Review").dt.month().alias("Month"),
    pl.col("Review").shift(-1).dt.add_business_days(-1).alias("Review-T-1")
)

# Filter for March and September
MarSep = dates.filter((pl.col("Month") == 3) | (pl.col("Month") == 9))
# Filter for June and December
JunDec = dates.filter((pl.col("Month") == 6) | (pl.col("Month") == 12))

# Output Frame
TimeSeriesMarSep = pl.DataFrame(schema=TimeSeries.schema)
TimeSeriesJunDec = pl.DataFrame(schema=TimeSeries.schema)

# Loop through the rows and access 'Review' and 'Review-T-1' values
for row in MarSep.iter_rows():
    review_date = row[0]  # First column is 'Review'
    review_t_1_date = row[2]  # Third column is 'Review-T-1'

    if review_t_1_date is not None:
        # Get the ranges in the TimeSeries
        temp = TimeSeries.filter(
            (pl.col("Date") >= review_date) & (pl.col("Date") <= review_t_1_date)
        )
    else:
        # Get the ranges in the TimeSeries
        temp = TimeSeries.filter(
            (pl.col("Date") >= review_date))

    TimeSeriesMarSep = TimeSeriesMarSep.vstack(temp)

TrackingMarSep = TimeSeriesMarSep.select(pl.col("Spread")).std() * math.sqrt(252)

# Loop through the rows and access 'Review' and 'Review-T-1' values
for row in JunDec.iter_rows():
    review_date = row[0]  # First column is 'Review'
    review_t_1_date = row[2]  # Third column is 'Review-T-1'

    if review_t_1_date is not None:
        # Get the ranges in the TimeSeries
        temp = TimeSeries.filter(
            (pl.col("Date") >= review_date) & (pl.col("Date") <= review_t_1_date)
        )
    else:
        # Get the ranges in the TimeSeries
        temp = TimeSeries.filter(
            (pl.col("Date") >= review_date))

    TimeSeriesJunDec = TimeSeriesJunDec.vstack(temp)

TrackingJunDec = TimeSeriesJunDec.select(pl.col("Spread")).std() * math.sqrt(252)

# Output
TimeSeriesMarSep.with_columns(pl.lit("MarSep").alias("Group")).select(pl.col(["Date", "Spread", "Group"])).vstack(
    TimeSeriesJunDec.with_columns(pl.lit("JunDec").alias("Group")).select(pl.col(["Date", "Spread", "Group"]))
).write_clipboard()

print(TrackingJunDec)
print(TrackingMarSep)
