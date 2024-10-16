import polars as pl
import numpy as np

Time_Series = pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Time_Series.csv", separator=";").with_columns(
                pl.col("Date").str.strptime(pl.Date, "%m-%d-%Y")
)

# Group by month and calculate mean returns
monthly_returns_MSCI = Time_Series.with_columns([
    pl.col("Date").dt.year().alias("year"),
    pl.col("Date").dt.month().alias("month"),
    pl.col("Date").dt.day().alias("day")
]).group_by(["year", "month"]).agg([
    pl.col("MSCIPriceUSD").last()
]).sort(by=["year", "month"], descending=[False, False])

monthly_returns_SAMCO = Time_Series.with_columns([
    pl.col("Date").dt.year().alias("year"),
    pl.col("Date").dt.month().alias("month"),
    pl.col("Date").dt.day().alias("day")
]).group_by(["year", "month"]).agg([
    pl.col("SAMCO").last()
]).sort(by=["year", "month"], descending=[False, False])

monthly_returns_STOXX = Time_Series.with_columns([
    pl.col("Date").dt.year().alias("year"),
    pl.col("Date").dt.month().alias("month"),
    pl.col("Date").dt.day().alias("day")
]).group_by(["year", "month"]).agg([
    pl.col("STOXX").last()
]).sort(by=["year", "month"], descending=[False, False])

monthly_returns = monthly_returns_MSCI.join(monthly_returns_SAMCO, on=["year", "month"], how="left").join(monthly_returns_STOXX, on=["year", "month"], how="left")

monthly_returns = monthly_returns.with_columns(
    (pl.col("MSCIPriceUSD") / pl.col("MSCIPriceUSD").shift(1) - 1).alias("Returns_MSCIPriceUSD"),
    (pl.col("SAMCO") / pl.col("SAMCO").shift(1) - 1).alias("Returns_SAMCO"),
    (pl.col("STOXX") / pl.col("STOXX").shift(1) - 1).alias("Returns_STOXX"),
).with_columns(
    (pl.col("Returns_MSCIPriceUSD") - pl.col("Returns_SAMCO")).alias("Difference_SAMCO"),
    (pl.col("Returns_MSCIPriceUSD") - pl.col("Returns_STOXX")).alias("Difference_STOXX")
)

tracking_error = monthly_returns.select(pl.col("Difference_STOXX").std()).to_numpy()[0][0] * np.sqrt(12)
print(tracking_error)