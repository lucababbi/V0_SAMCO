import polars as pl

Investable_Universe = pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Output\Code_Output\Investable_Universe.csv").with_columns(
                                pl.col("Date").cast(pl.Date)
).filter(pl.col("Segment") == "Emerging")

Standard_Index = pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Output\Code_Output\Standard_Index.csv").with_columns(
                                pl.col("Date").cast(pl.Date)
).filter(pl.col("Shadow_Company").is_null()).drop("Size").with_columns(
                                pl.lit("Standard").alias("Size")
)

SWACALLCAP = pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Output\Code_Output\STXWAGV_Close_Historical.csv", separator=";").select(pl.col(["Date",
                        "Internal_Number", "Instrument_Name", "Mcap_Units_Index_Currency"])).with_columns(
                            pl.col("Date").cast(pl.Date)
                        )

Investable_Universe = Investable_Universe.join(Standard_Index.select(pl.col(["Date", "Internal_Number", "Country", "Size"])), on=["Date", "Internal_Number"], how="left").join(
    SWACALLCAP, on=["Date", "Internal_Number"], how="left"
).with_columns(
    pl.col("Size").fill_null("Small")
).filter(pl.col("Size") == "Small")

print(Investable_Universe)