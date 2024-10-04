import polars as pl

Investable_Universe = pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Output\Code_Output\Investable_Universe_FOR_Applied.csv").with_columns(
                                pl.col("Date").cast(pl.Date)
).filter(pl.col("Segment") == "Emerging")

Standard_Index = pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Output\Code_Output\Standard_Index_FOR_Applied.csv").with_columns(
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

# Add Country
Columns = ["Date", "Index_Symbol", "Index_Name", "Internal_Number", "ISIN", "SEDOL", "RIC", "Instrument_Name", 
           "Country", "Currency", "Exchange", "ICB", "Free_Float", "Capfactor", "Shares", "Close_unadjusted_local", "FX_local_to_Index_Currency"]

Emerging = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Universe\SWEACGV.parquet", columns=Columns).with_columns([
                            pl.col("Free_Float").cast(pl.Float64),
                            pl.col("Capfactor").cast(pl.Float64),
                            pl.col("Shares").cast(pl.Float64),
                            pl.col("Close_unadjusted_local").cast(pl.Float64),
                            pl.col("FX_local_to_Index_Currency").cast(pl.Float64),
                            pl.col("Date").cast(pl.Date)
                            ]).select(pl.col(["Date", "Internal_Number", "Country"]))

Investable_Universe = Investable_Universe.join(Emerging, on=["Date", "Internal_Number", "Country"], how="left")

print(Investable_Universe)