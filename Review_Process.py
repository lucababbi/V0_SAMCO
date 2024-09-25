import polars as pl
import pandas as pd

##################################
##################################
# Read Developed/Emerging Universe
##################################
##################################

# Select columns to read from the Parquets
Columns = ["Date", "Index_Symbol", "Index_Name", "Internal_Number", "ISIN", "SEDOL", "RIC", "Instrument_Name", 
           "Country", "Currency", "Exchange", "ICB", "Free_Float", "Capfactor", "Shares", "Close_unadjusted_local", "FX_local_to_Index_Currency"]

# Developed Universe
Developed = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Universe\SWDACGV.parquet", columns=Columns).with_columns([
                            pl.col("Free_Float").cast(pl.Float64),
                            pl.col("Capfactor").cast(pl.Float64),
                            pl.col("Shares").cast(pl.Float64),
                            pl.col("Close_unadjusted_local").cast(pl.Float64),
                            pl.col("FX_local_to_Index_Currency").cast(pl.Float64),
                            pl.col("Date").cast(pl.Date)
                            ])

# Emerging Universe
Emerging = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Universe\SWEACGV.parquet", columns=Columns).with_columns([
                            pl.col("Free_Float").cast(pl.Float64),
                            pl.col("Capfactor").cast(pl.Float64),
                            pl.col("Shares").cast(pl.Float64),
                            pl.col("Close_unadjusted_local").cast(pl.Float64),
                            pl.col("FX_local_to_Index_Currency").cast(pl.Float64),
                            pl.col("Date").cast(pl.Date)
                            ])

# Calculate Free/Full MCAP USD for Developed Universe
Developed = Developed.with_columns(
                                    (pl.col("Free_Float") * pl.col("Capfactor") * pl.col("Close_unadjusted_local") * pl.col("FX_local_to_Index_Currency") * pl.col("Shares"))
                                    .alias("Free_Float_MCAP_USD"),
                                    (pl.col("Close_unadjusted_local") * pl.col("FX_local_to_Index_Currency") * pl.col("Shares") / (pl.col("Capfactor") * pl.col("Free_Float")))
                                    .alias("Full_MCAP_USD")
                                  )

# Calculate Free/Full MCAP USD for Emerging Universe
Emerging = Emerging.with_columns(
                                    (pl.col("Free_Float") * pl.col("Capfactor") * pl.col("Close_unadjusted_local") * pl.col("FX_local_to_Index_Currency") * pl.col("Shares"))
                                    .alias("Free_Float_MCAP_USD"),
                                    (pl.col("Close_unadjusted_local") * pl.col("FX_local_to_Index_Currency") * pl.col("Shares") / (pl.col("Capfactor") * pl.col("Free_Float")))
                                    .alias("Full_MCAP_USD")
                                  )

# Creation of main GMSR Frame
GMSR_Frame = pl.DataFrame({
                            "Date": pl.Series(dtype=pl.Date),
                            "GMSR_Developed": pl.Series(dtype=pl.Float64),
                            "GMSR_Emerging": pl.Series(dtype=pl.Float64),
                            "GMSR_Emerging_Upper": pl.Series(dtype=pl.Float64),
                            "GMSR_Emerging_Lower": pl.Series(dtype=pl.Float64),
})

# Calculate GMSR for Developed/Emerging Universes
for date in Developed["Date"].unique():
    temp_Developed = Developed.filter(pl.col("Date") == date)

    temp_Developed = temp_Developed.sort(["Full_MCAP_USD"], descending=True)
    temp_Developed = temp_Developed.with_columns(
                                                    (pl.col("Free_Float_MCAP_USD") / pl.col("Free_Float_MCAP_USD").sum()).alias("Weight")
    )

    temp_Developed = temp_Developed.with_columns(
                                                    (pl.col("Weight").cum_sum()).alias("CumWeight")
    )

    New_Data = pl.DataFrame({
                                "Date": [date],
                                "GMSR_Developed": [temp_Developed.filter(pl.col("CumWeight") >= 0.85).head(1)["Full_MCAP_USD"].to_numpy()[0]],
                                "GMSR_Emerging": [temp_Developed.filter(pl.col("CumWeight") >= 0.85).head(1)["Full_MCAP_USD"].to_numpy()[0] / 2],
                                "GMSR_Emerging_Upper": [temp_Developed.filter(pl.col("CumWeight") >= 0.85).head(1)["Full_MCAP_USD"].to_numpy()[0] / 2 * 1.15],
                                "GMSR_Emerging_Lower": [temp_Developed.filter(pl.col("CumWeight") >= 0.85).head(1)["Full_MCAP_USD"].to_numpy()[0] / 2 * 0.50],
    })

    GMSR_Frame = GMSR_Frame.vstack(New_Data)

print(Emerging)