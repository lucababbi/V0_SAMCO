import polars as pl
import pandas as pd
from datetime import date
import matplotlib.pyplot as plt
from pandasql import sqldf
import datetime
import glob
import os
import numpy as np
import math
import time

start_time = time.time()

##################################
###########Parameters#############
##################################
Starting_Date = date(2012, 6, 18)
Upper_Limit = 1.15
Lower_Limit = 0.50

Percentage = 0.995
Left_Limit = Percentage - 0.005
Right_Limit = 1.00

Threshold_NEW = 0.15
Threshold_OLD = 0.05

FOR_FF_Screen = 0.15
Screen_TOR = True

# MSCI GMSR Mar_2012
GMSR_Upper_Buffer = float(os.getenv("GMSR_Upper_Buffer"))
GMSR_Lower_Buffer = float(os.getenv("GMSR_Lower_Buffer"))
CN_Target_Percentage = float(os.getenv("CN_Target_Percentage"))
current_datetime = os.getenv("current_datetime")
# GMSR_Upper_Buffer = 0.995
# GMSR_Lower_Buffer = 0.9975
GMSR_MSCI = np.float64(330 * 1_000_000)

# Country Adjustment based on MSCI Mar_2012
MSCI_Curve_Adjustment = pl.DataFrame({"Country": ["AU", "BG", "BR", "CA", "CL", "CN", "CO", "EG", "HK", "HU", "ID", "IL", "IN", "JP", "KR", "MA", "MX", 
                                    "MY", "PH", "PL", "RU", "SG", "TH", "TR", "TW", "US", "ZA", "DK", "IE", "CH", "GB", "NL", "SE", "AT", 
                                    "GR", "NO", "FR", "ES", "DE", "FI", "IT", "BE", "PT", "CZ", "GR", "NZ"],
                                    "Coverage": [0.860, 0.860, 0.950, 0.846, 0.901, 0.901, 0.990, 0.860, 0.875, 0.975, 0.821, 0.810, 0.869, 0.841, 
                                    0.878, 0.910, 0.950, 0.830, 0.815, 0.890, 0.995, 0.837, 0.825, 0.815, 0.810, 0.862, 0.875, 0.900, 
                                    0.900, 0.900, 0.900, 0.900, 0.900, 0.900, 0.900, 0.900, 0.900, 0.900, 0.900, 0.900, 0.900, 0.900, 
                                    0.900, 0.85, 0.85, 0.85]})

# MSCI Equity_Minum_Size
MSCI_Equity_Minimum_Size = (130 * 1_000_000)

# Index Creation Country Coverage Adjustment for TMI Universe
Coverage_Adjustment = True

# Excel Setter
Excel_Recap = False
Excel_Recap_Rebalancing = False

Country_Plotting = "BR"
Output_File = rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Output\TopPercentage_Report_Rebalancing_{Country_Plotting}.xlsx"

# ETFs SPDR-iShares
ETF = pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Universe\ETFs_STANDARD-SMALL.csv", separator=";")

##################################
#########Index Continuity#########
##################################
def Index_Continuity(TopPercentage_Securities, TopPercentage, Segment: pl.Utf8, temp_Emerging, country, Standard_Index):

    # Check if there are at least 3 NON-NEW Companies
    if (Segment == "Emerging") & (len(TopPercentage_Securities.filter(pl.col("Shadow_Company") == False)) < 3):

        # Keep only Non-Shadow Securities
        TopPercentage_Securities = TopPercentage_Securities.filter(pl.col("Shadow_Company") == False) 

        # Take all Securities passing the Screens
        temp_Emerging_Country = temp_Emerging.filter(pl.col("Country")==country).sort("Free_Float_MCAP_USD_Cutoff", descending=True)

        Previous_Date = Standard_Index.filter(pl.col("Country") == country) \
            .unique(subset=["Date"]) \
            .select(pl.col("Date").max()) \
            .to_numpy()[0, 0] 
        
        if isinstance(Previous_Date, np.datetime64):
            Previous_Date = Previous_Date.astype('M8[D]').astype(datetime.date) 

        # Securities to pump 1.5 Free_Float_MCAP_USD_Cutoff (excluding the Securities who are already )
        temp_Emerging_Current = temp_Emerging_Country.filter(
                    pl.col("Internal_Number").is_in(
                        Standard_Index.filter(
                            (pl.col("Country") == country) & (pl.col("Date") == Previous_Date)
                        ).select(pl.col("Internal_Number"))
                    )
                ).with_columns(
                    (pl.col("Free_Float_MCAP_USD_Cutoff") * 1.5).alias("Free_Float_MCAP_USD_Cutoff")
                ).filter(~pl.col("Internal_Number").is_in(TopPercentage_Securities.select(pl.col("Internal_Number"))))

        # All Securities not included in the Standard Index for the Previous Date       
        temp_Emerging_Non_Current = temp_Emerging_Country.filter(
                    (~pl.col("Internal_Number").is_in(temp_Emerging_Current.select(pl.col("Internal_Number")))) &
                    (~pl.col("Internal_Number").is_in(TopPercentage_Securities.select(pl.col("Internal_Number"))))
                )

        # Stack the Frames
        temp_Emerging_Country = temp_Emerging_Current.vstack(temp_Emerging_Non_Current).with_columns(
            pl.lit("All_Cap").alias("Size"),
            pl.lit(False).alias("Shadow_Company")
        )

        if len(temp_Emerging_Country) >= (3 - len(TopPercentage_Securities)):

            # Keep the Securities needed to get the minimum number of Securities
            temp_Emerging_Country = temp_Emerging_Country.sort("Free_Float_MCAP_USD_Cutoff", descending = True).head(3 - len(TopPercentage_Securities))

            TopPercentage_Securities = TopPercentage_Securities.vstack(temp_Emerging_Country.select(TopPercentage_Securities.columns))

            # Fix the columns
            TopPercentage_Securities = TopPercentage_Securities.select(pl.col(["Date", "Internal_Number", "Instrument_Name", "ENTITY_QID", "Country"])).with_columns(
                pl.lit("All_Cap").alias("Size"),
                pl.lit(False).alias("Shadow_Company")
            )

            TopPercentage = TopPercentage_Securities.group_by(["Date", "ENTITY_QID"]).agg([
                pl.col("Country").first().alias("Country")
            ]).with_columns([
                pl.lit("All_Cap").alias("Size"),
                pl.lit("Maintenance").alias("Case")
            ])

        else:
            TopPercentage_Securities = TopPercentage_Securities.head(0)


    elif (Segment == "Developed") & (len(TopPercentage_Securities) < 5):
        print("Here")

    return TopPercentage_Securities, TopPercentage

##################################
#########FOR Screening############
##################################
def FOR_Sreening(Frame: pl.DataFrame, Full_Frame: pl.DataFrame, Pivot_TOR, Standard_Index, Small_Index, date, Segment: pl.Utf8) -> pl.DataFrame:

    # List of Unique Dates
    Dates_List = Pivot_TOR.index.to_list()

    Screened_Frame = pl.DataFrame()

    # Loop for all the Countries
    for country in Frame.select(["Country"]).unique().sort("Country").to_series():

        # Calculate FOR_FF
        temp_Frame = Frame.filter(pl.col("Country") == country).with_columns(
                                (pl.col("Free_Float") * pl.col("Capfactor")).alias("FOR_FF"))
        
        # List of the current traded Securities
        Full_Frame_Country = Full_Frame.filter((pl.col("Date") == date) & (pl.col("Country") == country)).select(pl.col(
            ["Date", "Internal_Number", "Instrument_Name", "ENTITY_QID", "Country", "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff"])).group_by(
                                                    ["Date", "ENTITY_QID"]).agg([
                                                        pl.col("Country").first().alias("Country"),
                                                        pl.col("Internal_Number").first().alias("Internal_Number"),
                                                        pl.col("Instrument_Name").first().alias("Instrument_Name"),
                                                        pl.col("Free_Float_MCAP_USD_Cutoff").sum().alias("Free_Float_MCAP_USD_Cutoff_Company"),
                                                        pl.col("Full_MCAP_USD_Cutoff").sum().alias("Full_MCAP_USD_Cutoff_Company")
                                                    ]).sort(["Date", "Full_MCAP_USD_Cutoff_Company"], descending = True)
        
        # Filter for those Securities failing the FOR_Screen
        Failing_Securities = temp_Frame.filter(pl.col("FOR_FF") < FOR_FF_Screen)

        if len(Failing_Securities) > 0:
            IDX_Current = Dates_List.index(date.strftime("%Y-%m-%d"))
            Previous_Date = datetime.datetime.strptime(Dates_List[max(0, IDX_Current - 1)], "%Y-%m-%d").date()

            # Take the full Index based on the previous date/country that are still traded (Company level)
            Investable_Index = Standard_Index.filter((pl.col("Country") == country) & (pl.col("Date") == Previous_Date)).vstack(Small_Index.filter(
                                (pl.col("Country") == country) & (pl.col("Date") == Previous_Date))).select(pl.col(["Date", "Internal_Number", "ENTITY_QID", "Country", "Shadow_Company"])
                                ).select(pl.col(["Date", "Internal_Number", "ENTITY_QID", "Country"])).group_by(
                                                    ["Date", "ENTITY_QID"]).agg([
                                                        pl.col("Country").first().alias("Country"),
                                                        pl.col("Internal_Number").first().alias("Internal_Number")
                                                    ]).join(
                                    Full_Frame_Country.select(pl.col(["ENTITY_QID", "Free_Float_MCAP_USD_Cutoff_Company", "Full_MCAP_USD_Cutoff_Company"])), 
                                    on=["ENTITY_QID"], how="left").filter((pl.col("Free_Float_MCAP_USD_Cutoff_Company") > 0) & (pl.col("Full_MCAP_USD_Cutoff_Company") > 0)
                                    ).sort("Full_MCAP_USD_Cutoff_Company", descending=True)                          
            
            # Case where Investable_Index is NULL
            if len(Investable_Index) > 0:

                # Count the Companies that made it in the previous Basket
                Previous_Count = len(Standard_Index.filter((pl.col("Country") == country) & (pl.col("Date") == Previous_Date))) - 1

                # Ensure Previous_Count is within bounds
                if Previous_Count >= Investable_Index.height:
                    # Take the last row if Previous_Count exceeds the DataFrame length
                    Previous_Count = Investable_Index.height - 1

                # Find the Country_Cutoff ["Full_MCAP_USD_Cutoff_Company"]
                Country_Cutoff = Investable_Index.sort("Full_MCAP_USD_Cutoff_Company", descending=True).row(Previous_Count)[Investable_Index.columns.index("Full_MCAP_USD_Cutoff_Company")] / 2 * 1.8

                # Check if the Failing_Securities have a Free_Float_MCAP_USD_Cutoff >= than the Country_Cutoff
                Failing_Securities = Failing_Securities.with_columns(
                                            (pl.col("Free_Float_MCAP_USD_Cutoff") >= Country_Cutoff).alias("Screen")
                )

                # Filter out the Securities not passing the Screen
                temp_Frame = temp_Frame.filter(
                        ~pl.col("Internal_Number").is_in(
                            Failing_Securities.filter(pl.col("Screen") == False).select("Internal_Number").to_series()
                        )
                    )

            else: # If Country is newly added

                # Check if the Failing_Securities have a Free_Float_MCAP_USD_Cutoff >= than the Country_Cutoff
                Failing_Securities = Failing_Securities.with_columns(
                                            (pl.col("Free_Float_MCAP_USD_Cutoff") >= FOR_FF_Screen).alias("Screen")
                )

                # Filter out the Securities not passing the Screen
                temp_Frame = temp_Frame.filter(
                        ~pl.col("Internal_Number").is_in(
                            Failing_Securities.filter(pl.col("Screen") == False).select("Internal_Number").to_series()
                        )
                    )

        # Stack the resulting Frame
        Screened_Frame = Screened_Frame.vstack(temp_Frame)
            
    return Screened_Frame

##################################
#######China A Securities#########
##################################
def China_A_Securities(Frame: pl.DataFrame) -> pl.DataFrame:

    Results = pl.DataFrame({"Date": pl.Series(dtype=pl.Date),
                            "Internal_Number": pl.Series(dtype=pl.Utf8),
                            "Capfactor": pl.Series(dtype=pl.Float64),
                            "Instrument_Name": pl.Series(dtype=pl.Utf8),
                            "Country": pl.Series(dtype=pl.Utf8),
                            "Capfactor_CN": pl.Series(dtype=pl.Float64),
                            "Adjustment": pl.Series(dtype=pl.Float64)
                            })

    for Date in Frame.select(["Date"]).unique().sort("Date").to_series():
        # Filter for the given date
        temp_Frame = Frame.filter(pl.col("Date") == Date)

        if  datetime.date(2019,3,18) <= Date < datetime.date(2019,9,23):
            Chinese_Securities = temp_Frame.filter(
                                (
                                    (pl.col("Country") == "CN") & (pl.col("Capfactor") < 1) &
                                    (
                                        pl.col("Instrument_Name").str.contains("'A'") |
                                        pl.col("Instrument_Name").str.contains("(CCS)")
                                    ))
                            ).select(pl.col(["Date", "Internal_Number", "Capfactor", "Instrument_Name", "Country"])).with_columns(
                                        (pl.col("Capfactor") / 0.1).alias("Capfactor_CN"),
                                        (pl.lit(0.10).alias("Adjustment"))
                                    )

        elif Date < datetime.date(2022,9,19):
            Chinese_Securities = temp_Frame.filter(
                                (
                                    (pl.col("Country") == "CN") & (pl.col("Capfactor") < 1) &
                                    (
                                        pl.col("Instrument_Name").str.contains("'A'") |
                                        pl.col("Instrument_Name").str.contains("(CCS)")
                                    ))
                            ).select(pl.col(["Date", "Internal_Number", "Capfactor", "Instrument_Name", "Country"])).with_columns(
                                        (pl.col("Capfactor") / 0.2).alias("Capfactor_CN"),
                                        (pl.lit(0.20).alias("Adjustment"))
                                    )
        else:
            Chinese_Securities = temp_Frame.filter(
                                ((pl.col("Exchange") == 'Stock Exchange of Hong Kong - SSE Securities') |
                                (pl.col("Exchange") == 'Stock Exchange of Hong Kong - SZSE Securities')) & 
                                (pl.col("Country") == "CN")
                            ).select(
                                ["Date", "Internal_Number", "Capfactor", "Instrument_Name", "Country"]
                            ).with_columns(
                                (pl.col("Capfactor") / 0.2).alias("Capfactor_CN"),
                                (pl.lit(0.20).alias("Adjustment"))
                            )
        
        Results = Results.vstack(Chinese_Securities)
            
    return pl.DataFrame(Results)

##################################
#######Quarterly Turnover#########
##################################
def Turnover_Check(Frame: pl.DataFrame, Pivot_TOR: pl.DataFrame, Threshold_NEW, Threshold_OLD, date, Starting_Date) -> pl.DataFrame:

    # List of Unique Dates
    Dates_List = Pivot_TOR.index.to_list()

    # Output
    Results = []
    Status = {}

    for Row in Frame.select(pl.col("Date")).unique().iter_rows(named=True):
        Date = Row["Date"].strftime("%Y-%m-%d")
        IDX_Current = Dates_List.index(date.strftime("%Y-%m-%d"))
        Previous_Date = datetime.datetime.strptime(Dates_List[max(0, IDX_Current - 1)], "%Y-%m-%d").date()

        # Find the index of Date in Pivot_TOR
        try:
            IDX_Current = Dates_List.index(Date)
        except ValueError:
            # If the date is not found, skip this row
            continue

        # Get the previous three Dates and Current Date
        Relevant_Dates = pl.Series(Dates_List[max(0, IDX_Current - 3): IDX_Current + 1])

        # Convert Relevant_Dates to DataFrame for joining
        Relevant_Dates_df = pl.DataFrame({"Date": Relevant_Dates}).with_columns(pl.col("Date").cast(pl.Date))

        # Keep only the needed Dates
        Relevant_TOR = pl.DataFrame(Turnover).with_columns(pl.col("Date").cast(pl.Date)).join(Relevant_Dates_df, on="Date", how="right")

        # Join the previous four quarters
        Frame = (
                    Frame.join(Relevant_TOR, on="Internal_Number", how="left")
                    .drop("Date")  # Drop the original Date column from Frame
                    .rename({"Date_right": "Date"})  # Rename Date from Relevant_TOR to Date
                ).filter(~pl.col("Date").is_null())

        # Pivot the Frame
        Frame = Frame.pivot(values="Turnover_Ratio",
                            index="Internal_Number",
                            on="Date"
                        )
        
        # Determine the Threshold for each Internal_Number
        if date == Starting_Date:
            Frame = Frame.with_columns(pl.lit(Threshold_NEW).alias("Threshold"))

        else:

            Frame = Frame.with_columns(
                                        pl.when(
                                            pl.col("Internal_Number").is_in(
                                                Screened_Securities.filter(pl.col("Date") == Previous_Date).select(pl.col("Internal_Number"))
                                            )
                                        )
                                        .then(pl.lit(Threshold_NEW))
                                        .otherwise(pl.lit(Threshold_OLD))
                                        .alias("Threshold")
                                    )
            
        # Determine Columns Date
        date_columns = [col for col in Frame.columns if col not in ["Threshold", "Internal_Number"]]
        sorted_date_columns = sorted(date_columns)  # Ensure columns are in chronological order

        # Identify the most recent date column
        most_recent_date_col = sorted_date_columns[-1]

        # Add a column to check if all Columns are filled
        Frame = Frame.with_columns(
            pl.all_horizontal([pl.col(col).is_not_null() for col in date_columns]).alias("All_Dates_Available")
        )

        # Step 2: For rows where All_Dates_Available is False, fill missing date columns with the value from most_recent_date_col
        Frame = Frame.with_columns(
            [
                pl.when(pl.col("All_Dates_Available") == False)
                .then(pl.col(most_recent_date_col))
                .otherwise(pl.col(col))
                .alias(col)
                for col in date_columns if col != most_recent_date_col  # Apply this to all date columns except the most recent one
            ]
        )
        
        # Screened Frame
        Results = (Frame.with_columns(
            pl.min_horizontal(date_columns).alias("Min_Date_Value")
        )).with_columns(
            (pl.col("Min_Date_Value") >= pl.col("Threshold")).alias("Status_TOR")
        )

    # Return results as Polars
    return Results.select(pl.col(["Internal_Number", "Status_TOR"]))

##################################
#######Equity Minimum Size########
##################################
def Equity_Minimum_Size(df: pl.DataFrame, Pivot_TOR, EMS_Frame, date, Segment: pl.Utf8) -> pl.DataFrame:
    # List to hold results
    results = []

    # List of Unique Dates
    Dates_List = Pivot_TOR.index.to_list()

    try:
        IDX_Current = Dates_List.index(date.strftime("%Y-%m-%d"))
        Previous_Date = datetime.datetime.strptime(Dates_List[max(0, IDX_Current - 1)], "%Y-%m-%d").date()
        previous_rank = EMS_Frame.filter((pl.col("Date") == Previous_Date)).select(pl.col("Rank")).to_numpy()[0][0]
    except:
        previous_rank = None
    final_df = pl.DataFrame()

    # Create a copy of DF at Security_Level
    Security_Level_DF = df

    # Aggregate Securities by ENTITY_QID
    df = df.select(pl.col(["Date", "Internal_Number", "Instrument_Name", "ENTITY_QID", "Country", "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff"])).group_by(
                                                    ["Date", "ENTITY_QID"]).agg([
                                                        pl.col("Country").first().alias("Country"),
                                                        pl.col("Internal_Number").first().alias("Internal_Number"),
                                                        pl.col("Instrument_Name").first().alias("Instrument_Name"),
                                                        pl.col("Free_Float_MCAP_USD_Cutoff").sum().alias("Free_Float_MCAP_USD_Cutoff_Company"),
                                                        pl.col("Full_MCAP_USD_Cutoff").sum().alias("Full_MCAP_USD_Cutoff_Company")
                                                    ]).sort(["Date", "Full_MCAP_USD_Cutoff_Company"], descending=True)
    
    # Calculate cumulative sums and coverage
    df_date = df.with_columns([
        pl.col("Free_Float_MCAP_USD_Cutoff_Company").cum_sum().alias("Cumulative_Free_Float_MCAP_USD_Cutoff_Company"),
        (pl.col("Free_Float_MCAP_USD_Cutoff_Company").cum_sum() / pl.col("Free_Float_MCAP_USD_Cutoff_Company").sum()).alias("Cumulative_Coverage_Cutoff")
    ])

    if Segment == "Developed":

        # Add the MSCI Equity_Minimum_Size for JUN_2012
        if date == Starting_Date:
            
            equity_universe_min_size = float(MSCI_Equity_Minimum_Size)
            previous_rank = df_date.filter(pl.col("Full_MCAP_USD_Cutoff_Company") >= equity_universe_min_size).height

            df_date1 = df_date.filter(pl.col("Full_MCAP_USD_Cutoff_Company") >= equity_universe_min_size).with_columns([
                pl.lit(equity_universe_min_size).alias("EUMSR"),
                pl.lit(previous_rank).alias("EUMSR_Rank")])

        else:
    
            total_market_cap = df_date.select(pl.col("Free_Float_MCAP_USD_Cutoff_Company").sum()).to_numpy()[0][0]
            
            if previous_rank is None:
                # Initial calculation
                min_size_company = df_date.filter(pl.col("Cumulative_Coverage_Cutoff") >= 0.99).select("Full_MCAP_USD_Cutoff_Company").head(1)
                equity_universe_min_size = min_size_company[0, 0]
                previous_rank = df_date.filter(pl.col("Full_MCAP_USD_Cutoff_Company") >= equity_universe_min_size).height

                df_date1 = df_date.filter(pl.col("Full_MCAP_USD_Cutoff_Company") >= equity_universe_min_size).with_columns([
                    pl.lit(equity_universe_min_size).alias("EUMSR"),
                    pl.lit(previous_rank).alias("EUMSR_Rank")
                ])
            else:
                # Ensure previous_rank - 1 is within the bounds
                if previous_rank - 1 < len(df_date):
                    previous_row = df_date.row(previous_rank - 1)
                    previous_coverage = previous_row[df_date.columns.index("Cumulative_Free_Float_MCAP_USD_Cutoff_Company")] / total_market_cap
                    
                    if 0.998 <= previous_coverage <= 1:
                        equity_universe_min_size = previous_row[df_date.columns.index("Full_MCAP_USD_Cutoff_Company")]
                        df_date1 = df_date.filter(pl.col("Full_MCAP_USD_Cutoff_Company") >= equity_universe_min_size).with_columns([
                            pl.lit(equity_universe_min_size).alias("EUMSR"),
                            pl.lit(previous_rank).alias("EUMSR_Rank")
                        ])
                    elif previous_coverage < 0.998:
                        min_size_company = df_date.filter(pl.col("Cumulative_Coverage_Cutoff") >= 0.998).select("Full_MCAP_USD_Cutoff_Company").head(1)
                        equity_universe_min_size = min_size_company[0, 0]
                        previous_rank = df_date.filter(pl.col("Full_MCAP_USD_Cutoff_Company") >= equity_universe_min_size).height

                        df_date1 = df_date.filter(pl.col("Full_MCAP_USD_Cutoff_Company") >= equity_universe_min_size).with_columns([
                            pl.lit(equity_universe_min_size).alias("EUMSR"),
                            pl.lit(previous_rank).alias("EUMSR_Rank")
                        ])
                    else:
                        min_size_company = df_date.filter(pl.col("Cumulative_Coverage_Cutoff") >= 1).select("Full_MCAP_USD_Cutoff_Company").head(1)
                        equity_universe_min_size = min_size_company[0, 0]
                        previous_rank = df_date.filter(pl.col("Full_MCAP_USD_Cutoff_Company") >= equity_universe_min_size).height

                        df_date1 = df_date.filter(pl.col("Full_MCAP_USD_Cutoff_Company") >= equity_universe_min_size).with_columns([
                            pl.lit(equity_universe_min_size).alias("EUMSR"),
                            pl.lit(previous_rank).alias("EUMSR_Rank")
                        ])

                else:
                    min_size_company = df_date.filter(pl.col("Cumulative_Coverage_Cutoff") >= 0.9999).select("Full_MCAP_USD_Cutoff_Company").head(1)
                    equity_universe_min_size = min_size_company[0, 0]
                    previous_rank = df_date.filter(pl.col("Full_MCAP_USD_Cutoff_Company") >= equity_universe_min_size).height

                    df_date1 = df_date.filter(pl.col("Full_MCAP_USD_Cutoff_Company") >= equity_universe_min_size).with_columns([
                            pl.lit(equity_universe_min_size).alias("EUMSR"),
                            pl.lit(previous_rank).alias("EUMSR_Rank")
                        ])
        
        # Stack the information to the frame
        EMS_Frame = EMS_Frame.vstack(pl.DataFrame({
                                    "Date": [date],
                                    "Segment": [Segment],
                                    "EMS": [equity_universe_min_size],
                                    "Rank": [previous_rank],
                                    "FreeFloatMCAP_Minimum_Size": [equity_universe_min_size / 2]
        }))
        final_df = pl.concat([final_df, df_date1])

        # Keep only Securities/Company that passed the first screen
        Security_Level_DF = Security_Level_DF.filter(pl.col("ENTITY_QID").is_in(final_df.select(pl.col("ENTITY_QID"))))
        
    elif Segment == "Emerging":

        equity_universe_min_size = EMS_Frame.filter(pl.col("Date") == date).select(pl.col("EMS")).to_numpy()[0][0]
        final_df = df.filter(pl.col("Full_MCAP_USD_Cutoff_Company") >= equity_universe_min_size)

        # Keep only Securities/Company that passed the first screen
        Security_Level_DF = Security_Level_DF.filter(pl.col("ENTITY_QID").is_in(final_df.select(pl.col("ENTITY_QID"))))
    
    return Security_Level_DF, EMS_Frame

##################################
###########MatplotLib#############
##################################
def Curve_Plotting(TopPercentage, temp_Country, Lower_GMSR, Upper_GMSR):
    # Convert the necessary columns to numpy arrays for plotting
    X_Axis = TopPercentage.select("CumWeight_Cutoff").to_numpy().flatten()
    Y_Axis = TopPercentage.select("Full_MCAP_USD_Cutoff_Company").to_numpy().flatten()

    # Convert temp_Country to numpy arrays for plotting
    X_Axis_Temp = temp_Country.select("CumWeight_Cutoff").to_numpy().flatten()
    Y_Axis_Temp = temp_Country.select("Full_MCAP_USD_Cutoff_Company").to_numpy().flatten()

    # Create the plot
    plt.figure(figsize=(12, 8))
    plt.plot(X_Axis, Y_Axis, marker='o', linestyle='-', color='b')
    plt.plot(X_Axis_Temp, Y_Axis_Temp, linestyle='-', linewidth = 0.5, color='black', label='Temp Country') 

    # Add horizontal lines for Lower_GMSR and Upper_GMSR
    plt.axhline(y=Lower_GMSR, color='r', linestyle='solid', label=f'Lower GMSR = {Lower_GMSR}')
    plt.axhline(y=Upper_GMSR, color='g', linestyle='dotted', label=f'Upper GMSR = {Upper_GMSR}')

    # Add vertical lines at 80%, 85%, and 90%
    plt.axvline(x=0.80, color='orange', linestyle='--', label='80%')
    plt.axvline(x=0.85, color='purple', linestyle='--', label='85%')
    plt.axvline(x=0.90, color='cyan', linestyle='--', label='90%')

    # Adjust limits as needed
    plt.xlim(0, 1.0)  

    # Add labels and title
    plt.xlabel("Cumulative Weight (Cutoff)")
    plt.ylabel("Full MCAP USD (Cutoff)")
    plt.title("Cumulative Weight vs Full MCAP USD Cutoff")

    # Disable scientific notation on y-axis
    plt.ticklabel_format(style='plain', axis='y')

    # Optionally, format y-axis tick labels with commas
    plt.gca().get_yaxis().set_major_formatter(plt.matplotlib.ticker.StrMethodFormatter('{x:,.0f}'))

    # Add label for the last point
    last_x = X_Axis[-1]
    last_y = Y_Axis[-1]
    label = f"Full MCAP: {last_y:,.0f}\nCumWeight: {last_x:.2%}"

    # Adjust the label and arrow to be above the last point for better visibility
    plt.text(last_x, last_y * 5.45, label, fontsize=9, verticalalignment='bottom', horizontalalignment='right', color='black')

    # Adjust the arrow to point from the middle of the label to the actual point
    plt.annotate('', xy=(last_x, last_y), xytext=(last_x, last_y * 5.5),
                 arrowprops=dict(facecolor='black', arrowstyle='->', connectionstyle='arc3,rad=0.3'))

    plt.grid(False)
    plt.tight_layout()
    # Save the figure
    chart_file = f"chart_{TopPercentage['Date'][0]}_{TopPercentage['Country'][0]}.png"
    plt.savefig(chart_file)
    plt.close()  # Close the plot to free up memory
    return chart_file

##################################
##########Deletion Rule###########
##################################
def Deletion_Rule(TopPercentage, temp_Country, Left_Limit, Right_Limit, Lower_Limit, Upper_Limit):

    # Calculate Maximum # of Companies that can be deleted
    Maximum_Deletion = int(round(len(TopPercentage) * 0.05))

    # In case the nearest Integer is 0, adjust it to 2
    if (Maximum_Deletion < 2): Maximum_Deletion = 2

    # Declare initial number of Companies to be deleted
    Companies_To_Delete = 1

    # Case where at least we have 2 Companies #
    if len(TopPercentage) > 1:

        # Check if there are Companies in between Left and Right Limit
        if len(TopPercentage.filter((pl.col("CumWeight_Cutoff") >= Left_Limit) & (pl.col("CumWeight_Cutoff") <= Right_Limit))) > 0:
            
            # Iterate and check the condition, allowing for a minimum of 1 if the condition is met
            while Companies_To_Delete <= Maximum_Deletion:

                # Check to have at least 2 Companies
                if len(TopPercentage.head(len(TopPercentage) - Companies_To_Delete)) > 1:
                    # If removing Companies lands us inside the CumWeight Right_Limit, we allow Companies_To_Delete to be 1
                    TopPercentage_Trimmed = TopPercentage.head(len(TopPercentage) - Companies_To_Delete)

                    if Left_Limit <= TopPercentage.head(len(TopPercentage) - Companies_To_Delete).tail(1).select("CumWeight_Cutoff").to_numpy()[0][0] <= Right_Limit:

                        break  # Break the loop once the condition is met

                    else: 
                        # Try to increase Companies_To_Delete by 1 (up to the rounded 5% cap)
                        Companies_To_Delete += 1
                else:
                    # In case there is only one Company, we keep only it
                    TopPercentage_Trimmed = TopPercentage.head(1)
                    break

            # Assign the new trimmed Frame to the original TopPercentage
            TopPercentage = TopPercentage_Trimmed

                        
            TopPercentage = TopPercentage.with_columns(
                                                pl.lit("Below - Companies in between Upper and Lower GMSR").alias("Case")
                    )

        # If there are no Companies in between Left and Right Limit
        else:

            # Iterate and check the condition, allowing for a minimum of 1 if the condition is met
            while Companies_To_Delete <= Maximum_Deletion:
                
                # Check to have at least 2 Companies
                if len(TopPercentage.head(len(TopPercentage) - Companies_To_Delete)) > 1:

                    if (TopPercentage.head(len(TopPercentage) - Companies_To_Delete).tail(1).select("CumWeight_Cutoff").to_numpy()[0][0] < TopPercentage.tail(1).select("CumWeight_Cutoff").to_numpy()[0][0]) & \
                        (TopPercentage.head(len(TopPercentage) - Companies_To_Delete).tail(1).select("CumWeight_Cutoff").to_numpy()[0][0] >= Right_Limit):

                        # If CumWeight_Cutoff is still above or equal to Left_Limit, proceed
                        TopPercentage = TopPercentage.head(len(TopPercentage) - Companies_To_Delete)

                        # Try to increase Companies_To_Delete by 1 (up to the rounded 5% cap)
                        Companies_To_Delete += 1

                    else:
                        # If the CumWeight_Cutoff falls below Left_Limit, stop increasing Companies_To_Delete
                        # Handle the situation (e.g., revert to the previous state, or stop deletion)
                        print("Reached below Left_Limit. Stopping deletion.")
                        break

                else:
                    # In case there is only one Company, we keep only it
                    TopPercentage_Trimmed = TopPercentage.head(1)
                    break

            TopPercentage = TopPercentage.with_columns(
                                                pl.lit("Below - Companies in between Upper and Lower GMSR").alias("Case")
                                                    )
        
        # Check if last Company Full_MCAP_USD_Cutoff is below Lower_GMSR
        if TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0] < Lower_GMSR:
            # Calculate Maximum # of Companies that can be deleted  
            Maximum_Deletion = int(round(len(TopPercentage) * 0.20))

            # In case the nearest Integer is 0, adjust it to 2
            if (Maximum_Deletion < 2): Maximum_Deletion = 2

            # Declare initial number of Companies to be deleted
            Companies_To_Delete = 1

            # Iterate and check the condition, allowing for a minimum of 1 if the condition is met
            while Companies_To_Delete <= Maximum_Deletion:
                # Check to have at least 2 Companies
                if len(TopPercentage.head(len(TopPercentage) - Companies_To_Delete)) > 1:

                    if TopPercentage.head(len(TopPercentage) - Companies_To_Delete).tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0] >= Lower_GMSR:
                        # If CumWeight_Cutoff is still above or equal to Left_Limit, proceed
                        TopPercentage = TopPercentage.head(len(TopPercentage) - Companies_To_Delete)
                        break  # Break the loop once the condition is met

                    else:
                        # Try to increase Companies_To_Delete by 1 (up to the rounded 20% cap)
                        Companies_To_Delete += 1

                    # In case the threshold is not reached, just remove what is possible
                    TopPercentage = TopPercentage.head(len(TopPercentage) - Maximum_Deletion)

                else:
                    # In case there is only one Company, we keep only it
                    TopPercentage_Trimmed = TopPercentage.head(1)
                    break
    else:
        TopPercentage = TopPercentage.with_columns(
                                    pl.lit("Number of Companies equal to 1 - No Deletion").alias("Case")
        )
    return TopPercentage

##################################
###########Chairs Rule############
##################################
def Fill_Chairs(temp_Country, Companies_To_Fill, Country_Cutoff, Country_Cutoff_Upper, Country_Cutoff_Lower):

    # Sort Companies
    temp_Country = temp_Country.sort("Full_MCAP_USD_Cutoff_Company", descending=True)

    # Check for those Companies above Country_Cutoff 1.5X
    priority1 = temp_Country.filter(pl.col("Full_MCAP_USD_Cutoff_Company") >= Country_Cutoff)

    if priority1.height >= Companies_To_Fill:
        return priority1.head(Companies_To_Fill).with_columns(
            pl.col("Shadow_Company").fill_null(True)
        )

    # Second priority: Add priority 2 to priority 1
    priority2 = temp_Country.filter(
                (pl.col("Full_MCAP_USD_Cutoff_Company") >= Country_Cutoff_Lower) & 
                (pl.col("Full_MCAP_USD_Cutoff_Company") < Country_Cutoff) & 
                (pl.col("Size") == "All_Cap")
                )
    
    if len(priority1) + len(priority2) >= Companies_To_Fill:
        TopPercentage = priority1.vstack(priority2)

        return TopPercentage.head(Companies_To_Fill).with_columns(
            pl.col("Shadow_Company").fill_null(True)
        )

##################################
##Minimum FreeFloatCountry Level##
##################################
def Minimum_FreeFloat_Country(TopPercentage, temp_Country, Lower_GMSR, Upper_GMSR, date, country, Segment: pl.Utf8, Original_MCAP_Emerging):
    # Check if last Company Full_MCAP_USD_Cutoff_Company is in between Upper and Lower GMSR

    # No Buffer for the Starting Date
    if (date == Starting_Date) | (len(Output_Standard_Index.filter(pl.col("Country") == country)) == 0):

        # Case inside the box
        if (TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0] <= Upper_GMSR) & (TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0] >= Lower_GMSR):
        
            # Country_Cutoff is the Full_MCAP_USD_Cutoff_Company
            Country_Cutoff_Shadow = TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0] / 2
            Country_Cutoff_Upper_Shadow = Country_Cutoff_Shadow * 1.5
            Country_Cutoff_Lower_Shadow = Country_Cutoff_Shadow * (2/3)

        # Case above the box
        elif (TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0] > Upper_GMSR):

            # Country_Cutoff is the Upper_GMSR
            Country_Cutoff_Shadow = Upper_GMSR / 2
            Country_Cutoff_Upper_Shadow = Country_Cutoff_Shadow * 1.5
            Country_Cutoff_Lower_Shadow = Country_Cutoff_Shadow * (2/3)

        # Case below the box
        else:

            # Country_Cutoff is the GMSR
            Country_Cutoff_Shadow = Lower_GMSR / 2
            Country_Cutoff_Upper_Shadow = Country_Cutoff_Shadow * 1.5
            Country_Cutoff_Lower_Shadow = Country_Cutoff_Shadow * (2/3)

        # Transform TopPercentage from Companies to Securities level
        TopPercentage_Securities = temp_Emerging.select(pl.col("Date", "Internal_Number", "Instrument_Name",
                                "ENTITY_QID", "Country", "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff")).filter(pl.col("ENTITY_QID").is_in(TopPercentage.select(
                                    pl.col("ENTITY_QID").unique()
                                ))).with_columns(
                                    pl.lit("All_Cap").alias("Size")
                                )
        
        # Calculate Country_Cutoff to remove too big Securities
        Country_Cutoff = Country_Cutoff_Shadow * 2

        # Check for Companies inside the ETFs
        TopPercentage_Securities = (
                            TopPercentage_Securities
                            .drop("Size")
                            .join(
                                ETF.select(pl.col(["Internal_Number", "Size"])),
                                on=["Internal_Number"],
                                how="left"
                            )
                        )
        
        if date == Starting_Date:
        
            # Add information for Company_Full_MCAP to the Security level Frame
            TopPercentage_Securities = TopPercentage_Securities.join(TopPercentage.select(pl.col(["ENTITY_QID", "Full_MCAP_USD_Cutoff_Company"])),
                                                                    on=["ENTITY_QID"], how="left")
            
            # Remove those Companies where Size == NULL and Full_MCAP_USD_Cutoff_Company > Country_Cutoff
            TopPercentage_Securities = TopPercentage_Securities.filter(~(pl.col("Size").is_null()) & 
                                        (pl.col("Full_MCAP_USD_Cutoff_Company") > Country_Cutoff)).drop("Full_MCAP_USD_Cutoff_Company")
        
        # Check for Shadow_Company
        TopPercentage_Securities = TopPercentage_Securities.with_columns(
                                    pl.when(pl.col("Size").is_not_null())
                                    .then(False)
                                    .otherwise(
                                        pl.when(pl.col("Free_Float_MCAP_USD_Cutoff") >= Country_Cutoff_Lower_Shadow)
                                        .then(False)
                                        .otherwise(True)
                                    ).alias("Shadow_Company")).with_columns(
                                        pl.lit("All_Cap").alias("Size")
                                    )

        # Check that there are at least 3 Companies
        if len(temp_Emerging.filter(pl.col("Country") == country)) >= 3:

            # Check number of Current Securities
            if len(TopPercentage_Securities.filter(pl.col("Shadow_Company") == False)) < 3:
                # Keep only Non-Shadow Securities
                TopPercentage_Securities = TopPercentage_Securities.filter(pl.col("Shadow_Company") == False).with_columns(
                    pl.lit("All_Cap").alias("Size"),
                    pl.lit("Index_Creation").alias("Case")
                    )

                # Check for Index Continuity
                TopPercentage_Securities_Addition = temp_Emerging.filter(pl.col("Country") == country).sort("Free_Float_MCAP_USD_Cutoff", descending=True).select(pl.col([
                    "Date", "Internal_Number", "Instrument_Name", "ENTITY_QID", "Country", "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff"
                ])).with_columns(
                    pl.lit("All_Cap").alias("Size"),
                    pl.lit("Index_Creation").alias("Case"),
                    pl.lit(False).alias("Shadow_Company")
                ).filter(~pl.col("Internal_Number").is_in(TopPercentage_Securities.select(pl.col("Internal_Number")))).sort("Free_Float_MCAP_USD_Cutoff",
                                                                                                                            descending=True)

                # Stack the two Frames
                TopPercentage_Securities = TopPercentage_Securities.vstack(TopPercentage_Securities_Addition.head(3 - len(TopPercentage_Securities))
                                                                           .select(TopPercentage_Securities.columns)) 
        
        # In case there are not enough Companies
        else:
            TopPercentage_Securities = TopPercentage_Securities.head(0)
            TopPercentage = TopPercentage.head(0)

        TopPercentage = TopPercentage_Securities.group_by(
                                                    ["Date", "ENTITY_QID"]).agg([
                                                        pl.col("Country").first().alias("Country"),
                                                    ]).with_columns(
                                                        pl.lit("All_Cap").alias("Size"),
                                                        pl.lit("Index Creation").alias("Case")
                                                    )

    else: 
        # Buffer for Companies
        Companies_To_Fill = TopPercentage.height

        # Create the list of Dates
        Dates_List = Pivot_TOR.index.to_list()
        Previous_Date = datetime.datetime.strptime(Dates_List[max(0, IDX_Current - 1)], "%Y-%m-%d").date()

        # Information at Security Level for Current Country Index transposed into Company Level
        QID_Standard_Index = Standard_Index.filter((pl.col("Country") == country) & (pl.col("Date") == Previous_Date)).select(
            pl.col("ENTITY_QID", "Shadow_Company", "Internal_Number"))

        # Information at Security Level for Current Country Index transposed into Company Level
        QID_Small_Index = Small_Index.filter((pl.col("Country") == country) & (pl.col("Date") == Previous_Date)).select(
            pl.col("ENTITY_QID", "Shadow_Company", "Internal_Number", "Country"))

        # Get which of the Current Index Components are still Investable by checking temp_Emerging/temp_Developed after Screens have been applied to them
        if Segment == "Emerging":
            Security_Standard_Index_Current = QID_Standard_Index.join(temp_Emerging.select(pl.col(["Internal_Number", "Country"])),
                on=["Internal_Number"], how="left")
            
            # Small #
            Security_Small_Index_Current = QID_Small_Index.join(temp_Emerging.select(pl.col(["Internal_Number", "Country"])),
                on=["Internal_Number"], how="left")
        
        elif Segment == "Developed":
            Security_Standard_Index_Current = QID_Standard_Index.join(temp_Developed.select(pl.col(["Internal_Number", "Country"])),
                on=["Internal_Number"], how="left")
            
            # Small #
            Security_Small_Index_Current = QID_Small_Index.join(temp_Developed.select(pl.col(["Internal_Number", "Country"])),
                on=["Internal_Number"], how="left")
            
        # Group them by ENTITY_QID
        Company_Standard_Index_Current = Security_Standard_Index_Current.group_by(
                                                    ["ENTITY_QID"]).agg([
                                                        pl.col("Country").first().alias("Country"),
                                                        pl.col("Shadow_Company").first().alias("Shadow_Company"),
                                                    ]).with_columns(
                                                        pl.lit("All_Cap").alias("Size")
                                                    )
        
        Company_Small_Index_Current = Security_Small_Index_Current.group_by(
                                            ["ENTITY_QID"]).agg([
                                                pl.col("Country").first().alias("Country"),
                                                pl.col("Shadow_Company").first().alias("Shadow_Company"),
                                            ]).with_columns(
                                                        pl.lit("SMALL").alias("Size"),
                                                        pl.col("Shadow_Company").fill_null(False)
                                                    )
        
        # Create the Current All_Cap Index
        Current_Index = Company_Standard_Index_Current.vstack(Company_Small_Index_Current)

        # Add information of Standard/Small Companies to Refreshed Universe
        temp_Country = temp_Country.join(Current_Index.select(pl.col(["ENTITY_QID", "Shadow_Company", "Size"])), on=["ENTITY_QID"], how="left").with_columns(
            pl.col("Size").fill_null("NEW")
        )
        
        #################
        # Case Analysis #
        #################

        # Case inside the box
        if (TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0] <= Upper_GMSR) & (TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0] >= Lower_GMSR):
        
            # Country_GMSR
            Country_Cutoff = TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0]
            Country_Cutoff_Upper = Country_Cutoff * 1.5
            Country_Cutoff_Lower = Country_Cutoff * (2/3)

        # Case above the box
        elif (TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0] > Upper_GMSR):

            # Country_GMSR is the Upper_GMSR
            Country_Cutoff = Upper_GMSR
            Country_Cutoff_Upper = Country_Cutoff * 1.5
            Country_Cutoff_Lower = Country_Cutoff * (2/3)

        # Case below the box
        else:

            # Country_GMSR is the GMSR
            Country_Cutoff = Lower_GMSR
            Country_Cutoff_Upper = Country_Cutoff * 1.5
            Country_Cutoff_Lower = Country_Cutoff * (2/3)


        # Eligibility Rule for All_Cap Index #
        TopPercentage = Fill_Chairs(temp_Country, Companies_To_Fill, Country_Cutoff, Country_Cutoff_Upper, Country_Cutoff_Lower)
        
        # Find if there are new Companies in the Yellow Box
        TopPercentage = TopPercentage.with_columns(
            pl.lit(None).alias("ELIGIBLE")
        )

        # Check if there are NEW Securities in the Yellow Box
        if len(TopPercentage.filter((pl.col("Full_MCAP_USD_Cutoff_Company") >= Country_Cutoff) &
                            (pl.col("Full_MCAP_USD_Cutoff_Company") <= Country_Cutoff_Upper) &
                            (pl.col("Size") == "NEW"))) > 0:
            
            # Check how many Current componets (Previous Index) have fallen below Country Cutoff 2/3
            Fallen_Current_Companies = temp_Country.filter((pl.col("Size") == "All_Cap") & (pl.col("Full_MCAP_USD_Cutoff_Company") < Country_Cutoff_Lower)).height

            # Isolate the NEW Companies in the Yellow box
            Yellow_Box_Companies = TopPercentage.filter((pl.col("Full_MCAP_USD_Cutoff_Company") >= Country_Cutoff) &
                            (pl.col("Full_MCAP_USD_Cutoff_Company") <= Country_Cutoff_Upper) &
                            (pl.col("Size") == "NEW"))
            
            # Remove them from TopPercentage
            TopPercentage = TopPercentage.filter(~pl.col("ENTITY_QID").is_in(Yellow_Box_Companies.select(pl.col("ENTITY_QID")))).with_columns(
                pl.lit(True).alias("ELIGIBLE")
            )

            # Apply the logic for Eligibility
            if Fallen_Current_Companies > 0:
                Yellow_Box_Companies = Yellow_Box_Companies.with_columns(
                    pl.when(pl.arange(0, pl.count()) < Fallen_Current_Companies)  # Set True for top `Fallen_Current_Companies` rows
                    .then(True)
                    .otherwise(False)
                    .alias("ELIGIBLE")
                )
            else:
                Yellow_Box_Companies = Yellow_Box_Companies.with_columns(
                    pl.lit(False).alias("ELIGIBLE")
                )

            # Stack Frames
            TopPercentage = TopPercentage.vstack(Yellow_Box_Companies).sort("Full_MCAP_USD_Cutoff_Company", descending=True)

        else:

            # In case there are no NEW Securities in the Yellow Box
            TopPercentage = TopPercentage.with_columns(
                                    pl.when(
                                        (pl.col("Full_MCAP_USD_Cutoff_Company") >= Country_Cutoff) &
                                        (pl.col("Full_MCAP_USD_Cutoff_Company") <= Country_Cutoff_Upper) &
                                        (pl.col("Size") == "NEW")
                                    )
                                    .then(pl.lit(False))
                                    .otherwise(pl.lit(True))
                                    .alias("ELIGIBLE")
                                )
            
        # Transform TopPercentage to Security level
        TopPercentage_Securities = temp_Emerging.select(pl.col("Date", "Internal_Number", "Instrument_Name",
                                "ENTITY_QID", "Country", "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff")).filter(pl.col("ENTITY_QID").is_in(TopPercentage.select(
                                    pl.col("ENTITY_QID").unique()
                                )))
        
        # Add ELIGIBLE information
        TopPercentage_Securities = TopPercentage_Securities.join(TopPercentage.select(["ENTITY_QID", "ELIGIBLE"]), on="ENTITY_QID", how="left").sort("Full_MCAP_USD_Cutoff", descending=True)
        
        # Add SHADOW information from Previous Standard_Index
        TopPercentage_Securities = TopPercentage_Securities.join(Standard_Index.filter((pl.col("Country") == country) & (pl.col("Date") == Previous_Date)).select(
            ["Internal_Number", "Shadow_Company"]), on=["Internal_Number"], how="left")
        
        # Ensure that there is not Empty Shadow_Company column
        TopPercentage_Securities = TopPercentage_Securities.with_columns(
                                    pl.col("Shadow_Company").fill_null(True).alias("Shadow_Company")
                                    )

        # Verify for Shadow Securities
        TopPercentage_Securities = TopPercentage_Securities.with_columns(
                                    pl.when((pl.col("Shadow_Company") == False) & (pl.col("ELIGIBLE") == True))
                                    .then(pl.col("Free_Float_MCAP_USD_Cutoff") < (Country_Cutoff * 0.5 * (2 / 3)))
                                    .when((pl.col("Shadow_Company") == True) & (pl.col("ELIGIBLE") == True))
                                    .then(pl.col("Free_Float_MCAP_USD_Cutoff") < (Country_Cutoff * 0.5))
                                    .otherwise(True)  # Set to True if none of the conditions match
                                    .alias("Update_Shadow_Company")
                                ).drop("Shadow_Company", "ELIGIBLE").rename({"Update_Shadow_Company": "Shadow_Company"})

        # Adapt TopPercentage
        TopPercentage = TopPercentage_Securities.group_by(["Date", "ENTITY_QID"]).agg([
                pl.col("Country").first().alias("Country")
            ]).with_columns([
                pl.lit("All_Cap").alias("Size"),
                pl.lit("Maintenance").alias("Case")
            ])

        # Variables Shadow for Index Continuity
        Country_Cutoff_Shadow = Country_Cutoff / 2
        Country_Cutoff_Upper_Shadow = Country_Cutoff_Shadow * 1.5
        Country_Cutoff_Lower_Shadow = Country_Cutoff_Shadow * (2/3)

        # Check that there are at least 3 Companies
        if len(temp_Emerging.filter(pl.col("Country") == country)) >= 3:

            # Check for Index Continuity
            TopPercentage_Securities, TopPercentage = Index_Continuity(TopPercentage_Securities, TopPercentage, "Emerging", temp_Emerging, country, Standard_Index)

        else:

            TopPercentage_Securities = TopPercentage_Securities.head(0)
            TopPercentage = TopPercentage.head(0)

        # Adjust the columns
        TopPercentage_Securities = TopPercentage_Securities.with_columns(
            pl.lit("All_Cap").alias("Size"),
            pl.lit("Buffer").alias("Case")
        )

        TopPercentage = TopPercentage.with_columns(
            pl.lit("Buffer").alias("Case"),
            pl.lit("All_Cap").alias("Size")
        )

    # Return the Frame
    return TopPercentage, TopPercentage_Securities

##################################
##########Index Creation##########
##################################
def Index_Creation_Box(Frame: pl.DataFrame, Lower_GMSR, Upper_GMSR, country, date, Excel_Recap, Percentage, Right_Limit, Left_Limit, Segment: pl.Utf8, writer):

    temp_Country = Frame.filter((pl.col("Date") == date) & (pl.col("Country") == country))

    # Sort in each Country the Companies by Full MCAP USD Cutoff
    temp_Country = temp_Country.sort("Full_MCAP_USD_Cutoff_Company", descending=True)

    # No Country_Adjustment as we are building All_Cap
    Country_Percentage = Percentage

    # Calculate their CumWeight_Cutoff
    temp_Country = temp_Country.with_columns(
                    (pl.col("Free_Float_MCAP_USD_Cutoff_Company") / pl.col("Free_Float_MCAP_USD_Cutoff_Company").sum()).alias("Weight_Cutoff"),
                    (((pl.col("Free_Float_MCAP_USD_Cutoff_Company") / pl.col("Free_Float_MCAP_USD_Cutoff_Company").sum()).cum_sum())).alias("CumWeight_Cutoff")
    )

    # Country Adjustment
    if Coverage_Adjustment == True:
        Country_Adjustment = Country_Coverage.filter(pl.col("Country") == country).select(pl.col("Coverage")).to_numpy()[0][0]
    else:
        Country_Adjustment = 1

    # Check where the top 99% (crossing it) lands us on the Curve
    TopPercentage = (
    temp_Country
    .select([
        "Date", 
        "Internal_Number", 
        "Instrument_Name", 
        "ENTITY_QID", 
        "Country", 
        "Free_Float_MCAP_USD_Cutoff_Company",
        "Full_MCAP_USD_Cutoff_Company", 
        "Weight_Cutoff", 
        "CumWeight_Cutoff"
    ])
    .filter(pl.col("CumWeight_Cutoff") <= (Country_Percentage / Country_Adjustment))
    .vstack(
        temp_Country
        .select([
            "Date", 
            "Internal_Number", 
            "Instrument_Name", 
            "ENTITY_QID", 
            "Country", 
            "Free_Float_MCAP_USD_Cutoff_Company", 
            "Full_MCAP_USD_Cutoff_Company", 
            "Weight_Cutoff", 
            "CumWeight_Cutoff"
        ])
        .filter(pl.col("CumWeight_Cutoff") > (Country_Percentage / Country_Adjustment))
        .head(1)
        )
    )

    # Check that the last Company is not below the Lower_GMSR
    if TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0] < Lower_GMSR:
        TopPercentage = TopPercentage.filter(pl.col("Full_MCAP_USD_Cutoff_Company") >= Lower_GMSR)

    #### This check should be at Security Level ###
    #### It goes at very last end ###

    # Check that minimum number is respected
    if Segment == "Developed":
        if len(TopPercentage) < 5: 
            TopPercentage = temp_Country.head(5)
            TopPercentage = TopPercentage.with_columns(
                pl.lit("Minimum Number of Companies").alias("Case"),
                pl.lit("Reintroduction").alias("Size")
            ).select(pl.col(["Date", "Internal_Number", "Instrument_Name", "ENTITY_QID", "Country", "Free_Float_MCAP_USD_Cutoff_Company",
                            "Full_MCAP_USD_Cutoff_Company", "Weight_Cutoff", "CumWeight_Cutoff", "Size", "Case"]))
    elif Segment == "Emerging":
        if len(TopPercentage) < 3: 
            TopPercentage = temp_Country.head(3)
            TopPercentage = TopPercentage.with_columns(
                pl.lit("Index Creation").alias("Case"),
                pl.lit("Minimum Number of Companies").alias("Size")
            ).select(pl.col(["Date", "Internal_Number", "Instrument_Name", "ENTITY_QID", "Country", "Free_Float_MCAP_USD_Cutoff_Company",
                            "Full_MCAP_USD_Cutoff_Company", "Weight_Cutoff", "CumWeight_Cutoff", "Size", "Case"]))
            
    # Add Case/Size
    TopPercentage = TopPercentage.with_columns(
                    pl.lit("All_Cap").alias("Size"),
                    pl.lit("Index Creation").alias("Case")
    )

    return TopPercentage, temp_Country

##################################
########Index Rebalancing#########
##################################
def Index_Rebalancing_Box(Frame: pl.DataFrame, SW_ACALLCAP, Output_Count_Standard_Index, Lower_GMSR, Upper_GMSR, country, date, Excel_Recap,  Right_Limit, Left_Limit, Segment: pl.Utf8, writer):
    temp_Country = Frame.filter((pl.col("Date") == date) & (pl.col("Country") == country))

    # Sort in each Country the Companies by Full MCAP USD Cutoff
    temp_Country = temp_Country.sort("Full_MCAP_USD_Cutoff_Company", descending=True)
    
    # Calculate their CumWeight_Cutoff
    temp_Country = temp_Country.with_columns(
                    (pl.col("Free_Float_MCAP_USD_Cutoff_Company") / pl.col("Free_Float_MCAP_USD_Cutoff_Company").sum()).alias("Weight_Cutoff"),
                    (((pl.col("Free_Float_MCAP_USD_Cutoff_Company") / pl.col("Free_Float_MCAP_USD_Cutoff_Company").sum()).cum_sum())).alias("CumWeight_Cutoff")
                    ).sort("Full_MCAP_USD_Cutoff_Company", descending=True)

    # Create the list of Dates
    Dates_List = Pivot_TOR.index.to_list()
    Previous_Date = datetime.datetime.strptime(Dates_List[max(0, IDX_Current - 1)], "%Y-%m-%d").date()

    # Information at Company Level
    QID_Standard_Index = Output_Standard_Index.filter((pl.col("Country") == country) & (pl.col("Date") == Previous_Date)).select(pl.col("Date", "ENTITY_QID"))

    # Check which of the QID_Standard_Index Companies is still alive (it is not relevant if it does not pass the Screens)
    # Duplicates are dropped due to ENTITY_QID / Keep only Free_Float_MCAP_USD_Cutoff > 0
    QID_Standard_Index = QID_Standard_Index.join(Emerging.filter((pl.col("Country")==country) & (pl.col("Date")==date)).select(pl.col("Free_Float_MCAP_USD_Cutoff",
                        "ENTITY_QID")), on=["ENTITY_QID"], how="left").unique(subset=["ENTITY_QID"]).filter(pl.col("Free_Float_MCAP_USD_Cutoff") > 0)

    # Check the number selected in the previous Index
    Company_Selection_Count = QID_Standard_Index.height

    # Check where X number of Companies lands us on the Curve
    TopPercentage = temp_Country.head(Company_Selection_Count)

    # Adjust the Left & Right Limit based on each Country
    Country_Adjustment =  Percentage

    #################
    # Case Analysis #
    #################

    # Best case where we land inside Upper and Lower GMSR # 
    if (TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0] >= Lower_GMSR) & (TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0] <= Upper_GMSR):
        
        ############
        #Ideal Case#    
        ############

        # Check that the Curve is in the desired target coverage TODO Done!
        if Left_Limit <= TopPercentage.tail(1).select("CumWeight_Cutoff").to_numpy()[0][0] <= Right_Limit:

            TopPercentage = TopPercentage.with_columns(
                                        pl.lit("All_Cap").alias("Size")
                                )
            
            TopPercentage = TopPercentage.with_columns(
                                        pl.lit("Inside").alias("Case")
            )

        ############
        # Addition #
        ############

        # If we are inside the Upper and Lower GMSR but below the target coverage [add the first one that crosses Left_Limit and is between Left_Limit and Right_Limit and then stop]
        elif Left_Limit > TopPercentage.tail(1).select("CumWeight_Cutoff").to_numpy()[0][0]:  #TODO To be Reviewed

            # Check if there are Companies in between the Upper and Lower GMSR
            if len(temp_Country.filter((pl.col("CumWeight_Cutoff") <= Right_Limit) & \
                                       (pl.col("Full_MCAP_USD_Cutoff_Company") >= Lower_GMSR) & \
                                        (~pl.col("Internal_Number").is_in(TopPercentage.select("Internal_Number"))))) > 0:

                TopPercentage_Extension = (
                    temp_Country
                    .filter((pl.col("Full_MCAP_USD_Cutoff_Company") < Upper_GMSR) & (pl.col("Full_MCAP_USD_Cutoff_Company") > Lower_GMSR) & (pl.col("CumWeight_Cutoff") < Left_Limit))
                    .sort("Full_MCAP_USD_Cutoff_Company", descending=True)
                    .filter(~pl.col("Internal_Number").is_in(TopPercentage.select(pl.col("Internal_Number"))))
                    .vstack(
                        temp_Country
                        .filter((pl.col("Full_MCAP_USD_Cutoff_Company") < Upper_GMSR) & (pl.col("Full_MCAP_USD_Cutoff_Company") > Lower_GMSR) & (pl.col("CumWeight_Cutoff") >= Left_Limit) & 
                                (~pl.col("Internal_Number").is_in(TopPercentage.select(pl.col("Internal_Number"))))
                        )
                        .sort("Full_MCAP_USD_Cutoff_Company", descending=True)
                        .head(1)  # Select the first row crossing the Left_Limit
                    )
                    .select([
                        "Date", 
                        "Internal_Number", 
                        "Instrument_Name", 
                        "ENTITY_QID", 
                        "Country", 
                        "Free_Float_MCAP_USD_Cutoff_Company", 
                        "Full_MCAP_USD_Cutoff_Company", 
                        "Weight_Cutoff", 
                        "CumWeight_Cutoff"
                    ])
                )

                TopPercentage = TopPercentage.with_columns(
                                    pl.lit("Addition").alias("Size")
                            )
            
                TopPercentage_Extension = TopPercentage_Extension.with_columns(
                                        pl.lit("Addition").alias("Size")
                            )
                
                # Merge the initial Frame with the additions
                if len(TopPercentage_Extension) > 0:
                    TopPercentage = TopPercentage.vstack(TopPercentage_Extension.select(TopPercentage.columns))

                TopPercentage = TopPercentage.with_columns(
                                        pl.lit("Above - Companies in between Upper and Lower GMSR").alias("Case")
                            )

            # There are no Companies in between the Upper and Lower GMSR and with a CumWeight higher than Left_Limit
            else:
                # Add as many Companies as possible to get closer to Left_Limit
                TopPercentage_Extension = (
                    temp_Country
                    .filter((pl.col("Full_MCAP_USD_Cutoff_Company") >= Lower_GMSR) & (pl.col("CumWeight_Cutoff") <= Right_Limit))
                    .sort("Full_MCAP_USD_Cutoff_Company", descending=True)
                    .filter(~pl.col("Internal_Number").is_in(TopPercentage.select(pl.col("Internal_Number"))))
                    .select([
                        "Date", 
                        "Internal_Number", 
                        "Instrument_Name", 
                        "ENTITY_QID", 
                        "Country", 
                        "Free_Float_MCAP_USD_Cutoff_Company", 
                        "Full_MCAP_USD_Cutoff_Company", 
                        "Weight_Cutoff", 
                        "CumWeight_Cutoff"
                    ])
                )

                TopPercentage = TopPercentage.with_columns(
                            pl.lit("Addition").alias("Size")
                    )
                
                TopPercentage_Extension = TopPercentage_Extension.with_columns(
                        pl.lit("Addition").alias("Size")
                    )
                                    
                # Merge the initial Frame with the additions
                if len(TopPercentage_Extension) > 0:
                    TopPercentage = TopPercentage.vstack(TopPercentage_Extension.select(TopPercentage.columns))      

                TopPercentage = TopPercentage.with_columns(
                                            pl.lit("Above - No Companies in between Upper and Lower GMSR").alias("Case")
                )
                
        ############
        # Deletion #
        ############
        
        # If we are inside the Upper and Lower GMSR but above the target coverage [delete until we get inside the box, between 80% and 90%]
        elif Right_Limit < TopPercentage.tail(1).select("CumWeight_Cutoff").to_numpy()[0][0]: # TODO To be Reviewed

            # Apply the Deletion Rule according to the Case if there Companies in between the Boundaries (GMSR & Coverage)
            TopPercentage = Deletion_Rule(TopPercentage, temp_Country, Left_Limit, Right_Limit, Lower_Limit, Upper_Limit)

            TopPercentage = TopPercentage.with_columns(
                        pl.lit("Deletion").alias("Size")
                    )

    # Case where we land above the box #
    elif TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0] > Upper_GMSR:

        # Check if there are Companies in between the Upper and Lower GMSR
        TopPercentage_Extension = (
            temp_Country
            .filter(pl.col("Full_MCAP_USD_Cutoff_Company") >= Upper_GMSR)
            .sort("Full_MCAP_USD_Cutoff_Company", descending=True)
            .filter(~pl.col("Internal_Number").is_in(TopPercentage.select(pl.col("Internal_Number"))))
            .select([
                "Date", 
                "Internal_Number", 
                "Instrument_Name", 
                "ENTITY_QID", 
                "Country", 
                "Free_Float_MCAP_USD_Cutoff_Company", 
                "Full_MCAP_USD_Cutoff_Company", 
                "Weight_Cutoff", 
                "CumWeight_Cutoff"
            ])
            .vstack(
                temp_Country
                .filter(pl.col("Full_MCAP_USD_Cutoff_Company") < Upper_GMSR)
                .sort("Full_MCAP_USD_Cutoff_Company", descending=True)
                .filter(~pl.col("Internal_Number").is_in(TopPercentage.select(pl.col("Internal_Number"))))
                .select([
                    "Date", 
                    "Internal_Number", 
                    "Instrument_Name", 
                    "ENTITY_QID", 
                    "Country", 
                    "Free_Float_MCAP_USD_Cutoff_Company", 
                    "Full_MCAP_USD_Cutoff_Company", 
                    "Weight_Cutoff", 
                    "CumWeight_Cutoff"
                ])
                .head(1)
            )
        )

        TopPercentage = TopPercentage.with_columns(
                                pl.lit("All_Cap").alias("Size")
                        )
        
        TopPercentage_Extension = TopPercentage_Extension.with_columns(
                                pl.lit("Addition").alias("Size")
                        )
        
        # Merge the initial Frame with the additions
        if len(TopPercentage_Extension) > 0:
            TopPercentage = TopPercentage.vstack(TopPercentage_Extension.select(TopPercentage.columns))

        TopPercentage = TopPercentage.with_columns(
                                pl.lit("Above - Companies in between Upper and Lower GMSR").alias("Case")
                )

        # Save DataFrame to Excel
        if Excel_Recap_Rebalancing == True and country == Country_Plotting:
            TopPercentage.to_pandas().to_excel(writer, sheet_name=f'{date}_{country}', index=False)

    # Case where we below above the box #
    elif TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0] < Lower_GMSR:
        # Apply the Deletion Rule according to the Case if there Companies in between the Boundaries (GMSR & Coverage)
        TopPercentage = Deletion_Rule(TopPercentage, temp_Country, Left_Limit, Right_Limit, Lower_Limit, Upper_Limit)

        TopPercentage = TopPercentage.with_columns(
                        pl.lit("Deletion").alias("Size")
                    )

    return TopPercentage, temp_Country

##################################
#Read Developed/Emerging Universe#
##################################

# Select columns to read from the Parquets
Columns = ["Date", "Index_Symbol", "Index_Name", "Internal_Number", "ISIN", "SEDOL", "RIC", "Instrument_Name", 
           "Country", "Currency", "Exchange", "ICB", "Free_Float", "Capfactor"]

# Developed Universe
Developed = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Universe\SWDACGV.parquet", columns=Columns).with_columns([
                            pl.col("Free_Float").cast(pl.Float64),
                            pl.col("Capfactor").cast(pl.Float64),
                            pl.col("Date").cast(pl.Date)
                            ])

# Emerging Universe
Emerging = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Universe\SWEACGV.parquet", columns=Columns).with_columns([
                            pl.col("Free_Float").cast(pl.Float64),
                            pl.col("Capfactor").cast(pl.Float64),
                            pl.col("Date").cast(pl.Date)
                            ])

# GCC Extra
GCC = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Universe\GCC.parquet").with_columns([
                            pl.col("Free_Float").cast(pl.Float64),
                            pl.col("Capfactor").cast(pl.Float64),
                            pl.col("Date").cast(pl.Date),
                            pl.col("ICB").cast(pl.Utf8),
                            pl.col("Exchange").cast(pl.Utf8)
                            ]).filter(pl.col("Date") >= datetime.date(2019,6,24))

# Merge Emerging with GCC
Emerging = Emerging.vstack(GCC)

# Entity_ID for matching Companies
Entity_ID = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Entity_ID\Entity_ID.parquet").select(pl.col(["ENTITY_QID", "STOXX_ID",
                            "RELATIONSHIP_VALID_FROM", "RELATIONSHIP_VALID_TO"])).with_columns(
                                pl.col("RELATIONSHIP_VALID_FROM").cast(pl.Date()),
                                pl.col("RELATIONSHIP_VALID_TO").cast(pl.Date()))

# SW AC ALLCAP for check on Cutoff
SW_ACALLCAP = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Universe\STXWAGV_Cutoff.parquet").with_columns([
                                pl.col("Date").cast(pl.Date),
                                pl.col("Mcap_Units_Index_Currency").cast(pl.Float64)
]).filter(pl.col("Mcap_Units_Index_Currency") > 0).join(pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Dates\Review_Date-QUARTERLY.csv").with_columns(
                        pl.col("Review").cast(pl.Utf8).str.strptime(pl.Date, "%m/%d/%Y"),
                        pl.col("Cutoff").cast(pl.Utf8).str.strptime(pl.Date, "%m/%d/%Y")
                      ), left_on="Date", right_on="Cutoff", how="left")

###################################
#####Filtering from StartDate######
###################################
Emerging = Emerging.filter(pl.col("Date") >= Starting_Date)
Developed = Developed.filter(pl.col("Date") >= Starting_Date)

##################################
######Add Cutoff Information######
##################################
Columns = ["validDate", "stoxxId", "currency", "closePrice", "shares"]

# Country Coverage for Index Creation
Country_Coverage = pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Universe\Country_Coverage.csv", separator=";")

# Read the Parquet and add the Review Date Column 
Securities_Cutoff = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Securities_Cutoff\Securities_Cutoff.parquet", columns=Columns).with_columns([
                      pl.col("closePrice").cast(pl.Float64),
                      pl.col("shares").cast(pl.Float64),
                      pl.col("validDate").cast(pl.Utf8).str.strptime(pl.Date, "%Y%m%d")
                      ]).join(pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Dates\Review_Date-QUARTERLY.csv").with_columns(
                        pl.col("Review").cast(pl.Utf8).str.strptime(pl.Date, "%m/%d/%Y"),
                        pl.col("Cutoff").cast(pl.Utf8).str.strptime(pl.Date, "%m/%d/%Y")
                      ), left_on="validDate", right_on="Cutoff", how="left")

FX_Cutoff = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Securities_Cutoff\FX_Historical.parquet").with_columns(
                            pl.col("Cutoff").cast(pl.Date)
)

# Add these information to the Developed and Emerging Universes
Developed = (
    Developed
    .join(
        Securities_Cutoff,
        left_on=["Date", "Internal_Number"],
        right_on=["Review", "stoxxId"],
        how="left"
    )
    .with_columns([
        # Fill null values in "currency" with the values from "Currency"
        pl.col("currency").fill_null(pl.col("Currency")),
    ])
    .drop("Currency")  # Drop the "Currency" column after filling nulls
    .rename({
        "validDate": "Cutoff",
        "closePrice": "Close_unadjusted_local_Cutoff",
        "shares": "Shares_Cutoff",
        "currency": "Currency"
    })
)

# Add FX_Cutoff
Developed = Developed.join(FX_Cutoff, on=["Cutoff", "Currency"], how="left")

Emerging = (
    Emerging
    .join(
        Securities_Cutoff,
        left_on=["Date", "Internal_Number"],
        right_on=["Review", "stoxxId"],
        how="left"
    )
    .with_columns([
        # Fill null values in "currency" with the values from "Currency"
        pl.col("currency").fill_null(pl.col("Currency")),
    ])
    .drop("Currency")  # Drop the "Currency" column after filling nulls
    .rename({
        "validDate": "Cutoff",
        "closePrice": "Close_unadjusted_local_Cutoff",
        "shares": "Shares_Cutoff",
        "currency": "Currency"
    })
)

# Add FX_Cutoff
Emerging = Emerging.join(FX_Cutoff, on=["Cutoff", "Currency"], how="left")

##################################
#########Drop Empty Rows##########
##################################
Developed = Developed.filter(~((pl.col("FX_local_to_Index_Currency_Cutoff").is_null()) | (pl.col("Close_unadjusted_local_Cutoff").is_null()) | (pl.col("Shares_Cutoff").is_null())))
Emerging = Emerging.filter(~((pl.col("FX_local_to_Index_Currency_Cutoff").is_null()) | (pl.col("Close_unadjusted_local_Cutoff").is_null()) | (pl.col("Shares_Cutoff").is_null())))

##################################
####Read Turnover Information#####
##################################

# TurnOverRatio
Turnover = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Turnover\Turnover_Cutoff_SWALL.parquet")
# Drop unuseful columns
Turnover = Turnover.drop(["vd", "calcType", "token"])
# Keep only relevant fields
Turnover = Turnover.filter(pl.col("field").is_in(["TurnoverRatioFO", "TurnoverRatioFO_India1"]))
# Transform the table
Turnover = Turnover.pivot(
                values="Turnover_Ratio",
                index=["Date", "Internal_Number"],
                on="field"
                ).rename({"TurnoverRatioFO": "Turnover_Ratio"})
# Fill NA in TurnoverRatioFO_India1
Turnover = Turnover.with_columns(
                                pl.col("TurnoverRatioFO_India1").fill_null(pl.col("Turnover_Ratio"))
                                ).drop("Turnover_Ratio").rename({"TurnoverRatioFO_India1": "Turnover_Ratio"}).to_pandas()
# Add Turnover Information
Pivot_TOR = Turnover.pivot(values="Turnover_Ratio", index="Date", columns="Internal_Number")

# Add ENTITY_QID to main Frames
Developed = Developed.join(
                            Entity_ID,
                            left_on="Internal_Number",
                            right_on="STOXX_ID",
                            how="left"
                          ).unique(["Date", "Internal_Number"]).sort("Date", descending=False).with_columns(
                              pl.col("ENTITY_QID").fill_null(pl.col("Internal_Number"))).drop({"RELATIONSHIP_VALID_FROM", "RELATIONSHIP_VALID_TO"})

Emerging = Emerging.join(
                            Entity_ID,
                            left_on="Internal_Number",
                            right_on="STOXX_ID",
                            how="left"
                          ).unique(["Date", "Internal_Number"]).sort("Date", descending=False).with_columns(
                              pl.col("ENTITY_QID").fill_null(pl.col("Internal_Number"))).drop({"RELATIONSHIP_VALID_FROM", "RELATIONSHIP_VALID_TO"})

# Mask CN Securities
Chinese_CapFactor = China_A_Securities(Emerging)

# Add the information to Emerging Universe
Emerging = Emerging.join(Chinese_CapFactor.select(pl.col(["Date", "Internal_Number", "Capfactor_CN"])), on=["Date", "Internal_Number"], how="left").with_columns(
                        pl.col("Capfactor_CN").fill_null(pl.col("Capfactor"))).drop("Capfactor").rename({"Capfactor_CN": "Capfactor"})

# Calculate Free/Full MCAP USD for Developed Universe
Developed = Developed.with_columns(
                                    (pl.col("Free_Float") * pl.col("Capfactor") * pl.col("Close_unadjusted_local_Cutoff") * pl.col("FX_local_to_Index_Currency_Cutoff") * pl.col("Shares_Cutoff"))
                                    .alias("Free_Float_MCAP_USD_Cutoff"),
                                    (pl.col("Close_unadjusted_local_Cutoff") * pl.col("FX_local_to_Index_Currency_Cutoff") * pl.col("Shares_Cutoff"))
                                    .alias("Full_MCAP_USD_Cutoff")
                                  )

# Calculate Free/Full MCAP USD for Emerging Universe
Emerging = Emerging.with_columns(
                                    (pl.col("Free_Float") * pl.col("Capfactor") * pl.col("Close_unadjusted_local_Cutoff") * pl.col("FX_local_to_Index_Currency_Cutoff") * pl.col("Shares_Cutoff"))
                                    .alias("Free_Float_MCAP_USD_Cutoff"),
                                    (pl.col("Close_unadjusted_local_Cutoff") * pl.col("FX_local_to_Index_Currency_Cutoff") * pl.col("Shares_Cutoff"))
                                    .alias("Full_MCAP_USD_Cutoff")
                                  )

# Check if there is any Free_Float_MCAP_USD_Cutoff Empty
Emerging = Emerging.filter(pl.col("Free_Float_MCAP_USD_Cutoff") > 0)
Developed = Developed.filter(pl.col("Free_Float_MCAP_USD_Cutoff") > 0)

###################################
#########Chinese Mapping###########
###################################
Chinese_Mapping = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Universe\Mapping_Chinese_Securities.parquet")
Chinese_Mapping = (Chinese_Mapping.unpivot(index="ISIN", on=["ID_1", "ID_2", "ID_3"], variable_name="ID_Type", value_name="Internal_Number")
                   .drop_nulls(subset=["Internal_Number"])
                   .drop("ID_Type"))

###################################
####Creation of main GMSR Frame####
###################################
GMSR_Frame = pl.DataFrame({
                            "Date": pl.Series(dtype=pl.Date),
                            "GMSR_Developed": pl.Series(dtype=pl.Float64),
                            "GMSR_Developed_Upper": pl.Series(dtype=pl.Float64),
                            "GMSR_Developed_Lower": pl.Series(dtype=pl.Float64),
                            "GMSR_Emerging": pl.Series(dtype=pl.Float64),
                            "GMSR_Emerging_Upper": pl.Series(dtype=pl.Float64),
                            "GMSR_Emerging_Lower": pl.Series(dtype=pl.Float64),
                            "Rank": pl.Series(dtype=pl.UInt32)
})

##################################
#########Review Process###########
##################################

Output_Standard_Index = pl.DataFrame({
    "Date": pl.Series([], dtype=pl.Date),
    "ENTITY_QID": pl.Series([], dtype=pl.Utf8),
    "Country": pl.Series([], dtype=pl.Utf8),
    "Size": pl.Series([], dtype=pl.Utf8),
    "Case": pl.Series([], dtype=pl.Utf8)
})

Output_Count_Standard_Index = pl.DataFrame({
    "Country": pl.Series([], dtype=pl.Utf8),
    "Count": pl.Series([], dtype=pl.UInt32),
    "Date": pl.Series([], dtype=pl.Date),
})

Screened_Securities = pl.DataFrame({
                                    "Date": pl.Series([], dtype=pl.Date),
                                    "Internal_Number": pl.Series([], dtype=pl.Utf8),
                                    "Segment": pl.Series([], dtype=pl.Utf8),
                                    "Country": pl.Series([], dtype=pl.Utf8)})

EMS_Frame = pl.DataFrame({
                        "Date": pl.Series([], dtype=pl.Date),
                        "Segment": pl.Series([], dtype=pl.Utf8),
                        "EMS": pl.Series([], dtype=pl.Float64),
                        "Rank": pl.Series([], dtype=pl.Int64),
                        "FreeFloatMCAP_Minimum_Size": pl.Series([], dtype=pl.Float64)
})

Standard_Index = pl.DataFrame({
    "Date": pl.Series([], dtype=pl.Date),
    "Internal_Number": pl.Series([], dtype=pl.Utf8),
    "Instrument_Name": pl.Series([], dtype=pl.Utf8),
    "ENTITY_QID": pl.Series([], dtype=pl.Utf8),
    "Country": pl.Series([], dtype=pl.Utf8),
    "Size": pl.Series([], dtype=pl.Utf8),
    "Shadow_Company": pl.Series([], dtype=pl.Boolean)
})

Small_Index = pl.DataFrame({
    "Date": pl.Series([], dtype=pl.Date),
    "Internal_Number": pl.Series([], dtype=pl.Utf8),
    "Instrument_Name": pl.Series([], dtype=pl.Utf8),
    "ENTITY_QID": pl.Series([], dtype=pl.Utf8),
    "Country": pl.Series([], dtype=pl.Utf8),
    "Size": pl.Series([], dtype=pl.Utf8),
    "Shadow_Company": pl.Series([], dtype=pl.Boolean)
})

with pd.ExcelWriter(Output_File, engine='xlsxwriter') as writer:
    for date in Emerging.select(["Date"]).unique().sort("Date").to_series():

        # Status
        print(date)

        # Keep only a slice of Frame with the current Date
        temp_Emerging = Emerging.filter(pl.col("Date") == date)
        temp_Developed = Developed.filter(pl.col("Date") == date)

        # First Review Date where Index is created
        if date == Starting_Date: 

            ###################################
            ##########Apply EMS Screen#########
            ###################################

            temp_Developed, EMS_Frame = Equity_Minimum_Size(temp_Developed, Pivot_TOR, EMS_Frame, date, "Developed")
            temp_Developed = temp_Developed.filter(pl.col("Free_Float_MCAP_USD_Cutoff") > EMS_Frame.filter(pl.col("Date") == date).select(
                pl.col("FreeFloatMCAP_Minimum_Size")).to_numpy()[0][0]).select(pl.col(["Date", "ENTITY_QID", "Country", "Internal_Number",
                "Instrument_Name", "Free_Float", "Capfactor", "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff"]))
            
            temp_Emerging, EMS_Frame = Equity_Minimum_Size(temp_Emerging, Pivot_TOR, EMS_Frame, date, "Emerging")
            temp_Emerging = temp_Emerging.filter(pl.col("Free_Float_MCAP_USD_Cutoff") > EMS_Frame.filter(pl.col("Date") == date).select(
                pl.col("FreeFloatMCAP_Minimum_Size")).to_numpy()[0][0]).select(pl.col(["Date", "ENTITY_QID", "Country", "Internal_Number",
                "Instrument_Name", "Free_Float", "Capfactor", "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff"]))

            ###################################
            ######TurnoverRatio Screening######
            ###################################

           # Apply the Check on Turnover for all Components
            Developed_Screened = Turnover_Check(temp_Developed, Pivot_TOR, Threshold_NEW, Threshold_OLD, date, Starting_Date)
            Emerging_Screened = Turnover_Check(temp_Emerging, Pivot_TOR, Threshold_NEW, Threshold_OLD, date, Starting_Date)

            # Remove Securities not passing the screen
            temp_Developed = temp_Developed.join(Developed_Screened, on=["Internal_Number"], how="left").filter(pl.col("Status_TOR") == True)
            temp_Emerging = temp_Emerging.join(Emerging_Screened, on=["Internal_Number"], how="left").filter(pl.col("Status_TOR") == True)

            ###################################
            #########FOR FF Screening##########
            ###################################

            # Filter for FOR_FF >= FOR_FF
            temp_Developed = temp_Developed.with_columns(
                                (pl.col("Free_Float") * pl.col("Capfactor")).alias("FOR_FF")
                                ).filter(pl.col("FOR_FF") >= FOR_FF_Screen)
            temp_Emerging = temp_Emerging.with_columns(
                                (pl.col("Free_Float") * pl.col("Capfactor")).alias("FOR_FF")
                                ).filter(pl.col("FOR_FF") >= FOR_FF_Screen)

            ##################################
            #Store Securities Passing Screens#
            ##################################

            Screened_Securities = Screened_Securities.vstack(temp_Developed.with_columns(pl.lit("Developed").alias("Segment")).select(Screened_Securities.columns))
            Screened_Securities = Screened_Securities.vstack(temp_Emerging.with_columns(pl.lit("Emerging").alias("Segment")).select(Screened_Securities.columns))

            ##################################
            #######Aggregate Companies########
            ##################################

            #Re-integrate the Full_MCAP_USD_Cutoff for those Securities that have been excluded
            Original_MCAP_Emerging = Emerging.filter(pl.col("Date") == date).group_by("ENTITY_QID").agg(pl.col("Full_MCAP_USD_Cutoff").sum().alias("Full_MCAP_USD_Cutoff_Company"))
            Original_MCAP_Developed = Developed.filter(pl.col("Date") == date).group_by("ENTITY_QID").agg(pl.col("Full_MCAP_USD_Cutoff").sum().alias("Full_MCAP_USD_Cutoff_Company"))

            temp_Developed_Aggregate = temp_Developed.filter(pl.col("Date") == date).select(pl.col(["Date", "Internal_Number", "Instrument_Name", "ENTITY_QID", "Country", "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff"])).group_by(
                                                    ["Date", "ENTITY_QID"]).agg([
                                                        pl.col("Country").first().alias("Country"),
                                                        pl.col("Internal_Number").first().alias("Internal_Number"),
                                                        pl.col("Instrument_Name").first().alias("Instrument_Name"),
                                                        pl.col("Free_Float_MCAP_USD_Cutoff").sum().alias("Free_Float_MCAP_USD_Cutoff_Company")
                                                    ]).join(Original_MCAP_Developed, on=["ENTITY_QID"], how="left").sort(["Date", "Full_MCAP_USD_Cutoff_Company"], descending = True)

            temp_Emerging_Aggregate = temp_Emerging.filter(pl.col("Date") == date).select(pl.col(["Date", "Internal_Number", "Instrument_Name", "ENTITY_QID", "Country", "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff"])).group_by(
                                                    ["Date", "ENTITY_QID"]).agg([
                                                        pl.col("Country").first().alias("Country"),
                                                        pl.col("Internal_Number").first().alias("Internal_Number"),
                                                        pl.col("Instrument_Name").first().alias("Instrument_Name"),
                                                        pl.col("Free_Float_MCAP_USD_Cutoff").sum().alias("Free_Float_MCAP_USD_Cutoff_Company")
                                                    ]).join(Original_MCAP_Emerging, on=["ENTITY_QID"], how="left").sort(["Date", "Full_MCAP_USD_Cutoff_Company"], descending = True)

            #################################
            #########GMSR Calculation########
            #################################

            temp_Developed_Aggregate = temp_Developed_Aggregate.sort(["Full_MCAP_USD_Cutoff_Company"], descending=True)
            temp_Developed_Aggregate = temp_Developed_Aggregate.with_columns(
                                                            (pl.col("Free_Float_MCAP_USD_Cutoff_Company") / pl.col("Free_Float_MCAP_USD_Cutoff_Company").sum()).alias("Weight_Cutoff")
            )

            temp_Developed_Aggregate = temp_Developed_Aggregate.with_columns(
                                                            (pl.col("Weight_Cutoff").cum_sum()).alias("CumWeight_Cutoff"),
                                                            (-pl.col("Full_MCAP_USD_Cutoff_Company")).rank("dense").alias("Rank")
            )

            # Check if the MSCI_GMSR is between 99.5% and 100%
            if GMSR_Upper_Buffer <= temp_Developed_Aggregate.filter(pl.col("Full_MCAP_USD_Cutoff_Company") <= GMSR_MSCI).head(1).select(pl.col("CumWeight_Cutoff")).to_numpy()[0][0] <= GMSR_Lower_Buffer:
                New_Data = pl.DataFrame({
                                            "Date": [date],
                                            "GMSR_Developed": [GMSR_MSCI],
                                            "GMSR_Developed_Upper": [GMSR_MSCI * Upper_Limit],
                                            "GMSR_Developed_Lower": [GMSR_MSCI * Lower_Limit], 
                                            "GMSR_Emerging": [GMSR_MSCI / 2],
                                            "GMSR_Emerging_Upper": [GMSR_MSCI / 2 * Upper_Limit],
                                            "GMSR_Emerging_Lower": [GMSR_MSCI / 2 * Lower_Limit],
                                            "Rank": [temp_Developed_Aggregate.filter(pl.col("Full_MCAP_USD_Cutoff_Company") <= GMSR_MSCI).head(1).select(pl.col("Rank")).to_numpy()[0][0]]
                })

            elif temp_Developed_Aggregate.filter(pl.col("Full_MCAP_USD_Cutoff_Company") <= GMSR_MSCI).head(1).select(pl.col("CumWeight_Cutoff")).to_numpy()[0][0] > GMSR_Lower_Buffer:
                New_Data = pl.DataFrame({
                                            "Date": [date],
                                            "GMSR_Developed": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") < GMSR_Lower_Buffer).tail(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0]],
                                            "GMSR_Developed_Upper": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") < GMSR_Lower_Buffer).tail(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] * Upper_Limit],
                                            "GMSR_Developed_Lower": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") < GMSR_Lower_Buffer).tail(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] * Lower_Limit], 
                                            "GMSR_Emerging": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") < GMSR_Lower_Buffer).tail(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2],
                                            "GMSR_Emerging_Upper": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") < GMSR_Lower_Buffer).tail(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2 * Upper_Limit],
                                            "GMSR_Emerging_Lower": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") < GMSR_Lower_Buffer).tail(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2 * Lower_Limit],
                                            "Rank": [
                                                        temp_Developed_Aggregate
                                                        .filter(pl.col("Full_MCAP_USD_Cutoff_Company") == 
                                                            temp_Developed_Aggregate
                                                            .filter(pl.col("CumWeight_Cutoff") < GMSR_Lower_Buffer)
                                                            .tail(1)["Full_MCAP_USD_Cutoff_Company"]
                                                            .to_numpy()[0]
                                                        )
                                                        .head(1)["Rank"]
                                                        .to_numpy()[0]
                                                    ]

                })

            elif temp_Developed_Aggregate.filter(pl.col("Full_MCAP_USD_Cutoff_Company") <= GMSR_MSCI).head(1).select(pl.col("CumWeight_Cutoff")).to_numpy()[0][0] < GMSR_Upper_Buffer:
                New_Data = pl.DataFrame({
                                        "Date": [date],
                                        "GMSR_Developed": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0]],
                                        "GMSR_Developed_Upper": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] * Upper_Limit],
                                        "GMSR_Developed_Lower": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] * Lower_Limit], 
                                        "GMSR_Emerging": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2],
                                        "GMSR_Emerging_Upper": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2 * Upper_Limit],
                                        "GMSR_Emerging_Lower": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2 * Lower_Limit],
                                        "Rank": [
                                                    temp_Developed_Aggregate
                                                    .filter(pl.col("Full_MCAP_USD_Cutoff_Company") == 
                                                        temp_Developed_Aggregate
                                                        .filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer)
                                                        .head(1)["Full_MCAP_USD_Cutoff_Company"]
                                                        .to_numpy()[0]
                                                    )
                                                    .head(1)["Rank"]
                                                    .to_numpy()[0]
                                                ]
            })

            # Drop the Rank column
            temp_Developed_Aggregate = temp_Developed_Aggregate.drop("Rank")

            GMSR_Frame = GMSR_Frame.vstack(New_Data)

            #################################
            ##Start the Size Classification##
            #################################

            # Store the Screened Securities for Checks
            # Stored_Securities = temp_Emerging_Aggregate.filter(pl.col("Country") == Country_Plotting)

            # Get the GMSR
            Lower_GMSR = GMSR_Frame.select(["GMSR_Emerging_Lower", "Date"]).filter(pl.col("Date") == date).to_numpy()[0][0]
            Upper_GMSR = GMSR_Frame.select(["GMSR_Emerging_Upper", "Date"]).filter(pl.col("Date") == date).to_numpy()[0][0]

            # Filter only for Country_Plotting
            # temp_Emerging_Aggregate = temp_Emerging_Aggregate.filter(pl.col("Country") == Country_Plotting)

            # Emerging #
            for country in temp_Emerging_Aggregate.select(pl.col("Country")).unique().sort("Country").to_series():
                
                TopPercentage, temp_Country = Index_Creation_Box(temp_Emerging_Aggregate, Lower_GMSR, Upper_GMSR, country, date, Excel_Recap, Percentage, Right_Limit, Left_Limit, "Emerging", writer)

                # Apply the check on Minimum_FreeFloat_MCAP_USD_Cutoff
                TopPercentage, TopPercentage_Securities = Minimum_FreeFloat_Country(TopPercentage, temp_Country, Lower_GMSR, Upper_GMSR, date, country, "Emerging", Original_MCAP_Emerging)

                # Stack to Output_Standard_Index
                Output_Standard_Index = Output_Standard_Index.vstack(TopPercentage.select(Output_Standard_Index.columns))

                # Create the Output_Count_Standard_Index for future rebalacing
                Output_Count_Standard_Index = Output_Count_Standard_Index.vstack(TopPercentage.group_by("Country").agg(
                        pl.len().alias("Count"),
                        pl.col("Date").first().alias("Date")
                    ).sort("Count", descending=True))

                #################################
                ###########Assign Size###########
                #################################

                # Standard Index #
                Standard_Index = Standard_Index.vstack(TopPercentage_Securities.select(Standard_Index.columns))

                # Small Index #
                Emerging_Small = temp_Emerging.filter((~pl.col("Internal_Number").is_in(TopPercentage_Securities.select(pl.col("Internal_Number")))) & (
                    pl.col("Country") == country)).select(
                    pl.col(["Date", "Internal_Number", "Instrument_Name", "ENTITY_QID", "Country", "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff"])
                ).with_columns(
                    pl.lit("Small").alias("Size"),
                    pl.lit(False).alias("Shadow_Company")).select(Small_Index.columns).head(0)
                
                Small_Index = Small_Index.vstack(Emerging_Small)
            
        # Following Reviews where Index is rebalanced
        else:

            ###################################
            ##########Apply EMS Screen#########
            ###################################

            temp_Developed, EMS_Frame = Equity_Minimum_Size(temp_Developed, Pivot_TOR, EMS_Frame, date, "Developed")
            temp_Developed = temp_Developed.filter(pl.col("Free_Float_MCAP_USD_Cutoff") > EMS_Frame.filter(pl.col("Date") == date).select(
                pl.col("FreeFloatMCAP_Minimum_Size")).to_numpy()[0][0]).select(pl.col(["Date", "ENTITY_QID", "Country", "Internal_Number",
                "Instrument_Name", "Free_Float", "Capfactor", "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff"]))
            
            temp_Emerging, EMS_Frame = Equity_Minimum_Size(temp_Emerging, Pivot_TOR, EMS_Frame, date, "Emerging")
            temp_Emerging = temp_Emerging.filter(pl.col("Free_Float_MCAP_USD_Cutoff") > EMS_Frame.filter(pl.col("Date") == date).select(
                pl.col("FreeFloatMCAP_Minimum_Size")).to_numpy()[0][0]).select(pl.col(["Date", "ENTITY_QID", "Country", "Internal_Number",
                "Instrument_Name", "Free_Float", "Capfactor", "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff"]))

            ###################################
            ######TurnoverRatio Screening######
            ###################################

            # Apply the Check on Turnover for all Components
            Developed_Screened = Turnover_Check(temp_Developed, Pivot_TOR, Threshold_NEW, Threshold_OLD, date, Starting_Date)
            Emerging_Screened = Turnover_Check(temp_Emerging, Pivot_TOR, Threshold_NEW, Threshold_OLD, date, Starting_Date)

            # Remove Securities not passing the screen
            temp_Developed = temp_Developed.join(Developed_Screened, on=["Internal_Number"], how="left").filter(pl.col("Status_TOR") == True)
            temp_Emerging = temp_Emerging.join(Emerging_Screened, on=["Internal_Number"], how="left").filter(pl.col("Status_TOR") == True)

            ###################################
            #########FOR FF Screening##########
            ###################################

            # # Filter for FOR FF Screening
            temp_Developed = FOR_Sreening(temp_Developed, Developed, Pivot_TOR, Standard_Index, Small_Index, date, "Developed")
            temp_Emerging = FOR_Sreening(temp_Emerging, Emerging, Pivot_TOR, Standard_Index, Small_Index, date, "Emerging")

            ##################################
            #Store Securities Passing Screens#
            ##################################

            Screened_Securities = Screened_Securities.vstack(temp_Developed.with_columns(pl.lit("Developed").alias("Segment")).select(Screened_Securities.columns))
            Screened_Securities = Screened_Securities.vstack(temp_Emerging.with_columns(pl.lit("Emerging").alias("Segment")).select(Screened_Securities.columns))

            ##################################
            #######Aggregate Companies########
            ##################################

            #Re-integrate the Full_MCAP_USD_Cutoff for those Securities that have been excluded
            Original_MCAP_Emerging = Emerging.filter(pl.col("Date") == date).group_by("ENTITY_QID").agg(pl.col("Full_MCAP_USD_Cutoff").sum().alias("Full_MCAP_USD_Cutoff_Company"))
            Original_MCAP_Developed = Developed.filter(pl.col("Date") == date).group_by("ENTITY_QID").agg(pl.col("Full_MCAP_USD_Cutoff").sum().alias("Full_MCAP_USD_Cutoff_Company"))

            temp_Developed_Aggregate = temp_Developed.filter(pl.col("Date") == date).select(pl.col(["Date", "Internal_Number", "Instrument_Name", "ENTITY_QID", "Country", "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff"])).group_by(
                                                    ["Date", "ENTITY_QID"]).agg([
                                                        pl.col("Country").first().alias("Country"),
                                                        pl.col("Internal_Number").first().alias("Internal_Number"),
                                                        pl.col("Instrument_Name").first().alias("Instrument_Name"),
                                                        pl.col("Free_Float_MCAP_USD_Cutoff").sum().alias("Free_Float_MCAP_USD_Cutoff_Company")
                                                    ]).join(Original_MCAP_Developed, on=["ENTITY_QID"], how="left").sort(["Date", "Full_MCAP_USD_Cutoff_Company"], descending = True)

            temp_Emerging_Aggregate = temp_Emerging.filter(pl.col("Date") == date).select(pl.col(["Date", "Internal_Number", "Instrument_Name", "ENTITY_QID", "Country", "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff"])).group_by(
                                                    ["Date", "ENTITY_QID"]).agg([
                                                        pl.col("Country").first().alias("Country"),
                                                        pl.col("Internal_Number").first().alias("Internal_Number"),
                                                        pl.col("Instrument_Name").first().alias("Instrument_Name"),
                                                        pl.col("Free_Float_MCAP_USD_Cutoff").sum().alias("Free_Float_MCAP_USD_Cutoff_Company")
                                                    ]).join(Original_MCAP_Emerging, on=["ENTITY_QID"], how="left").sort(["Date", "Full_MCAP_USD_Cutoff_Company"], descending = True)

            #################################
            #########GMSR Calculation########
            #################################

            temp_Developed_Aggregate = temp_Developed_Aggregate.sort(["Full_MCAP_USD_Cutoff_Company"], descending=True)
            temp_Developed_Aggregate = temp_Developed_Aggregate.with_columns(
                                                            (pl.col("Free_Float_MCAP_USD_Cutoff_Company") / pl.col("Free_Float_MCAP_USD_Cutoff_Company").sum()).alias("Weight_Cutoff")
            )

            temp_Developed_Aggregate = temp_Developed_Aggregate.with_columns(
                                                            (pl.col("Weight_Cutoff").cum_sum()).alias("CumWeight_Cutoff"),
                                                            (-pl.col("Full_MCAP_USD_Cutoff_Company")).rank("dense").alias("Rank")
            )

            # Check if Previous Ranking Company lies between GMSR_Upper_Buffer and GMSR_Lower_Buffer
            
            # List of Unique Dates
            Dates_List = Pivot_TOR.index.to_list()
            IDX_Current = Dates_List.index(date.strftime("%Y-%m-%d"))
            Previous_Date = datetime.datetime.strptime(Dates_List[max(0, IDX_Current - 1)], "%Y-%m-%d").date()

            # Retrieve the Previous Rank in the GMSR Frame
            Previous_Rank_GMSR = GMSR_Frame.filter(pl.col("Date") == Previous_Date).select(pl.col("Rank")).to_numpy()[0][0]

            # CumWeight_Cutoff of the Previous Rank_GMSR
            CumWeight_Cutoff_Rank = temp_Developed_Aggregate.filter(pl.col("Rank") == Previous_Rank_GMSR).select(pl.col("CumWeight_Cutoff")).to_numpy()[0][0]

            if (GMSR_Upper_Buffer) <= CumWeight_Cutoff_Rank <= (GMSR_Lower_Buffer):
                New_Data = pl.DataFrame({   
                                            "Date": [date],
                                            "GMSR_Developed": [temp_Developed_Aggregate.filter(pl.col("Rank") == Previous_Rank_GMSR).select(pl.col("Full_MCAP_USD_Cutoff_Company")).to_numpy()[0][0]],
                                            "GMSR_Developed_Upper": [temp_Developed_Aggregate.filter(pl.col("Rank") == Previous_Rank_GMSR).select(pl.col("Full_MCAP_USD_Cutoff_Company")).to_numpy()[0][0] * Upper_Limit],
                                            "GMSR_Developed_Lower": [temp_Developed_Aggregate.filter(pl.col("Rank") == Previous_Rank_GMSR).select(pl.col("Full_MCAP_USD_Cutoff_Company")).to_numpy()[0][0] * Lower_Limit], 
                                            "GMSR_Emerging": [temp_Developed_Aggregate.filter(pl.col("Rank") == Previous_Rank_GMSR).select(pl.col("Full_MCAP_USD_Cutoff_Company")).to_numpy()[0][0] / 2],
                                            "GMSR_Emerging_Upper": [temp_Developed_Aggregate.filter(pl.col("Rank") == Previous_Rank_GMSR).select(pl.col("Full_MCAP_USD_Cutoff_Company")).to_numpy()[0][0] / 2 * Upper_Limit],
                                            "GMSR_Emerging_Lower": [temp_Developed_Aggregate.filter(pl.col("Rank") == Previous_Rank_GMSR).select(pl.col("Full_MCAP_USD_Cutoff_Company")).to_numpy()[0][0] / 2 * Lower_Limit],
                                            "Rank": [temp_Developed_Aggregate.filter(pl.col("Rank") == Previous_Rank_GMSR).select(pl.col("Rank")).to_numpy()[0][0]]
                                        })

            elif CumWeight_Cutoff_Rank < (GMSR_Upper_Buffer):
                New_Data = pl.DataFrame({
                                        "Date": [date],
                                        "GMSR_Developed": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0]],
                                        "GMSR_Developed_Upper": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] * Upper_Limit],
                                        "GMSR_Developed_Lower": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] * Lower_Limit], 
                                        "GMSR_Emerging": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2],
                                        "GMSR_Emerging_Upper": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2 * Upper_Limit],
                                        "GMSR_Emerging_Lower": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2 * Lower_Limit],
                                        "Rank": [
                                                    temp_Developed_Aggregate
                                                    .filter(pl.col("Full_MCAP_USD_Cutoff_Company") == 
                                                        temp_Developed_Aggregate
                                                        .filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer)
                                                        .head(1)["Full_MCAP_USD_Cutoff_Company"]
                                                        .to_numpy()[0]
                                                    )
                                                    .head(1)["Rank"]
                                                    .to_numpy()[0]
                                                ]})
                
            elif CumWeight_Cutoff_Rank > (GMSR_Lower_Buffer):
                New_Data = pl.DataFrame({
                                        "Date": [date],
                                        "GMSR_Developed": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0]],
                                        "GMSR_Developed_Upper": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] * Upper_Limit],
                                        "GMSR_Developed_Lower": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] * Lower_Limit], 
                                        "GMSR_Emerging": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2],
                                        "GMSR_Emerging_Upper": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2 * Upper_Limit],
                                        "GMSR_Emerging_Lower": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2 * Lower_Limit],
                                        "Rank": [
                                                    temp_Developed_Aggregate
                                                    .filter(pl.col("Full_MCAP_USD_Cutoff_Company") == 
                                                        temp_Developed_Aggregate
                                                        .filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer)
                                                        .head(1)["Full_MCAP_USD_Cutoff_Company"]
                                                        .to_numpy()[0]
                                                    )
                                                    .head(1)["Rank"]
                                                    .to_numpy()[0]
                                                ]})
                    
            # Drop the Rank column
            temp_Developed_Aggregate = temp_Developed_Aggregate.drop("Rank")

            GMSR_Frame = GMSR_Frame.vstack(New_Data)

            #################################
            ##Start the Size Classification##
            #################################

            # Filter only for Country_Plotting
            # temp_Emerging_Aggregate = temp_Emerging_Aggregate.filter(pl.col("Country") == Country_Plotting)

            # Emerging #
            for country in temp_Emerging_Aggregate.select(pl.col("Country")).unique().sort("Country").to_series():

                # List of Unique Dates
                Dates_List = Pivot_TOR.index.to_list()

                IDX_Current = Dates_List.index(date.strftime("%Y-%m-%d"))
                Previous_Date = datetime.datetime.strptime(Dates_List[max(0, IDX_Current - 1)], "%Y-%m-%d").date()

                Lower_GMSR = GMSR_Frame.select(["GMSR_Emerging_Lower", "Date"]).filter(pl.col("Date") == date).to_numpy()[0][0]
                Upper_GMSR = GMSR_Frame.select(["GMSR_Emerging_Upper", "Date"]).filter(pl.col("Date") == date).to_numpy()[0][0]

                # Check if there is already a previous Index creation for the current country
                if len(Output_Count_Standard_Index.filter((pl.col("Country") == country) & (pl.col("Date") == Previous_Date))) > 0:

                    TopPercentage, temp_Country = Index_Rebalancing_Box(temp_Emerging_Aggregate, SW_ACALLCAP, Output_Count_Standard_Index, Lower_GMSR, Upper_GMSR, country, date, Excel_Recap,  Right_Limit, Left_Limit, "Emerging" ,writer)

                    # Apply the check on Minimum_FreeFloat_MCAP_USD_Cutoff
                    TopPercentage, TopPercentage_Securities = Minimum_FreeFloat_Country(TopPercentage, temp_Country, Lower_GMSR, Upper_GMSR, date, country, "Emerging", Original_MCAP_Emerging)

                    if Excel_Recap_Rebalancing == True and country == Country_Plotting:

                        # Save DataFrame to Excel
                        TopPercentage.to_pandas().to_excel(writer, sheet_name=f'{date}_{country}', index=False)
                        # Create and save the chart
                        chart_file = Curve_Plotting(TopPercentage, temp_Country, Lower_GMSR, Upper_GMSR)

                        # Insert the chart into the Excel file
                        workbook = writer.book
                        worksheet = writer.sheets[f'{date}_{country}']
                        worksheet.insert_image('O2', chart_file)

                        # Check that minimum number is respected

                    # Stack to Output_Standard_Index
                    Output_Standard_Index = Output_Standard_Index.vstack(TopPercentage.select(Output_Standard_Index.columns))
                    
                    # Create the Output_Count_Standard_Index for future rebalacing
                    Output_Count_Standard_Index = Output_Count_Standard_Index.vstack(TopPercentage.group_by("Country").agg(
                        pl.len().alias("Count"),
                        pl.col("Date").first().alias("Date")
                    ).sort("Count", descending=True))
                
                # If there is no composition, a new Index will be created
                else:
                    TopPercentage, temp_Country = Index_Creation_Box(temp_Emerging_Aggregate, Lower_GMSR, Upper_GMSR, country, date, Excel_Recap, Percentage, Right_Limit, Left_Limit, "Emerging", writer)
                    
                    # Apply the check on Minimum_FreeFloat_MCAP_USD_Cutoff
                    TopPercentage, TopPercentage_Securities = Minimum_FreeFloat_Country(TopPercentage, temp_Country, Lower_GMSR, Upper_GMSR, date, country, "Emerging", Original_MCAP_Emerging)

                    if Excel_Recap_Rebalancing == True and country == Country_Plotting:

                        # Save DataFrame to Excel
                        TopPercentage.to_pandas().to_excel(writer, sheet_name=f'{date}_{country}', index=False)
                        # Create and save the chart
                        chart_file = Curve_Plotting(TopPercentage, temp_Country, Lower_GMSR, Upper_GMSR)

                        # Insert the chart into the Excel file
                        workbook = writer.book
                        worksheet = writer.sheets[f'{date}_{country}']
                        worksheet.insert_image('O2', chart_file)

                    # Stack to Output_Standard_Index
                    Output_Standard_Index = Output_Standard_Index.vstack(TopPercentage.select(Output_Standard_Index.columns))
                    
                    # Create the Output_Count_Standard_Index for future rebalacing
                    Output_Count_Standard_Index = Output_Count_Standard_Index.vstack(TopPercentage.group_by("Country").agg(
                        pl.len().alias("Count"),
                        pl.col("Date").first().alias("Date")
                    ).sort("Count", descending=True))

                #################################
                ###########Assign Size###########
                #################################

                # Standard Index #
                Standard_Index = Standard_Index.vstack(TopPercentage_Securities.select(Standard_Index.columns))

                # Small Index #
                Emerging_Small = temp_Emerging.filter((~pl.col("Internal_Number").is_in(TopPercentage_Securities.select(pl.col("Internal_Number")))) & (
                    pl.col("Country") == country)).select(
                    pl.col(["Date", "Internal_Number", "Instrument_Name", "ENTITY_QID", "Country", "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff"])
                ).with_columns(
                    pl.lit("Small").alias("Size"),
                    pl.lit(False).alias("Shadow_Company")).select(Small_Index.columns).head(0)
                
                Small_Index = Small_Index.vstack(Emerging_Small)

    # Add Recap_GMSR_Frame to Excel
    GMSR_Frame.to_pandas().to_excel(writer, sheet_name="GMSR_Historical", index=False)

### Standard Index ###

# Implement to remove Shadow Company here #
Standard_Index = Standard_Index.filter(pl.col("Shadow_Company")==False)

# Add SEDOL/ISIN
Standard_Index = Standard_Index.join(Emerging.select(pl.col(["Date", "Internal_Number", "ISIN", "SEDOL"])), on=["Date", "Internal_Number"], how="left")

# Add information of CapFactor/Mcap_Units_Index_Currency
Standard_Index = Standard_Index.join(pl.read_parquet(
    r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Universe\STXWAGV_Review.parquet").with_columns(
        pl.col("Date").cast(pl.Date),
        pl.col("Mcap_Units_Index_Currency").cast(pl.Float64)
    ), on=["Date", "Internal_Number"], how="left")

# Calculate the Weights for each Date
Standard_Index = Standard_Index.with_columns(
    (pl.col("Mcap_Units_Index_Currency") / pl.col("Mcap_Units_Index_Currency").sum().over("Date")).alias("Weight")
)

### Small Index ###

# Add SEDOL/ISIN
Small_Index = Small_Index.join(Emerging.select(pl.col(["Date", "Internal_Number", "ISIN", "SEDOL"])), on=["Date", "Internal_Number"], how="left")

# Add information of CapFactor/Mcap_Units_Index_Currency
Small_Index = Small_Index.join(pl.read_parquet(
    r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Universe\STXWAGV_Review.parquet").with_columns(
        pl.col("Date").cast(pl.Date),
        pl.col("Mcap_Units_Index_Currency").cast(pl.Float64)
    ), on=["Date", "Internal_Number"], how="left")

# Remove China A / CCS
Small_Index = Small_Index.filter(
    ~(
        (pl.col("Country") == "CN") &
        (
            pl.col("Instrument_Name").str.contains("'A'") |
            pl.col("Instrument_Name").str.contains("(CCS)")
        )))

# Calculate the Weights for each Date
Small_Index = Small_Index.with_columns(
    (pl.col("Mcap_Units_Index_Currency") / pl.col("Mcap_Units_Index_Currency").sum().over("Date")).alias("Weight")
)

# Create a Recap
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

# Recap Standard Index
Recap_Count_Standard = (
    Standard_Index
    .group_by(["Country", "Date"])  # Group by Country and Date
    .agg(pl.col("Internal_Number").count().alias("Sum_Components"))  # Count "Count" column and alias it
    .sort("Date")  # Ensure sorting by Date for proper column ordering in the pivot
    .pivot(
        index="Country",  # Set Country as the row index
        on="Date",        # Create columns for each unique Date
        values="Sum_Components"  # Fill in values with Sum_Components
    )
)

Recap_Weight_Standard = (
    Standard_Index
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

# Store the Results
Standard_Index.write_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Output\Tests\AllCap_Index_Security_Level_{GMSR_Upper_Buffer}_{GMSR_Lower_Buffer}_" + current_datetime + ".csv")
# Recap_Count_Standard.write_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Output\Test\Recap_Count_AllCap_{Percentage}_ETF_Version_Coverage_Adjustment_{Coverage_Adjustment}_" + current_datetime + ".csv")
# Recap_Weight_Standard.write_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Output\Test\Recap_Weight_AllCap_{Percentage}_ETF_Version_Coverage_Adjustment_{Coverage_Adjustment}_" + current_datetime + ".csv")
# GMSR_Frame.write_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Output\Test\GMSR_Frame_{Percentage}_ETF_Version_Coverage_Adjustment_{Coverage_Adjustment}_" + current_datetime + ".csv")
# EMS_Frame.write_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Output\Test\EMS_Frame_{Percentage}_ETF_Version_Coverage_Adjustment_{Coverage_Adjustment}_" + current_datetime + ".csv")

# Delete .PNG from main folder
Main_path = r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO"

# Use glob to find all PNG files in the folder
PNG_Files = glob.glob(os.path.join(Main_path, '*.PNG'))

# Iterate over the list of PNG files and remove them
for file in PNG_Files:
    try:
        os.remove(file)
        print(f"Deleted: {file}")
    except Exception as e:
        print(f"Error deleting file {file}: {e}")

end_time = time.time()
execution_time = end_time - start_time
print(f"Execution time: {execution_time} seconds")