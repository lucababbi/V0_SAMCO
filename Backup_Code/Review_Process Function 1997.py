import polars as pl
import pandas as pd
from datetime import date
import matplotlib.pyplot as plt
from pandasql import sqldf
import datetime
import glob
import os
import numpy as np

##################################
###########Parameters#############
##################################
Starting_Date = date(1997, 3, 24)
Upper_Limit = 1.15
Lower_Limit = 0.50
Percentage = 0.85
Left_Limit = Percentage - 0.05
Right_Limit = Percentage +  0.05
Threshold_NEW = 0.15
Threshold_OLD = 0.10
FOR_FF_Screen = 0.15
Screen_TOR = True

# MSCI GMSR Mar_2019
GMSR_Upper_Buffer = 0.85
GMSR_Lower_Buffer = 0.87
GMSR_Percentage = 0.85
GMSR_MSCI = np.float64(6_203 * 1_000_000)

# Index Creation Country Coverage Adjustment
Coverage_Adjustment = True

# Excel Setter
Excel_Recap = False
Excel_Recap_Rebalancing = False

Country_Plotting = "KR"
Output_File = rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Output\TopPercentage_Report_Rebalancing_{Country_Plotting}.xlsx"

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
                Country_Cutoff = Investable_Index.sort("Full_MCAP_USD_Cutoff_Company", descending=True).row(Previous_Count)[Investable_Index.columns.index("Full_MCAP_USD_Cutoff_Company")] / 2

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
                            "Capfactor_CN": pl.Series(dtype=pl.Float64)
                            })

    for Date in Frame.select(["Date"]).unique().sort("Date").to_series():
        # Filter for the given date
        temp_Frame = Frame.filter(pl.col("Date") == Date)

        if Date <= datetime.date(2022,3,21):
            Chinese_Securities = temp_Frame.filter(
                                (
                                    (pl.col("Country") == "CN") &
                                    (
                                        pl.col("Instrument_Name").str.contains("'A'") |
                                        pl.col("Instrument_Name").str.contains("(CCS)")
                                    ))
                            ).select(pl.col(["Date", "Internal_Number", "Capfactor"])).with_columns(
                                        (pl.col("Capfactor") / 2).alias("Capfactor_CN")
                                    )
        else:
            Chinese_Securities = temp_Frame.filter(
                                        (pl.col("Exchange") != 'Stock Exchange of Hong Kong - SSE Securities') &
                                        (pl.col("Exchange") != 'Stock Exchange of Hong Kong - SZSE Securities')
                                    ).select(pl.col(["Date", "Internal_Number", "Capfactor"])).with_columns(
                                        (pl.col("Capfactor") / 2).alias("Capfactor_CN")
                                    )
        
        Results = Results.vstack(Chinese_Securities)
            
    return pl.DataFrame(Results)

##################################
#######Quarterly Turnover#########
##################################

def Turnover_Check(Frame: pl.DataFrame, Pivot_TOR: pl.DataFrame, Threshold_NEW, Threshold_OLD, date, Starting_Date, Screened_Securities) -> pl.DataFrame:

    # List of Unique Dates
    Dates_List = Pivot_TOR.index.to_list()

    # Output
    Results = []
    Status = {}

    for Row in Frame.iter_rows(named=True):
        Date = Row["Date"].strftime("%Y-%m-%d")
        Internal_Number = Row["Internal_Number"]

        # Ensure the Internal_Number exists in Pivot_TOR's columns
        if Internal_Number not in Pivot_TOR.columns:
            continue

        # Find the index of Date in Pivot_TOR
        try:
            IDX_Current = Dates_List.index(Date)
        except ValueError:
            # If the date is not found, skip this row
            continue

        # Get the previous three Dates and Current Date
        Relevant_Dates = pl.Series(Dates_List[max(0, IDX_Current - 3): IDX_Current + 1])

        # Extract the Turnover_Ratio values for the relevant dates and Internal_Number
        TOR_Values = Pivot_TOR.loc[Relevant_Dates, Internal_Number].to_list()

        # Determine the Threshold
        if date == Starting_Date:
            Threshold = Threshold_NEW
        else:
            Threshold = Threshold_OLD if len(Screened_Securities.filter((pl.col("Internal_Number") == Internal_Number) & (pl.col("Date") == date))) == 0 else Threshold_NEW

        # Check if all values are above the threshold
        if min(TOR_Values) >= Threshold:
            Results.append({"Date": Date, "Internal_Number": Internal_Number, "Status_TOR": "Pass"})
            Status[Internal_Number] = True  # Mark as passed
        else:
            Results.append({"Date": Date, "Internal_Number": Internal_Number, "Status_TOR": "Fail"})
            Status[Internal_Number] = False  # Mark as failed

    # Return results as Polars
    return pl.DataFrame(Results)

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
                
                if 0.99 <= previous_coverage <= 0.9925:
                    equity_universe_min_size = previous_row[df_date.columns.index("Full_MCAP_USD_Cutoff_Company")]
                    df_date1 = df_date.filter(pl.col("Full_MCAP_USD_Cutoff_Company") >= equity_universe_min_size).with_columns([
                        pl.lit(equity_universe_min_size).alias("EUMSR"),
                        pl.lit(previous_rank).alias("EUMSR_Rank")
                    ])
                elif previous_coverage < 0.99:
                    min_size_company = df_date.filter(pl.col("Cumulative_Coverage_Cutoff") >= 0.99).select("Full_MCAP_USD_Cutoff_Company").head(1)
                    equity_universe_min_size = min_size_company[0, 0]
                    previous_rank = df_date.filter(pl.col("Full_MCAP_USD_Cutoff_Company") >= equity_universe_min_size).height

                    df_date1 = df_date.filter(pl.col("Full_MCAP_USD_Cutoff_Company") >= equity_universe_min_size).with_columns([
                        pl.lit(equity_universe_min_size).alias("EUMSR"),
                        pl.lit(previous_rank).alias("EUMSR_Rank")
                    ])
                else:
                    min_size_company = df_date.filter(pl.col("Cumulative_Coverage_Cutoff") >= 0.9925).select("Full_MCAP_USD_Cutoff_Company").head(1)
                    equity_universe_min_size = min_size_company[0, 0]
                    previous_rank = df_date.filter(pl.col("Full_MCAP_USD_Cutoff_Company") >= equity_universe_min_size).height

                    df_date1 = df_date.filter(pl.col("Full_MCAP_USD_Cutoff_Company") >= equity_universe_min_size).with_columns([
                        pl.lit(equity_universe_min_size).alias("EUMSR"),
                        pl.lit(previous_rank).alias("EUMSR_Rank")
                    ])

            else:
                min_size_company = df_date.filter(pl.col("Cumulative_Coverage_Cutoff") >= 0.99).select("Full_MCAP_USD_Cutoff_Company").head(1)
                equity_universe_min_size = min_size_company[0, 0]
                previous_rank = df_date.filter(pl.col("Full_MCAP_USD_Cutoff_Company") >= equity_universe_min_size).height

                df_date1 = df_date.filter(pl.col("Full_MCAP_USD_Cutoff_Company") >= equity_universe_min_size).with_columns([
                        pl.lit(equity_universe_min_size).alias("EUMSR"),
                        pl.lit(previous_rank).alias("EUMSR_Rank")
                    ])

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

        # Filter for Securities whose Free_Float_MCAP_USD_Cutoff is >= than FreeFloatMCAP_Minimum_Size
        Security_Level_DF = Security_Level_DF.filter(pl.col("Free_Float_MCAP_USD_Cutoff") >= EMS_Frame.filter(
                            (pl.col("Date") == date) & (pl.col("Segment") == Segment)).select(pl.col("FreeFloatMCAP_Minimum_Size")).to_numpy()[0][0])
        
    elif Segment == "Emerging":

        equity_universe_min_size = EMS_Frame.filter(pl.col("Date") == date).select(pl.col("EMS")).to_numpy()[0][0]
        FreeFloatMCAP_Minimum_Size = EMS_Frame.filter(pl.col("Date") == date).select(pl.col("FreeFloatMCAP_Minimum_Size")).to_numpy()[0][0]
        final_df = df.filter((pl.col("Full_MCAP_USD_Cutoff_Company") >= equity_universe_min_size) & (pl.col("Free_Float_MCAP_USD_Cutoff_Company") >= FreeFloatMCAP_Minimum_Size))

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
    ### To be verified ###

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
                if TopPercentage.head(len(TopPercentage) - Companies_To_Delete).tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0] >= Lower_GMSR:
                    # If CumWeight_Cutoff is still above or equal to Left_Limit, proceed
                    TopPercentage = TopPercentage.head(len(TopPercentage) - Companies_To_Delete)
                    break  # Break the loop once the condition is met

                else:
                    # Try to increase Companies_To_Delete by 1 (up to the rounded 20% cap)
                    Companies_To_Delete += 1

    else:
        TopPercentage = TopPercentage.with_columns(
                                    pl.lit("Number of Companies equal to 1 - No Deletion").alias("Case")
        )

    return TopPercentage
    
##################################
##Minimum FreeFloatCountry Level##
##################################

def Minimum_FreeFloat_Country(TopPercentage, Lower_GMSR, Upper_GMSR):
    # Check if last Company Full_MCAP_USD_Cutoff_Company is in between Upper and Lower GMSR

    # Case inside the box
    if (TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0] <= Upper_GMSR) & (TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0] >= Lower_GMSR):
    
        # Country_GMSR is the Full_MCAP_USD_Cutoff_Company / 2
        Country_GMSR = TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0] / 2

        # Check which Companies are below the Country_GMSR
        TopPercentage = TopPercentage.with_columns(
            pl.when(pl.col("Free_Float_MCAP_USD_Cutoff_Company") < Country_GMSR)
            .then(True)
            .otherwise(None)
            .alias("Shadow_Company")
        )

    # Case above the box
    elif (TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0] > Upper_GMSR):

        # Country_GMSR is the Upper_GMSR / 2
        Country_GMSR = Upper_GMSR / 2

        # Check which Companies are below the Country_GMSR
        TopPercentage = TopPercentage.with_columns(
            pl.when(pl.col("Free_Float_MCAP_USD_Cutoff_Company") < Country_GMSR)
            .then(True)
            .otherwise(None)
            .alias("Shadow_Company")
        )

    # Case below the box
    else:

        # Country_GMSR is the GMSR / 2
        Country_GMSR = GMSR_Frame.filter(pl.col("Date") == date).select(pl.col("GMSR_Emerging")).to_numpy()[0][0] / 2

        # Check which Companies are below the Country_GMSR
        TopPercentage = TopPercentage.with_columns(
            pl.when(pl.col("Free_Float_MCAP_USD_Cutoff_Company") < Country_GMSR)
            .then(True)
            .otherwise(None)
            .alias("Shadow_Company")
        )

    # Return the Frame
    return TopPercentage

##################################
##########Index Creation##########
##################################

def Index_Creation_Box(temp_Emerging_Aggregate, Lower_GMSR, Upper_GMSR, country, date, Excel_Recap, Percentage, Right_Limit, Left_Limit, writer):

    temp_Country = temp_Emerging_Aggregate.filter((pl.col("Date") == date) & (pl.col("Country") == country))

    # Sort in each Country the Companies by Full MCAP USD Cutoff
    temp_Country = temp_Country.sort("Full_MCAP_USD_Cutoff_Company", descending=True)

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

    # Check where the top 85% (crossing it) lands us on the Curve
    TopPercentage = temp_Country.select(["Date", "Internal_Number", "Instrument_Name", "ENTITY_QID", "Country", "Free_Float_MCAP_USD_Cutoff_Company",
                    "Full_MCAP_USD_Cutoff_Company", "Weight_Cutoff", "CumWeight_Cutoff"]).filter(
                    pl.col("CumWeight_Cutoff") < (Percentage / Country_Adjustment)).vstack(temp_Country.select(["Date", "Internal_Number", "Instrument_Name", "ENTITY_QID", "Country", 
                    "Free_Float_MCAP_USD_Cutoff_Company", "Full_MCAP_USD_Cutoff_Company", "Weight_Cutoff", "CumWeight_Cutoff"]).filter(pl.col("CumWeight_Cutoff") >= (Percentage / Country_Adjustment)).head(1))
    
    #################
    # Case Analysis #
    #################

    # Best case where we land inside the box # 
    if (TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0] >= Lower_GMSR) & (TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0] <= Upper_GMSR):

        # Check how different is the CumWeight from Target Coverage
        TopPercentage = TopPercentage.with_columns(
                                    (abs(pl.col("CumWeight_Cutoff") - Percentage)).alias("CumWeight_Cutoff_Difference")
        )

        # Check if CumWeight of the Company right across the Upper GMSR is > 90%
        if TopPercentage.tail(1).select(pl.col("CumWeight_Cutoff")).to_numpy()[0][0] > Right_Limit:
            next

        # If CumWeight of the Company right across the Upper GMSR is <= 90%:
        elif TopPercentage.tail(1).select(pl.col("CumWeight_Cutoff")).to_numpy()[0][0] <= Right_Limit:
            # Filter by keeping only the closest CumWeight near to Percentage
            TopPercentage = TopPercentage.head(TopPercentage["CumWeight_Cutoff_Difference"].arg_min() + 1)

        TopPercentage = TopPercentage.with_columns(
                                    pl.lit("Standard").alias("Size")
                            ).drop("CumWeight_Cutoff_Difference")
        
        TopPercentage = TopPercentage.with_columns(
                                    pl.lit("Inside").alias("Case")
        )

        if Excel_Recap == True and country == Country_Plotting:

            # Save DataFrame to Excel
            TopPercentage.to_pandas().to_excel(writer, sheet_name=f'{date}_{country}', index=False)
            # Create and save the chart
            chart_file = Curve_Plotting(TopPercentage, temp_Country, Lower_GMSR, Upper_GMSR)

            # Insert the chart into the Excel file
            workbook = writer.book
            worksheet = writer.sheets[f'{date}_{country}']
            worksheet.insert_image('O2', chart_file)

    # Case where we land below the box
    elif (TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0] < Lower_GMSR):
        # Keep only Companies whose Full_MCAP_USD_Cutoff_Company is at least equal or higher than Lowe GMSR
        TopPercentage = TopPercentage.filter(pl.col("Full_MCAP_USD_Cutoff_Company") >= Lower_GMSR)

        # In this case we do not care about the CumWeight_Cutoff
        TopPercentage = TopPercentage.with_columns(
                                    pl.lit("Standard").alias("Size")
                            )
        
        TopPercentage = TopPercentage.with_columns(
                                    pl.lit("Below").alias("Case")
        )

        if Excel_Recap == True and country == Country_Plotting:

            # Save DataFrame to Excel
            TopPercentage.to_pandas().to_excel(writer, sheet_name=f'{date}_{country}', index=False)
            # Create and save the chart
            chart_file = Curve_Plotting(TopPercentage, temp_Country, Lower_GMSR, Upper_GMSR)

            # Insert the chart into the Excel file
            workbook = writer.book
            worksheet = writer.sheets[f'{date}_{country}']
            worksheet.insert_image('O2', chart_file)

    # Case where we land above the box
    elif (TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0] > Upper_GMSR):

        # Check if there are still Companies in between the Upper and Lower GMSR
        if len(temp_Country.filter((pl.col("Full_MCAP_USD_Cutoff_Company") <= Upper_GMSR) & (pl.col("Full_MCAP_USD_Cutoff_Company") >= Lower_GMSR))) > 0:

            TopPercentage_Extension = temp_Country.filter(pl.col("Full_MCAP_USD_Cutoff_Company") >= Upper_GMSR).sort("Full_MCAP_USD_Cutoff_Company",
                descending=True).filter(~pl.col("Internal_Number").is_in(TopPercentage.select(pl.col("Internal_Number")))).select(
                    ["Date", "Internal_Number", "Instrument_Name", "ENTITY_QID", "Country", "Free_Float_MCAP_USD_Cutoff_Company", "Full_MCAP_USD_Cutoff_Company", 
                    "Weight_Cutoff", "CumWeight_Cutoff"]).vstack(temp_Country.filter(pl.col("Full_MCAP_USD_Cutoff_Company") < Upper_GMSR).sort("Full_MCAP_USD_Cutoff_Company",
                descending=True).filter(~pl.col("Internal_Number").is_in(TopPercentage.select(pl.col("Internal_Number")))).select(
                    ["Date", "Internal_Number", "Instrument_Name", "ENTITY_QID", "Country", "Free_Float_MCAP_USD_Cutoff_Company",
                    "Full_MCAP_USD_Cutoff_Company", "Weight_Cutoff", "CumWeight_Cutoff"]).head(1)
                    )

            TopPercentage = TopPercentage.with_columns(
                                    pl.lit("Standard").alias("Size")
                            )
            
            TopPercentage_Extension = TopPercentage_Extension.with_columns(
                                    pl.lit("Standard").alias("Size")
                            )
            
            # Merge the initial Frame with the additions
            TopPercentage = TopPercentage.vstack(TopPercentage_Extension)

            TopPercentage = TopPercentage.with_columns(
                                    pl.lit("Above - Companies in between Upper and Lower GMSR").alias("Case")
                    )

            if Excel_Recap == True and country == Country_Plotting:

                # Save DataFrame to Excel
                TopPercentage.to_pandas().to_excel(writer, sheet_name=f'{date}_{country}', index=False)
                # Create and save the chart
                chart_file = Curve_Plotting(TopPercentage, temp_Country, Lower_GMSR, Upper_GMSR)

                # Insert the chart into the Excel file
                workbook = writer.book
                worksheet = writer.sheets[f'{date}_{country}']
                worksheet.insert_image('O2', chart_file)

        # Case where we do not have any company in between the Upper and Lower GMSR
        elif len(temp_Country.filter((pl.col("Full_MCAP_USD_Cutoff_Company") <= Upper_GMSR) & (pl.col("Full_MCAP_USD_Cutoff_Company") >= Lower_GMSR))) == 0:
            # Try to get as close as possible to Upper GMSR
            TopPercentage_Extension = temp_Country.filter(pl.col("Full_MCAP_USD_Cutoff_Company") >= Lower_GMSR).sort("Full_MCAP_USD_Cutoff_Company", descending=True,
                                        ).filter(~pl.col("Internal_Number").is_in(TopPercentage.select(pl.col("Internal_Number")))).select(
                                        ["Date", "Internal_Number", "Instrument_Name", "ENTITY_QID", "Country", "Free_Float_MCAP_USD_Cutoff_Company", 
                                        "Full_MCAP_USD_Cutoff_Company", "Weight_Cutoff", "CumWeight_Cutoff"])
            
            TopPercentage = TopPercentage.with_columns(
                                    pl.lit("Standard").alias("Size")
                            )
            
            TopPercentage_Extension = TopPercentage_Extension.with_columns(
                                    pl.lit("Standard").alias("Size")
                            )
            
            # Merge the initial Frame with the additions
            TopPercentage = TopPercentage.vstack(TopPercentage_Extension)

            
            TopPercentage = TopPercentage.with_columns(
                                    pl.lit("Above - No Companies in between Upper and Lower GMSR").alias("Case")
                    )
            
            if Excel_Recap == True and country == Country_Plotting:

                # Save DataFrame to Excel
                TopPercentage.to_pandas().to_excel(writer, sheet_name=f'{date}_{country}', index=False)
                # Create and save the chart
                chart_file = Curve_Plotting(TopPercentage, temp_Country, Lower_GMSR, Upper_GMSR)

                # Insert the chart into the Excel file
                workbook = writer.book
                worksheet = writer.sheets[f'{date}_{country}']
                worksheet.insert_image('O2', chart_file)

    return TopPercentage

##################################
########Index Rebalancing#########
##################################

def Index_Rebalancing_Box(Frame: pl.DataFrame, SW_ACALLCAP, Output_Count_Standard_Index, Lower_GMSR, Upper_GMSR, country, date, Excel_Recap,  Right_Limit, Left_Limit, writer):
    temp_Country = Frame.filter((pl.col("Date") == date) & (pl.col("Country") == country))

    # Sort in each Country the Companies by Full MCAP USD Cutoff
    temp_Country = temp_Country.sort("Full_MCAP_USD_Cutoff_Company", descending=True)

    # Check for Companies that are still alive
    temp_Country = temp_Country.filter(pl.col("Internal_Number").is_in(SW_ACALLCAP.filter(pl.col("Review") == date).select(pl.col("Internal_Number")).to_series()))

    # Calculate their CumWeight_Cutoff
    temp_Country = temp_Country.with_columns(
                    (pl.col("Free_Float_MCAP_USD_Cutoff_Company") / pl.col("Free_Float_MCAP_USD_Cutoff_Company").sum()).alias("Weight_Cutoff"),
                    (((pl.col("Free_Float_MCAP_USD_Cutoff_Company") / pl.col("Free_Float_MCAP_USD_Cutoff_Company").sum()).cum_sum())).alias("CumWeight_Cutoff")
    ).sort("Full_MCAP_USD_Cutoff_Company", descending=True)

    # Get the previous newest date from the already created Index
    Previous_Creation_Index_Date = Output_Count_Standard_Index.filter(pl.col("Country") == country).select(pl.col("Date").max()).to_series()[0]

    # Check the number selected in the previous Index
    Company_Selection_Count = Output_Count_Standard_Index.filter((pl.col("Date") == Previous_Creation_Index_Date) & (pl.col("Country") == country)).select(pl.col("Count")).to_numpy()[0][0]

    # Check where X number of Companies lands us on the Curve
    TopPercentage = temp_Country.head(Company_Selection_Count)

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
                                        pl.lit("Standard").alias("Size")
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
        if len(temp_Country.filter((pl.col("Full_MCAP_USD_Cutoff_Company") <= Upper_GMSR) & (pl.col("Full_MCAP_USD_Cutoff_Company") >= Lower_GMSR))) > 0:

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
                                    pl.lit("Standard").alias("Size")
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

        # In case there are no Companies in between the Upper and Lower GMSR
        elif len(temp_Country.filter((pl.col("Full_MCAP_USD_Cutoff_Company") <= Upper_GMSR) & (pl.col("Full_MCAP_USD_Cutoff_Company") >= Lower_GMSR))) == 0:
            TopPercentage_Extension = (
                temp_Country
                .filter((pl.col("Full_MCAP_USD_Cutoff_Company") >= Upper_GMSR) & (pl.col("CumWeight_Cutoff") >= Left_Limit))
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
                ]))
            
            TopPercentage_Extension = TopPercentage_Extension.with_columns(
                                    pl.lit("Addition").alias("Size")
                            )
            
            # Merge the initial Frame with the additions
            if len(TopPercentage_Extension) > 0:
                TopPercentage = TopPercentage.vstack(TopPercentage_Extension.select(TopPercentage.columns))
            else:
                TopPercentage = TopPercentage.with_columns(
                                    pl.lit("Addition").alias("Size")
                )

            TopPercentage = TopPercentage.with_columns(
                                    pl.lit("Above - No Companies in between Upper and Lower GMSR").alias("Case"),
                                    pl.lit("Addition").alias("Size")
                    )
        
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
    "Internal_Number": pl.Series([], dtype=pl.Utf8),
    "Instrument_Name": pl.Series([], dtype=pl.Utf8),
    "ENTITY_QID": pl.Series([], dtype=pl.Utf8),
    "Country": pl.Series([], dtype=pl.Utf8),
    "Free_Float_MCAP_USD_Cutoff_Company": pl.Series([], dtype=pl.Float64),
    "Full_MCAP_USD_Cutoff_Company": pl.Series([], dtype=pl.Float64),
    "Weight_Cutoff": pl.Series([], dtype=pl.Float64),
    "CumWeight_Cutoff": pl.Series([], dtype=pl.Float64),
    "Size": pl.Series([], dtype=pl.Utf8),
    "Case": pl.Series([], dtype=pl.Utf8),
    "Shadow_Company": pl.Series([], dtype=pl.Boolean)
})

Output_Count_Standard_Index = pl.DataFrame({
    "Country": pl.Series([], dtype=pl.Utf8),
    "Count": pl.Series([], dtype=pl.UInt32),
    "Date": pl.Series([], dtype=pl.Date),
})

Screened_Securities = pl.DataFrame({
                                    "Date": pl.Series([], dtype=pl.Date),
                                    "Internal_Number": pl.Series([], dtype=pl.Utf8),
                                    "Segment": pl.Series([], dtype=pl.Utf8)
})

EMS_Frame = pl.DataFrame({
                        "Date": pl.Series([], dtype=pl.Date),
                        "Segment": pl.Series([], dtype=pl.Utf8),
                        "EMS": pl.Series([], dtype=pl.Float64),
                        "Rank": pl.Series([], dtype=pl.Int64),
                        "FreeFloatMCAP_Minimum_Size": pl.Series([], dtype=pl.Float64)
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
            temp_Developed = temp_Developed.select(pl.col(["Date", "ENTITY_QID", "Country", "Internal_Number", "Instrument_Name", "Free_Float", "Capfactor",
                                                                                        "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff"]))
            
            temp_Emerging, EMS_Frame = Equity_Minimum_Size(temp_Emerging, Pivot_TOR, EMS_Frame, date, "Emerging")
            temp_Emerging = temp_Emerging.select(pl.col(["Date", "ENTITY_QID", "Country", "Internal_Number", "Instrument_Name", "Free_Float", "Capfactor",
                                                                                        "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff"]))

            ###################################
            ######TurnoverRatio Screening######
            ###################################

            # Apply the Check on Turnover for all Components
            Developed_Screened = Turnover_Check(temp_Developed, Pivot_TOR, Threshold_NEW, Threshold_OLD, date, Starting_Date, Screened_Securities).with_columns(
                pl.col("Date").cast(pl.Date))
            Emerging_Screened = Turnover_Check(temp_Emerging, Pivot_TOR, Threshold_NEW, Threshold_OLD, date, Starting_Date, Screened_Securities).with_columns(
                pl.col("Date").cast(pl.Date))

            # Remove Securities not passing the screen
            temp_Developed = temp_Developed.join(Developed_Screened, on=["Date", "Internal_Number"], how="left").filter(pl.col("Status_TOR") == "Pass")
            temp_Emerging = temp_Emerging.join(Emerging_Screened, on=["Date", "Internal_Number"], how="left").filter(pl.col("Status_TOR") == "Pass")

            ###################################
            #########FOR FF Screening##########
            ###################################

            # # Filter for FOR_FF >= FOR_FF
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

            temp_Developed_Aggregate = temp_Developed.select(pl.col(["Date", "Internal_Number", "Instrument_Name", "ENTITY_QID", "Country", "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff"])).group_by(
                                                    ["Date", "ENTITY_QID"]).agg([
                                                        pl.col("Country").first().alias("Country"),
                                                        pl.col("Internal_Number").first().alias("Internal_Number"),
                                                        pl.col("Instrument_Name").first().alias("Instrument_Name"),
                                                        pl.col("Free_Float_MCAP_USD_Cutoff").sum().alias("Free_Float_MCAP_USD_Cutoff_Company"),
                                                        pl.col("Full_MCAP_USD_Cutoff").sum().alias("Full_MCAP_USD_Cutoff_Company")
                                                    ]).sort(["Date", "Full_MCAP_USD_Cutoff_Company"], descending = True)

            temp_Emerging_Aggregate = temp_Emerging.select(pl.col(["Date", "Internal_Number", "Instrument_Name", "ENTITY_QID", "Country", "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff"])).group_by(
                                                    ["Date", "ENTITY_QID"]).agg([
                                                        pl.col("Country").first().alias("Country"),
                                                        pl.col("Internal_Number").first().alias("Internal_Number"),
                                                        pl.col("Instrument_Name").first().alias("Instrument_Name"),
                                                        pl.col("Free_Float_MCAP_USD_Cutoff").sum().alias("Free_Float_MCAP_USD_Cutoff_Company"),
                                                        pl.col("Full_MCAP_USD_Cutoff").sum().alias("Full_MCAP_USD_Cutoff_Company")
                                                    ]).sort(["Date", "Full_MCAP_USD_Cutoff_Company"], descending = True)

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

            # Take the first Company crossing the 85% border
            New_Data = pl.DataFrame({
                                "Date": [date],
                                "GMSR_Developed": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") >= Percentage).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0]],
                                "GMSR_Developed_Upper": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") >= Percentage).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] * Upper_Limit],
                                "GMSR_Developed_Lower": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") >= Percentage).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] * Lower_Limit],
                                "GMSR_Emerging": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") >= Percentage).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2],
                                "GMSR_Emerging_Upper": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") >= Percentage).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2 * Upper_Limit],
                                "GMSR_Emerging_Lower": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") >= Percentage).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2 * Lower_Limit],
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
            Stored_Securities = temp_Emerging_Aggregate.filter(pl.col("Country") == Country_Plotting)

            Lower_GMSR = GMSR_Frame.select(["GMSR_Emerging_Lower", "Date"]).filter(pl.col("Date") == date).to_numpy()[0][0]
            Upper_GMSR = GMSR_Frame.select(["GMSR_Emerging_Upper", "Date"]).filter(pl.col("Date") == date).to_numpy()[0][0]

            # Emerging #
            for country in temp_Emerging_Aggregate.select(pl.col("Country")).unique().sort("Country").to_series():
            
                TopPercentage = Index_Creation_Box(temp_Emerging_Aggregate, Lower_GMSR, Upper_GMSR, country, date, Excel_Recap, Percentage, Right_Limit, Left_Limit, writer)

                # Apply the check on Minimum_FreeFloat_MCAP_USD_Cutoff
                TopPercentage = Minimum_FreeFloat_Country(TopPercentage, Lower_GMSR, Upper_GMSR)

                # Stack to Output_Standard_Index
                Output_Standard_Index = Output_Standard_Index.vstack(TopPercentage)

                # Create the Output_Count_Standard_Index for future rebalacing
                Output_Count_Standard_Index = Output_Count_Standard_Index.vstack(TopPercentage.group_by("Country").agg(
                        pl.len().alias("Count"),
                        pl.col("Date").first().alias("Date")
                    ).sort("Count", descending=True))

            # Developed #
            Lower_GMSR = GMSR_Frame.select(["GMSR_Developed_Lower", "Date"]).filter(pl.col("Date") == date).to_numpy()[0][0]
            Upper_GMSR = GMSR_Frame.select(["GMSR_Developed_Upper", "Date"]).filter(pl.col("Date") == date).to_numpy()[0][0]

            # Developed #
            for country in temp_Developed_Aggregate.select(pl.col("Country")).unique().sort("Country").to_series():
            
                TopPercentage = Index_Creation_Box(temp_Developed_Aggregate, Lower_GMSR, Upper_GMSR, country, date, Excel_Recap, Percentage, Right_Limit, Left_Limit, writer)

                # Apply the check on Minimum_FreeFloat_MCAP_USD_Cutoff
                TopPercentage = Minimum_FreeFloat_Country(TopPercentage, Lower_GMSR, Upper_GMSR)

                # Stack to Output_Standard_Index
                Output_Standard_Index = Output_Standard_Index.vstack(TopPercentage)

                # Create the Output_Count_Standard_Index for future rebalacing
                Output_Count_Standard_Index = Output_Count_Standard_Index.vstack(TopPercentage.group_by("Country").agg(
                        pl.len().alias("Count"),
                        pl.col("Date").first().alias("Date")
                    ).sort("Count", descending=True))

            #################################
            ###########Assign Size###########
            #################################

            Standard_Index = temp_Emerging.filter(pl.col("ENTITY_QID").is_in(Output_Standard_Index.select(pl.col("ENTITY_QID")))).join(
                Output_Standard_Index.select("Date", "ENTITY_QID", "Shadow_Company"), on=["Date", "ENTITY_QID"], how="left"
            ).vstack(temp_Developed.filter(pl.col("ENTITY_QID").is_in(Output_Standard_Index.select(pl.col("ENTITY_QID")))).join(
                Output_Standard_Index.select("Date", "ENTITY_QID", "Shadow_Company"), on=["Date", "ENTITY_QID"], how="left"))
            
            Small_Index = temp_Emerging.filter(~pl.col("ENTITY_QID").is_in(Output_Standard_Index.select(pl.col("ENTITY_QID")))).join(
                Output_Standard_Index.select("Date", "ENTITY_QID", "Shadow_Company"), on=["Date", "ENTITY_QID"], how="left"
            ).vstack(temp_Developed.filter(~pl.col("ENTITY_QID").is_in(Output_Standard_Index.select(pl.col("ENTITY_QID")))).join(
                Output_Standard_Index.select("Date", "ENTITY_QID", "Shadow_Company"), on=["Date", "ENTITY_QID"], how="left"))
            
        # Following Reviews where Index is rebalanced
        else:

            ###################################
            ##########Apply EMS Screen#########
            ###################################

            temp_Developed, EMS_Frame = Equity_Minimum_Size(temp_Developed, Pivot_TOR, EMS_Frame, date, "Developed")
            temp_Developed = temp_Developed.select(pl.col(["Date", "ENTITY_QID", "Country", "Internal_Number", "Instrument_Name", "Free_Float", "Capfactor",
                                                                                        "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff"]))
            
            temp_Emerging, EMS_Frame = Equity_Minimum_Size(temp_Emerging, Pivot_TOR, EMS_Frame, date, "Emerging")
            temp_Emerging = temp_Emerging.select(pl.col(["Date", "ENTITY_QID", "Country", "Internal_Number", "Instrument_Name", "Free_Float", "Capfactor",
                                                                                        "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff"]))

            ###################################
            ######TurnoverRatio Screening######
            ###################################

            # Apply the Check on Turnover for all Components
            Developed_Screened = Turnover_Check(temp_Developed, Pivot_TOR, Threshold_NEW, Threshold_OLD, date, Starting_Date, Screened_Securities).with_columns(
                pl.col("Date").cast(pl.Date))
            Emerging_Screened = Turnover_Check(temp_Emerging, Pivot_TOR, Threshold_NEW, Threshold_OLD, date, Starting_Date, Screened_Securities).with_columns(
                pl.col("Date").cast(pl.Date))

            # Remove Securities not passing the screen
            temp_Developed = temp_Developed.join(Developed_Screened, on=["Date", "Internal_Number"], how="left").filter(pl.col("Status_TOR") == "Pass")
            temp_Emerging = temp_Emerging.join(Emerging_Screened, on=["Date", "Internal_Number"], how="left").filter(pl.col("Status_TOR") == "Pass")

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

            temp_Developed_Aggregate = temp_Developed.select(pl.col(["Date", "Internal_Number", "Instrument_Name", "ENTITY_QID", "Country", "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff"])).group_by(
                                                    ["Date", "ENTITY_QID"]).agg([
                                                        pl.col("Country").first().alias("Country"),
                                                        pl.col("Internal_Number").first().alias("Internal_Number"),
                                                        pl.col("Instrument_Name").first().alias("Instrument_Name"),
                                                        pl.col("Free_Float_MCAP_USD_Cutoff").sum().alias("Free_Float_MCAP_USD_Cutoff_Company"),
                                                        pl.col("Full_MCAP_USD_Cutoff").sum().alias("Full_MCAP_USD_Cutoff_Company")
                                                    ]).sort(["Date", "Full_MCAP_USD_Cutoff_Company"], descending = True)

            temp_Emerging_Aggregate = temp_Emerging.select(pl.col(["Date", "Internal_Number", "Instrument_Name", "ENTITY_QID", "Country", "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff"])).group_by(
                                                    ["Date", "ENTITY_QID"]).agg([
                                                        pl.col("Country").first().alias("Country"),
                                                        pl.col("Internal_Number").first().alias("Internal_Number"),
                                                        pl.col("Instrument_Name").first().alias("Instrument_Name"),
                                                        pl.col("Free_Float_MCAP_USD_Cutoff").sum().alias("Free_Float_MCAP_USD_Cutoff_Company"),
                                                        pl.col("Full_MCAP_USD_Cutoff").sum().alias("Full_MCAP_USD_Cutoff_Company")
                                                    ]).sort(["Date", "Full_MCAP_USD_Cutoff_Company"], descending = True)

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
            Previous_Rank_GMSR = GMSR_Frame.filter(pl.col("Date") <= GMSR_MSCI).select(pl.col("Rank")).to_numpy()[0][0]

            # CumWeight_Cutoff of the Previous Rank_GMSR
            CumWeight_Cutoff_Rank = temp_Developed_Aggregate.filter(pl.col("Rank") == Previous_Rank_GMSR).select(pl.col("CumWeight_Cutoff")).to_numpy()[0][0]

            if GMSR_Upper_Buffer <= CumWeight_Cutoff_Rank <= GMSR_Lower_Buffer:
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
                
            elif CumWeight_Cutoff_Rank > GMSR_Lower_Buffer:
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

            elif CumWeight_Cutoff_Rank < GMSR_Upper_Buffer:
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
            Stored_Securities = Stored_Securities.vstack(temp_Emerging_Aggregate.filter(pl.col("Country") == Country_Plotting))

            # Emerging #
            Lower_GMSR = GMSR_Frame.select(["GMSR_Emerging_Lower", "Date"]).filter(pl.col("Date") == date).to_numpy()[0][0]
            Upper_GMSR = GMSR_Frame.select(["GMSR_Emerging_Upper", "Date"]).filter(pl.col("Date") == date).to_numpy()[0][0]

            for country in temp_Emerging_Aggregate.select(pl.col("Country")).unique().sort("Country").to_series():

                # Check if there is already a previous Index creation for the current country
                if len(Output_Count_Standard_Index.filter((pl.col("Country") == country) & (pl.col("Date") < date))) > 0:

                    TopPercentage, temp_Country = Index_Rebalancing_Box(temp_Emerging_Aggregate, SW_ACALLCAP, Output_Count_Standard_Index, Lower_GMSR, Upper_GMSR, country, date, Excel_Recap,  Right_Limit, Left_Limit, writer)

                    # Apply the check on Minimum_FreeFloat_MCAP_USD_Cutoff
                    TopPercentage = Minimum_FreeFloat_Country(TopPercentage, Lower_GMSR, Upper_GMSR)

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
                
                # If there is no composition, a new Index will be created
                else:
                    TopPercentage = Index_Creation_Box(temp_Emerging_Aggregate, Lower_GMSR, Upper_GMSR, country, date, Excel_Recap, Percentage, Right_Limit, Left_Limit, writer)
                    
                    # Apply the check on Minimum_FreeFloat_MCAP_USD_Cutoff
                    TopPercentage = Minimum_FreeFloat_Country(TopPercentage, Lower_GMSR, Upper_GMSR)

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

            # Developed #
            Lower_GMSR = GMSR_Frame.select(["GMSR_Developed_Lower", "Date"]).filter(pl.col("Date") == date).to_numpy()[0][0]
            Upper_GMSR = GMSR_Frame.select(["GMSR_Developed_Upper", "Date"]).filter(pl.col("Date") == date).to_numpy()[0][0]

            for country in temp_Developed_Aggregate.select(pl.col("Country")).unique().sort("Country").to_series():

                # Check if there is already a previous Index creation for the current country
                if len(Output_Count_Standard_Index.filter((pl.col("Country") == country) & (pl.col("Date") < date))) > 0:

                    TopPercentage, temp_Country = Index_Rebalancing_Box(temp_Developed_Aggregate, SW_ACALLCAP, Output_Count_Standard_Index, Lower_GMSR, Upper_GMSR, country, date, Excel_Recap, Right_Limit, Left_Limit, writer)

                    # Apply the check on Minimum_FreeFloat_MCAP_USD_Cutoff
                    TopPercentage = Minimum_FreeFloat_Country(TopPercentage, Lower_GMSR, Upper_GMSR)

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
                
                # If there is no composition, a new Index will be created
                else:
                    TopPercentage = Index_Creation_Box(temp_Developed_Aggregate, Lower_GMSR, Upper_GMSR, country, date, Excel_Recap, Percentage, Right_Limit, Left_Limit, writer)
                    
                    # Apply the check on Minimum_FreeFloat_MCAP_USD_Cutoff
                    TopPercentage = Minimum_FreeFloat_Country(TopPercentage, Lower_GMSR, Upper_GMSR)

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

            Standard_Index = Standard_Index.vstack(temp_Emerging.filter(pl.col("ENTITY_QID").is_in(Output_Standard_Index.select(pl.col("ENTITY_QID")))).join(
                Output_Standard_Index.select("Date", "ENTITY_QID", "Shadow_Company"), on=["Date", "ENTITY_QID"], how="left"
            )).vstack(temp_Developed.filter(pl.col("ENTITY_QID").is_in(Output_Standard_Index.select(pl.col("ENTITY_QID")))).join(
                Output_Standard_Index.select("Date", "ENTITY_QID", "Shadow_Company"), on=["Date", "ENTITY_QID"], how="left"
            ))

            Small_Index = Small_Index.vstack(temp_Emerging.filter(~pl.col("ENTITY_QID").is_in(Output_Standard_Index.select(pl.col("ENTITY_QID")))).join(
                Output_Standard_Index.select("Date", "ENTITY_QID", "Shadow_Company"), on=["Date", "ENTITY_QID"], how="left"
            )).vstack(temp_Developed.filter(~pl.col("ENTITY_QID").is_in(Output_Standard_Index.select(pl.col("ENTITY_QID")))).join(
                Output_Standard_Index.select("Date", "ENTITY_QID", "Shadow_Company"), on=["Date", "ENTITY_QID"], how="left"
            ))

    # Add Recap_GMSR_Frame to Excel
    Stored_Securities.to_pandas().to_excel(writer, sheet_name="Historical_Screened_Securities", index=False)
    GMSR_Frame.to_pandas().to_excel(writer, sheet_name="GMSR_Historical", index=False)

### Small Index ###

# Filter the Securities from Company Level
Small_Index_Security_Level = Emerging.select(pl.col(["Date", "ENTITY_QID", "Country", "Internal_Number", "Capfactor", "ISIN", "SEDOL"])).join(Small_Index.filter(
    pl.col("Country").is_in(Emerging.select(pl.col("Country").unique()))
), on=["Date", "ENTITY_QID"], how="semi")

# Add information of CapFactor/Mcap_Units_Index_Currency
Small_Index_Security_Level = Small_Index_Security_Level.join(pl.read_parquet(
    r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Universe\STXWAGV_Review.parquet").with_columns(
        pl.col("Date").cast(pl.Date)
    ), on=["Date", "Internal_Number"], how="left")

# Remove China A / CCS
Small_Index_Security_Level = Small_Index_Security_Level.filter(
    ~(
        (pl.col("Country") == "CN") &
        (
            pl.col("Instrument_Name").str.contains("'A'") |
            pl.col("Instrument_Name").str.contains("(CCS)")
        )))

# Calculate the Weights for each Date
Small_Index_Security_Level = Small_Index_Security_Level.with_columns(
    (pl.col("Mcap_Units_Index_Currency") / pl.col("Mcap_Units_Index_Currency").sum().over("Date")).alias("Weight")
)

### Standard Index ###
# Filter the Securities from Company Level
Standard_Index_Security_Level = Emerging.select(pl.col(["Date", "ENTITY_QID", "Country", "Internal_Number", "Capfactor", "ISIN", "SEDOL"])).join(Standard_Index.filter(
    pl.col("Country").is_in(Emerging.select(pl.col("Country").unique()))
), on=["Date", "ENTITY_QID"], how="semi")

# Add information of CapFactor/Mcap_Units_Index_Currency
Standard_Index_Security_Level = Standard_Index_Security_Level.join(pl.read_parquet(
    r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Universe\STXWAGV_Review.parquet").with_columns(
        pl.col("Date").cast(pl.Date)
    ), on=["Date", "Internal_Number"], how="left")

# Calculate the Weights for each Date
Standard_Index_Security_Level = Standard_Index_Security_Level.with_columns(
    (pl.col("Mcap_Units_Index_Currency") / pl.col("Mcap_Units_Index_Currency").sum().over("Date")).alias("Weight")
)

# Create a Recap
Recap_Count = (
    Small_Index_Security_Level
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
    Small_Index_Security_Level
    .group_by(["Country", "Date"])  # Group by Country and Date
    .agg(pl.col("Weight").sum().alias("Weight_Components"))  # Count "Count" column and alias it
    .sort("Date")  # Ensure sorting by Date for proper column ordering in the pivot
    .pivot(
        index="Country",  # Set Country as the row index
        on="Date",        # Create columns for each unique Date
        values="Weight_Components"  # Fill in values with Sum_Components
    )
)

# Store the Results
Small_Index_Security_Level.write_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Output\Small_Index_Security_Level_1997.csv")
Standard_Index_Security_Level.write_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Output\Standard_Index_Security_Level_1997.csv")
Recap_Count.write_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Output\Recap_Count_1997.csv")
Recap_Weight.write_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Output\Recap_Weight_1997.csv")
GMSR_Frame.write_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Output\GMSR_Frame_1997.csv")

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