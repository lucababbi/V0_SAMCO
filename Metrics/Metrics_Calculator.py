import pandas as pd
import numpy as np
import os

def calculate_metrics(df, csv_name):
    # Ensure the DataFrame is sorted by Date
    df.sort_index(inplace=True)
    
    # Calculate daily returns
    df['Return'] = df['STOXX-Price'].pct_change()

    # Function to calculate annualized return
    def annualized_return(total_return, periods):
        return (1 + total_return) ** (1 / periods) - 1

    # Function to calculate annualized volatility
    def annualized_volatility(returns):
        return returns.std() * np.sqrt(252)
    
    # Function to calculate Sharpe Ratio
    def sharpe_ratio(total_return, returns, risk_free_rate, periods):
        excess_returns = (1 + total_return) ** (1 / periods) - 1
        volatility = returns.std() * np.sqrt(252)
        return excess_returns / volatility
    
    # Function to calculate Maximum Drawdown
    def max_drawdown(returns):
        cumulative = (1 + returns).cumprod()
        peak = cumulative.cummax()
        drawdown = (cumulative - peak) / peak
        return abs(drawdown.min())
    
    # Period definitions
    end_date = df.index[-1]
    periods = {
        'YTD': df.loc[df.index >= pd.Timestamp(end_date.year, 1, 1)],
        '1Y': df.loc[df.index >= end_date - pd.DateOffset(years=1)],
        '3Y': df.loc[df.index >= end_date - pd.DateOffset(years=3)],
        '5Y': df.loc[df.index >= end_date - pd.DateOffset(years=5)],
        'Overall': df
    }
    
    metrics = {}

    # Risk-free rate assumption (for Sharpe Ratio calculation)
    risk_free_rate = 0.00  # Example risk-free rate

    for period_name, period_df in periods.items():
        if len(period_df) > 1:
            total_return = period_df['STOXX-Price'].iloc[-1] / period_df['STOXX-Price'].iloc[0] - 1
            periods_in_year = len(period_df) / 252  # Assuming 252 trading days in a year
            
            metrics[f'{period_name} Return (actual)'] = total_return * 100
            metrics[f'{period_name} Return (annualized)'] = annualized_return(total_return, periods_in_year) * 100
            metrics[f'{period_name} Volatility (annualized)'] = annualized_volatility(period_df['Return'].dropna()) * 100
            metrics[f'{period_name} Sharpe ratio'] = sharpe_ratio(total_return, period_df['Return'].dropna(), risk_free_rate, periods_in_year)
            metrics[f'{period_name} Maximum drawdown'] = max_drawdown(period_df['Return'].dropna()) * 100
    
    metrics_df = pd.DataFrame(metrics, index=[csv_name]).transpose()
    
    return metrics_df

# Load data from CSV
csv_file_path = r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Metrics\Input\Developed_EU_MID-Cap.csv"
csv_name = os.path.splitext(os.path.basename(csv_file_path))[0]
df = pd.read_csv(csv_file_path, sep=";")[["Date", "STOXX-Price"]]


# Ensure 'Date' column is parsed as datetime and set as index
df["Date"] = pd.to_datetime(df["Date"])
df.set_index('Date', inplace=True)

# Calculate metrics and return as DataFrame
metrics_df = calculate_metrics(df, csv_name)
print(metrics_df)
metrics_df.to_clipboard()