"""
This script performs analysis done in the paper "Equity Index Spread Arbitrage".
The data here uses the implied dividend yield (the market's expectation) rather than the realized dividend yield.
"""

import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from settings import config
import numpy as np
import clean_bloomberg as clean_bbg
import pull_optionm_api_data as pull_optionm
import matplotlib.pyplot as plt

# ------------------------------------------------------------------------------
# 1. Load Data and Set Dates
# ------------------------------------------------------------------------------

# Load raw Bloomberg historical data from a Parquet file
import os
import pandas as pd

# Set the path to the Bloomberg historical data file
root_path = os.getcwd()
data_path = os.path.join(root_path, 'data_manual', 'bloomberg_historical_data.parquet')

df_raw = pd.read_parquet(data_path)

# Retrieve configuration parameters: start date, end date, and output directory
START_DATE = config("START_DATE")
END_DATE = config("END_DATE")
OUTPUT_DIR = config("OUTPUT_DIR")

# Adjust the start date to one year before the configured start date,
# ensuring there is enough data before the expiration date.
start_date = datetime.strftime(config("START_DATE").date() - relativedelta(years=1), format="%Y-%m-%d")
end_date = datetime.strftime(config("END_DATE"), format="%Y-%m-%d")

# ------------------------------------------------------------------------------
# 2. Define Expiration Dates and Index Pairs
# ------------------------------------------------------------------------------

# Define expiration months (March, June, September, December)
expiration_months = [3, 6, 9, 12]
# Get expiration dates between start_date and end_date for the specified months
expiration_dates = pull_optionm.get_expiration_dates(start_date, end_date, expiration_months)

# Create pairs of consecutive expiration dates for near and far futures
date_ranges = [(start.date(), end.date()) for start, end in zip(expiration_dates, expiration_dates[1:])]

# Define index symbols for SPX, NDX, and DJX (represented as INDU)
spx = ['ES1 Index', 'ES2 Index', 'ES3 Index']
spx_pairs = list(zip(spx, spx[1:]))

ndx = ['NQ1 Index', 'NQ2 Index', 'NQ3 Index']
ndx_pairs = list(zip(ndx, ndx[1:]))

djx = ['DM1 Index', 'DM2 Index', 'DM3 Index']
djx_pairs = list(zip(djx, djx[1:]))

# Extract the OIS (Overnight Index Swap) rate from the raw data and drop missing values
ois_df = df_raw['USSOC CMPN Curncy']['PX_LAST']
ois_df.dropna(inplace=True)

# Create a mapping of index names to their corresponding pairs for later processing
index_pairs_map = {
    "SPX": spx_pairs,
    "NDX": ndx_pairs,
    "INDU": djx_pairs
}

# ------------------------------------------------------------------------------
# 3. Prepare Data Structures and Process Implied Dividend Yield
# ------------------------------------------------------------------------------

# Initialize dictionaries to store option market data, future data, and index data for each index
index_optionm_df = dict()
index_future_df = dict()
index_df = dict()

# Loop over each index to pull and process the implied dividend yield data
for index_name in index_pairs_map.keys():
    # For the DJX (INDU) index, adjust the name to "DJX" as required by the API; otherwise, use the index name directly.
    optionm_index_name = "DJX" if index_name == "INDU" else index_name

    # Pull implied dividend yield data from the API for the given index and date range
    df_div_yield = pull_optionm.pull_index_implied_dividend_yield(
        optionm_index_name, start_date=START_DATE, end_date=END_DATE
    )

    # Load the dividend yield data that was pulled
    df_loaded = pull_optionm.load_index_implied_dividend_yield(optionm_index_name)

    # Filter the dividend yield data to retain only the relevant entries
    df_filtered = pull_optionm.filter_index_implied_dividend_yield(df_loaded)
    # Sort data by date and expiration, and group by date to aggregate values into lists
    df_filtered = df_filtered.sort_values(by=['date', 'expiration']).groupby('date').agg(list)
    # Split the aggregated lists into "near" and "next" (far) expiration columns for dates and rates
    df_filtered[['expiration_near', 'expiration_next']] = pd.DataFrame(df_filtered['expiration'].tolist(), index=df_filtered.index)
    df_filtered[['rate_near', 'rate_next']] = pd.DataFrame(df_filtered['rate'].tolist(), index=df_filtered.index)
    # Drop the original columns and filter the DataFrame to retain only the needed columns
    df_filtered = df_filtered.drop(columns=['expiration', 'rate'])
    df_filtered = df_filtered.filter(items=['date', 'expiration_near', 'expiration_next', 'rate_near', 'rate_next'])
    # Convert the DataFrame index from timestamp to date
    df_filtered.index = [ts.date() for ts in df_filtered.index]
    
    # Store the filtered dividend yield data in the dictionary
    index_optionm_df[index_name] = df_filtered

    # Get cleaned future data for the current index using a custom cleaning function
    index_future_df[index_name] = clean_bbg.get_clean_df(df_raw, date_ranges, index_pairs_map[index_name]) 

    # Extract index price data and remove any missing values
    index_df[index_name] = df_raw[index_name + " Index"]
    index_df[index_name].dropna(inplace=True)

# ------------------------------------------------------------------------------
# 4. Calculate Days to Expiration and Dividend Amounts
# ------------------------------------------------------------------------------

for keys, val in index_optionm_df.items():
    # Calculate the difference between expiration dates and the current date for near and far contracts
    index_optionm_df[keys][['days_to_near_expiry', 'days_to_far_expiry']] = index_optionm_df[keys][['expiration_near', 'expiration_next']].apply(lambda x: x - index_optionm_df[keys].index)
    # Convert the time difference to a fraction of a year (using 360 days)
    index_optionm_df[keys][['days_to_near_expiry', 'days_to_far_expiry']] = index_optionm_df[keys][['days_to_near_expiry', 'days_to_far_expiry']].applymap(lambda x: x.days) / 360
    # Calculate dividend amounts by applying the implied dividend yield to the current index price and converting from percentage to absolute value
    index_optionm_df[keys][['div_near', 'div_next']] = index_optionm_df[keys][['rate_near', 'rate_next']].apply(lambda x: x * index_df[keys]['PX_LAST']) / 100
    # Scale the dividend amounts by the time to expiration
    index_optionm_df[keys][['div_near', 'div_next']] = (
        index_optionm_df[keys][['div_near', 'div_next']].values *
        index_optionm_df[keys][['days_to_near_expiry', 'days_to_far_expiry']].values
    )

# ------------------------------------------------------------------------------
# 5. Compute Implied Forward Rate and Annualised Rate
# ------------------------------------------------------------------------------

# Initialize a dictionary to store the implied forward rate calculations for each index
implied_forward_df = dict()
for keys, val in index_future_df.items():
    # Create a DataFrame with columns for forward rate calculations and time-to-maturity (TTM)
    implied_forward_df[keys] = pd.DataFrame(index=index_future_df[keys].index, columns=['Implied Forward', 'Near Month TTM', 'Deferred Month TTM', 'Annualised'])
    # Calculate the implied forward rate using future prices adjusted by dividend amounts
    implied_forward_df[keys]['Implied Forward'] = (((index_future_df[keys]['Deferred Month PX_LAST'] + index_optionm_df[keys]['div_next']) \
        / (index_future_df[keys]['Near Month PX_LAST'] + index_optionm_df[keys]['div_near'])) - 1) * 100
    # Store the time-to-maturity for the near and deferred contracts
    implied_forward_df[keys]['Near Month TTM'] = index_optionm_df[keys]['days_to_near_expiry']
    implied_forward_df[keys]['Deferred Month TTM'] = index_optionm_df[keys]['days_to_far_expiry']
    # Annualise the implied forward rate based on the time difference between the near and deferred expiration dates
    implied_forward_df[keys]['Annualised'] = (implied_forward_df[keys]['Implied Forward']) / (implied_forward_df[keys]['Deferred Month TTM'] - implied_forward_df[keys]['Near Month TTM'])

# Calculate the annualised OIS rate using the SPX time-to-maturity as a reference.
implied_forward_df['OIS'] = pd.DataFrame(index=ois_df.index, columns=['Annualised'])
implied_forward_df['OIS']['Annualised'] = ((1 + ois_df / 100 * implied_forward_df['SPX']['Deferred Month TTM']) / (1 + ois_df / 100 * implied_forward_df['SPX']['Near Month TTM']) - 1) * 4 * 100

# ------------------------------------------------------------------------------
# 6. Combine Data and Compute Spreads
# ------------------------------------------------------------------------------

# Merge the annualised rates for SPX, NDX, INDU, and OIS into a single DataFrame
total_df = pd.concat([implied_forward_df['SPX']['Annualised'],
                      implied_forward_df['NDX']['Annualised'],
                      implied_forward_df['INDU']['Annualised'],
                      implied_forward_df['OIS']], axis=1)
total_df.columns = ['SPX', 'NDX', 'INDU', 'OIS']
total_df.dropna(inplace=True)

# Compute the spread for each index by subtracting the OIS rate from the index rate
total_df['SPX_Spread'] = total_df['SPX'] - total_df['OIS']
total_df['NDX_Spread'] = total_df['NDX'] - total_df['OIS']
total_df['INDU_Spread'] = total_df['INDU'] - total_df['OIS']

# ------------------------------------------------------------------------------
# 7. Remove Outliers and Convert Units
# ------------------------------------------------------------------------------

def remove_outliers(df, column, window=45, threshold=10):
    """
    Remove outliers from a column using a rolling median and mean absolute deviation (MAD).
    Values with a deviation ratio greater than the threshold are replaced with NaN.
    """
    median_col = f"{column}_median"
    # Compute the rolling median
    df[median_col] = df[column].rolling(window=window, center=True, min_periods=1).median()
    # Compute absolute deviation from the median
    df['abs_dev'] = np.abs(df[column] - df[median_col])
    
    mad_col = f"{column}_mad"
    # Compute the rolling mean absolute deviation (MAD)
    df[mad_col] = df['abs_dev'].rolling(window=window, center=True, min_periods=1).mean()
    # Identify outliers based on the threshold
    df['bad_price'] = (df['abs_dev'] / df[mad_col]) >= threshold
    # Replace outlier values with NaN
    df.loc[df['bad_price'], column] = np.nan 
    # Remove temporary columns used in calculations
    df.drop(columns=[median_col, 'abs_dev', mad_col, 'bad_price'], inplace=True)
    return df

# Apply the outlier removal function to the spread columns
for col in ['SPX_Spread', 'NDX_Spread', 'INDU_Spread']:
    total_df = remove_outliers(total_df, col)

# Convert the spread values to basis points (bps)
total_df['SPX_Spread'] *= 100
total_df['NDX_Spread'] *= 100
total_df['INDU_Spread'] *= 100

# Save the final DataFrame to a Parquet file for later use
total_df.to_parquet(f'{OUTPUT_DIR}/total_df.parquet')

desc = total_df.filter(items= ['SPX_Spread', 'NDX_Spread','INDU_Spread']).describe()
data = desc.values.tolist()  
col_labels = list(desc.columns)
row_labels = list(desc.index)

fig, ax = plt.subplots()
ax.axis('tight')
ax.axis('off')

table = ax.table(cellText=data, colLabels=col_labels, rowLabels=row_labels, loc='center')

plt.savefig(f'{OUTPUT_DIR}/table_proxy_replication.pdf')

# ------------------------------------------------------------------------------
# 8. Plotting the Equity Index Spread
# ------------------------------------------------------------------------------

# Plot the spread for SPX, NDX, and INDU over time
plt.figure(figsize=(12, 6))
plt.plot(total_df.index, total_df['SPX_Spread'], label='SPX Spread', linestyle='--', markersize=0.5)
plt.plot(total_df.index, total_df['NDX_Spread'], label='NDX Spread', linestyle='--', markersize=0.5)
plt.plot(total_df.index, total_df['INDU_Spread'], label='INDU Spread', linestyle='--', markersize=0.5)

plt.title('Equity Index Spread (After Anomaly Removal)')
plt.xlabel('Date')
plt.ylabel('Spread (bps)')
plt.legend()
plt.grid(True)
# Draw a horizontal red dashed line at 0 for reference
plt.hlines(0, total_df.index[0], total_df.index[-1], 'r', linestyles='--', linewidth=1)
plt.ylim(-50, 150)
# Save the plot to a PDF file
plt.savefig(f'{OUTPUT_DIR}/equity_index_spread_plot_proxy_replication.pdf')
plt.show()

# ------------------------------------------------------------------------------
# 9. Yearly Comparison Plot of the Time Series Data
# ------------------------------------------------------------------------------

# Calculate the NDX spread: difference between the annualised NDX rate and the OIS rate
series = implied_forward_df['NDX']['Annualised'] - implied_forward_df['OIS']['Annualised']
# Convert the index to datetime format for proper plotting
series.index = pd.to_datetime(series.index)

# Create a plot for a yearly comparison of the NDX spread
fig, ax = plt.subplots(figsize=(12, 6))
# Identify the unique years in the data
years = series.index.year.unique()
# Generate a distinct color for each year using the Viridis colormap
colors = plt.cm.viridis_r(np.linspace(0, 1, len(years)))

# Plot the data for each year, using the day of the year on the x-axis
for i, (year, color) in enumerate(zip(years, colors)):
    yearly_data = series[series.index.year == year]
    day_of_year = yearly_data.index.dayofyear  
    ax.plot(day_of_year, yearly_data.values, label=str(year), color=color, alpha=0.8)

ax.set_title("Yearly Comparison of Time Series Data")
ax.set_xlabel("Day of Year (1-365)")
ax.set_ylabel("Value")
ax.legend(title="Year", loc='upper left', bbox_to_anchor=(1, 1))
# Set x-axis ticks to represent months
ax.set_xticks(np.linspace(1, 365, 12))
ax.set_xticklabels(["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])
ax.set_ylim(-1, 1)
plt.grid(True, linestyle='--', alpha=0.5)
# Save the yearly comparison plot as a PDF file
plt.savefig(f'{OUTPUT_DIR}/yearly_comparison.pdf')
plt.show()
