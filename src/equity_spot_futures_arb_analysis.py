import pandas as pd
from datetime import datetime
from itertools import cycle, islice
from dateutil.relativedelta import relativedelta
from settings import config
import numpy as np
import clean_bloomberg as clean_bbg
import pull_optionm_api_data as pull_optionm
import matplotlib.pyplot as plt


df_raw = pd.read_parquet('../data_manual/bloomberg_historical_data.parquet')

START_DATE = config("START_DATE")
END_DATE = config("END_DATE")
# Set it slightly earlier than the start date to get the data before the expiration date
start_date = datetime.strftime(config("START_DATE").date()-relativedelta(years=1),format="%Y-%m-%d") 
end_date = datetime.strftime(config("END_DATE"),format="%Y-%m-%d")

expiration_months = [3, 6, 9, 12]
expiration_dates = pull_optionm.get_expiration_dates(start_date, end_date, expiration_months)

# near future expiration date and far future expiration date for each dates
date_ranges = [(start.date(), end.date()) for start, end in zip(expiration_dates, expiration_dates[1:])]

spx = ['ES1 Index','ES2 Index','ES3 Index']
spx_pairs= list(zip(spx,spx[1:]))

ndx = ['NQ1 Index','NQ2 Index','NQ3 Index']
ndx_pairs= list(zip(ndx,ndx[1:]))

djx = ['DM1 Index','DM2 Index','DM3 Index']
djx_pairs= list(zip(djx,djx[1:]))

ois_df = df_raw['USSOC CMPN Curncy']['PX_LAST']
ois_df.dropna(inplace=True)

index_pairs_map = {
    "SPX": spx_pairs,
    "NDX": ndx_pairs,
    "INDU": djx_pairs
}

index_optionm_df= dict()
index_future_df= dict()
index_df= dict()

for index_name in index_pairs_map.keys():
    optionm_index_name = "DJX" if index_name == "INDU" else index_name

    df_div_yield = pull_optionm.pull_index_implied_dividend_yield(
        optionm_index_name, start_date=START_DATE, end_date=END_DATE
    )

    df_loaded = pull_optionm.load_index_implied_dividend_yield(optionm_index_name)

    df_filtered = pull_optionm.filter_index_implied_dividend_yield(df_loaded)
    df_filtered = df_filtered.sort_values(by=['date','expiration']).groupby('date').agg(list)
    df_filtered[['expiration_near','expiration_next']] = pd.DataFrame(df_filtered['expiration'].tolist(), index=df_filtered.index)
    df_filtered[['rate_near','rate_next']] = pd.DataFrame(df_filtered['rate'].tolist(), index=df_filtered.index)
    df_filtered = df_filtered.drop(columns=['expiration','rate'])
    df_filtered = df_filtered.filter(items=['date','expiration_near','expiration_next','rate_near','rate_next'])
    df_filtered.index = [ts.date() for ts in df_filtered.index]
    
    index_optionm_df[index_name] = df_filtered

    index_future_df[index_name] = clean_bbg.get_clean_df(df_raw, date_ranges, index_pairs_map[index_name]) 
    index_df[index_name] = df_raw[index_name + " Index"]
    index_df[index_name].dropna(inplace=True)

for keys,val in index_optionm_df.items():
    index_optionm_df[keys][['days_to_near_expiry','days_to_far_expiry']] = index_optionm_df[keys][['expiration_near','expiration_next']].apply(lambda x: x - index_optionm_df[keys].index)
    index_optionm_df[keys][['days_to_near_expiry','days_to_far_expiry']] = index_optionm_df[keys][['days_to_near_expiry','days_to_far_expiry']].applymap(lambda x: x.days)/360
    index_optionm_df[keys][['div_near','div_next']] = index_optionm_df[keys][['rate_near','rate_next']] .apply(lambda x: x *index_df[keys]['PX_LAST'])/100
    index_optionm_df[keys][['div_near', 'div_next']] = (
    index_optionm_df[keys][['div_near', 'div_next']].values *
    index_optionm_df[keys][['days_to_near_expiry', 'days_to_far_expiry']].values
)


implied_forward_df = dict()
for keys,val in index_future_df.items():
    implied_forward_df[keys] = pd.DataFrame(index=index_future_df[keys].index, columns=['Implied Forward','Near Month TTM','Deferred Month TTM','Annualised'])
    implied_forward_df[keys]['Implied Forward'] = (((index_future_df[keys]['Deferred Month PX_LAST']+index_optionm_df[keys]['div_next']) \
        /(index_future_df[keys]['Near Month PX_LAST']+index_optionm_df[keys]['div_near']))-1)*100
    implied_forward_df[keys]['Near Month TTM'] = index_optionm_df[keys]['days_to_near_expiry']
    implied_forward_df[keys]['Deferred Month TTM'] = index_optionm_df[keys]['days_to_far_expiry']
    implied_forward_df[keys]['Annualised'] = (implied_forward_df[keys]['Implied Forward']) / (implied_forward_df[keys]['Deferred Month TTM'] - implied_forward_df[keys]['Near Month TTM']) 

implied_forward_df['OIS'] = pd.DataFrame(index=ois_df.index, columns=['Annualised'])
implied_forward_df['OIS']['Annualised'] = ((1+ois_df/100 * implied_forward_df['SPX']['Deferred Month TTM'])/(1+ois_df/100 * implied_forward_df['SPX']['Near Month TTM'])-1)*4*100



total_df = pd.concat([implied_forward_df['SPX']['Annualised']
    ,implied_forward_df['NDX']['Annualised']
    ,implied_forward_df['INDU']['Annualised']
    ,implied_forward_df['OIS']],axis=1)
total_df.columns = ['SPX','NDX','INDU','OIS']
total_df.dropna(inplace=True)

total_df['SPX_Spread'] = total_df['SPX'] -total_df['OIS']
total_df['NDX_Spread'] = total_df['NDX']- total_df['OIS']
total_df['INDU_Spread'] = total_df['INDU'] - total_df['OIS']

# Removing Outliers
def remove_outliers(df, column, window=45, threshold=10):
    median_col = f"{column}_median"
    mad_col = f"{column}_mad"
    

    df[median_col] = df[column].rolling(window=window, center=True, min_periods=1).median()
    df['abs_dev'] = np.abs(df[column] - df[median_col])
    
    df[mad_col] = df['abs_dev'].rolling(window=window, center=True, min_periods=1).mean()
    df['bad_price'] = (df['abs_dev'] / df[mad_col]) >= threshold
    df.loc[df['bad_price'], column] = np.nan 
    df.drop(columns=[median_col, 'abs_dev', mad_col, 'bad_price'], inplace=True)
    return df

for col in ['SPX_Spread', 'NDX_Spread', 'INDU_Spread']:
    total_df = remove_outliers(total_df, col)

# Basis Points
total_df['SPX_Spread'] *= 100
total_df['NDX_Spread'] *= 100
total_df['INDU_Spread'] *= 100

# Plotting
plt.figure(figsize=(12, 6))
plt.plot(total_df.index, total_df['SPX_Spread'], label='SPX Spread', linestyle='--', markersize=0.5)
plt.plot(total_df.index, total_df['NDX_Spread'], label='NDX Spread', linestyle='--', markersize=0.5)
# plt.plot(total_df.index, total_df['INDU_Spread'], label='INDU Spread', linestyle='--', markersize=0.5)

plt.title('Equity Index Spread (After Anomaly Removal)')
plt.xlabel('Date')
plt.ylabel('Spread (bps)')
plt.legend()
plt.grid(True)
plt.hlines(0, total_df.index[0], total_df.index[-1], 'r', linestyles='--', linewidth=1)
plt.ylim(-50, 150)
plt.show()



series = implied_forward_df['NDX']['Annualised'] - implied_forward_df['OIS']['Annualised']
series.index = pd.to_datetime(series.index)

fig, ax = plt.subplots(figsize=(12, 6))
years = series.index.year.unique()
colors = plt.cm.viridis_r(np.linspace(0, 1, len(years)))

for i, (year, color) in enumerate(zip(years, colors)):
    yearly_data = series[series.index.year == year]
    day_of_year = yearly_data.index.dayofyear  # 1~365 or 366 (윤년)
    ax.plot(day_of_year, yearly_data.values, label=str(year), color=color, alpha=0.8)


ax.set_title("Yearly Comparison of Time Series Data")
ax.set_xlabel("Day of Year (1-365)")
ax.set_ylabel("Value")
ax.legend(title="Year", loc='upper left', bbox_to_anchor=(1, 1))  
ax.set_xticks(np.linspace(1, 365, 12))  
ax.set_xticklabels(["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])  
ax.set_ylim(-1,1)
plt.grid(True, linestyle='--', alpha=0.5) 
plt.show()


