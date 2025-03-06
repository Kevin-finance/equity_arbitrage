import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
from itertools import cycle, islice
from settings import config
from datetime import datetime 


def get_adjacent_dates(target_date :datetime.date , date_ranges: list):
    
    for i, (start, end) in enumerate(date_ranges):
        if start <= target_date <= end:
            next_start = date_ranges[i + 1][0] if i + 1 < len(date_ranges) else None
            next_end = date_ranges[i + 1][1] if i + 1 < len(date_ranges) else None
            return end, next_end 

    return None, None  

def roll_over(target_date:datetime.date, date_range :tuple , date_ranges:list, days_before : int): 
    # target_date : datetime.date
    # roll over the date to the next expiration date
    # takes date_range which is list of tuples (start_date, end_date)

    if date_range[0] - target_date <= pd.Timedelta(days=days_before):

        # 엄밀히 말하면 bdays 가 아니라 market days로 해야함
        return get_adjacent_dates(target_date + relativedelta(days=days_before+1), date_ranges)

    else:
        return date_range

def get_clean_df (df_raw : pd.DataFrame, date_ranges : list, index_pairs : list):
    # At maturity, the near month contract is rolled over to the deferred month contract

    df = pd.DataFrame(columns= ['Near Month PX_LAST','Deferred Month PX_LAST'])
    
    for i in range(len(df_raw.index)):
        
        target_date = df_raw.index[i]
        range_ = get_adjacent_dates(target_date, date_ranges)
        near_month, deferred_month = roll_over(target_date, range_, date_ranges, 0) 
        # quarter = int((near_month.month / 3) -1)
        if range_ == (near_month, deferred_month):
            df.loc[target_date] = [df_raw[index_pairs[0][0]]['PX_LAST'].loc[target_date], df_raw[index_pairs[0][1]]['PX_LAST'].loc[target_date]]

        else:
            df.loc[target_date] = [df_raw[index_pairs[1][0]]['PX_LAST'].loc[target_date], df_raw[index_pairs[1][1]]['PX_LAST'].loc[target_date]]
            print("rolled over")
            print(target_date,range_,( near_month, deferred_month))
    df.dropna(inplace=True)
    return df

if __name__ == "__main__":
    df_raw = pd.read_parquet('../data_manual/bloomberg_historical_data.parquet')

