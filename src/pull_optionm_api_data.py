"""
Functions to pull and store implied dividend yields for SPX (S&P 500), DJX (Dow Jones), and MNX (Nasdaq 100) from WRDS (OptionMetrics `optionm.idxdvd`).

 - Data source: https://wrds-www.wharton.upenn.edu/data-dictionary/optionm_idxdvd/
 - Why implied dividend yield matters: Used in futures pricing and arbitrage models.

"""

from datetime import datetime
from pathlib import Path
import pandas as pd
import wrds
import matplotlib.pyplot as plt
from settings import config

from dateutil.relativedelta import relativedelta
import pandas_market_calendars as mcal

# Load configuration from settings or environment variables
DATA_DIR = Path(config("DATA_DIR"))
WRDS_USERNAME = config("WRDS_USERNAME")
START_DATE = config("START_DATE")
END_DATE = config("END_DATE")


INDEX_LIST = ["SPX", "DJX", "NDX"]


def pull_index_implied_dividend_yield(index_name, start_date=START_DATE, end_date=END_DATE, wrds_username=WRDS_USERNAME):
    """
    Pulls implied dividend yield for a given index from WRDS (OptionMetrics `optionm.idxdvd`).
    
    Parameters:
    - index_name (str): Name of the index (SPX, DJX, NDX)
    - secid (int): SECID corresponding to the index
    - start_date (str): Data start date (YYYY-MM-DD)
    - end_date (str): Data end date (YYYY-MM-DD)
    - wrds_username (str): WRDS username for authentication

    Returns:
    - DataFrame containing date and implied dividend yield.
    """

    query = f"""
     WITH keys_secid AS (
        SELECT secid
        FROM optionm.securd1
        WHERE ticker = '{index_name}'
    ),
    final AS (
        SELECT 
            i.secid, 
            i.date, 
            s.cusip, 
            s.ticker, 
            s.sic, 
            s.index_flag, 
            s.exchange_d, 
            s.class,
            s.issue_type, 
            s.industry_group, 
            i.expiration, 
            i.rate
        FROM optionm.idxdvd i
        JOIN optionm.securd1 s ON s.secid = i.secid
        JOIN keys_secid k ON s.secid = k.secid
        WHERE i.date BETWEEN '{start_date}' AND '{end_date}'
    )
    SELECT 
        secid, date, cusip, ticker, sic, index_flag, 
        exchange_d, class, issue_type, industry_group, 
        expiration, rate 
    FROM final
    ORDER BY date ASC;

    """
      

    # Connect to WRDS and fetch data
    db = wrds.Connection(wrds_username=wrds_username)
    df = db.raw_sql(query, date_cols=["date"])
    db.close()

    # Display DataFrame info
    print(f"\n📌 DataFrame Info for {index_name}:")
    print(df.info())

    # Show first few rows
    print(f"\n📊 First 5 Rows for {index_name}:")
    print(df.head())

    # Show last few rows
    print(f"\n📊 Last 5 Rows for {index_name}:")
    print(df.tail())

    # Save to Parquet
    save_path = Path(DATA_DIR) / f"{index_name}_implied_div_yield.parquet"
    df.to_parquet(save_path)
    print(f"\n✅ Saved {index_name} Implied Dividend Yield to {save_path}")

    # Plot the implied dividend yield trend
    plt.figure(figsize=(12, 6))
    plt.plot(df["date"], df["rate"], label=f"Implied Dividend Yield ({index_name})", color='blue')
    plt.xlabel("Date")
    plt.ylabel("Dividend Yield")
    plt.title(f"{index_name} Implied Dividend Yield Over Time")
    plt.legend()
    plt.grid()
    plt.show()

    return df


def load_index_implied_dividend_yield(index_name, data_dir=DATA_DIR):
    """
    Loads the saved implied dividend yield data for a given index from Parquet.
    
    Parameters:
<<<<<<< HEAD
    - index_name (str): Name of the index (SPX, DJX, MNX)
=======
    - index_name (str): Name of the index (SPX, INDU, NDX)
>>>>>>> 68ab925d8a3458f392c11bb2a1ac0299986fea2c
    - data_dir (Path): Directory where Parquet file is stored

    Returns:
    - DataFrame containing implied dividend yield data.
    """
    path = Path(data_dir) / f"{index_name}_implied_div_yield.parquet"
    df = pd.read_parquet(path)
    print(f"\n📌 Loaded DataFrame Info for {index_name}:")
    print(df.info())

    print(f"\n📊 First 5 Rows for {index_name} (Loaded from Parquet):")
    print(df.head())

    return df

<<<<<<< HEAD
def filter_index_implied_dividend_yield(df, start_date=START_DATE, end_date=END_DATE):
    """
    Filters the DataFrame to include only the first two maturities for each third Friday of March, June, September, and December.
    
    Parameters:
    - df (DataFrame): DataFrame containing implied dividend yield data.
    - start_date (str): Data start date (YYYY-MM-DD)
    - end_date (str): Data end date (YYYY-MM-DD)
    
    Returns:
    - DataFrame containing the filtered implied dividend yield data."""
    # Get all third Fridays & the next day
    # Getting all Saturdays give errors as it is not necessarilly a day after the third Friday
    all_third_fridays = pd.date_range(start=start_date, end=end_date, freq='WOM-3FRI')
    all_third_fridays_tom = pd.DatetimeIndex([all_third_fridays[i]+ pd.Timedelta(days=1)for i in range(len(all_third_fridays))])


    # Filter only the months of March (3), June (6), September (9), and December (12)
    filtered_third_fridays = all_third_fridays[all_third_fridays.month.isin([3, 6, 9, 12])]
    filtered_third_fridays = pd.DatetimeIndex(filtered_third_fridays).tz_localize('UTC')

    filtered_third_fridays_tom = all_third_fridays_tom[all_third_fridays_tom.month.isin([3, 6, 9, 12])]
    filtered_third_fridays_tom = pd.DatetimeIndex(filtered_third_fridays_tom).tz_localize('UTC')

    # Convert to a mutable list
    filtered_third_fridays = list(filtered_third_fridays)
    filtered_third_fridays_tom = list(filtered_third_fridays_tom)

    # Get valid market open days
    ushol = mcal.get_calendar("Financial_Markets_US")
    market_open = ushol.valid_days(start_date=start_date, end_date=end_date)
=======
def get_expiration_dates(start_date: str, end_date: str, expiration_months: list, freq = 'WOM-3FRI') -> list:
    """
    OptionMetrics expiration dates are saved as the day after the third Friday before or around 2017
    Thus, we need to both check the third Friday and the next day to get the expiration date
    If it is non trading day, we need to adjust the date to the previous trading day
    """

    # Get all third fridays in the date range
    all_target_dates = pd.date_range(start=start_date, end=end_date, freq= freq)
    # Get all third fridays that are in the expiration months
    expiration_target_dates= all_target_dates[all_target_dates.month.isin(expiration_months)]
    
    expiration_target_dates = pd.DatetimeIndex(expiration_target_dates).tolist()

    ushol = mcal.get_calendar("Financial_Markets_US")
    market_open = ushol.valid_days(start_date= start_date, end_date= end_date).tz_localize(None)
>>>>>>> 68ab925d8a3458f392c11bb2a1ac0299986fea2c

    # Convert market open dates to a set for faster lookup
    market_open_set = set(market_open)

    # Adjust non-trading days
<<<<<<< HEAD
    for i in range(len(filtered_third_fridays)):
        current_date = filtered_third_fridays[i]
=======
    for i in range(len(expiration_target_dates)):
        current_date = expiration_target_dates[i]
>>>>>>> 68ab925d8a3458f392c11bb2a1ac0299986fea2c
        
        # If the third Friday is not a trading day, shift backward
        while current_date not in market_open_set:
            print(f"Public holiday on {current_date}. Shifting back one day.")
            current_date -= pd.Timedelta(days=1)  # Move one day back
            
        # Update the list with the adjusted date
<<<<<<< HEAD
        print(f"current_date: {current_date}, ")
        filtered_third_fridays[i] = current_date
        filtered_third_fridays_tom[i] = current_date + pd.Timedelta(days=1)

    # Convert back to DatetimeIndex
    filtered_third_fridays = pd.DatetimeIndex(filtered_third_fridays)
    filtered_third_fridays_tom = pd.DatetimeIndex(filtered_third_fridays_tom)

    # Filter only the third Fridays and the next day
    filtered_df = df[df["expiration"].isin(filtered_third_fridays.date) | df["expiration"].isin(filtered_third_fridays_tom.date)]
    
    # Filter the first three maturities for each date (method changed in 2017)
    filtered_df = filtered_df.groupby('date').head(3)


    # IF date == expiration, tail(2), otherwise head(2)
    def conditional_selection(group):
    # Convert 'date' column to datetime.date before subtraction
        date_diff = group['date'].dt.date - group['expiration']  # timedelta object

        # Check if any difference is ≤ 1 day
        if ((pd.Timedelta(days=0) >= date_diff) & (date_diff>= pd.Timedelta(days=-1))).any():
            return group.tail(2)
        else:
            return group.head(2)

    # Apply grouping and selection
    filtered_df = filtered_df.groupby("date", group_keys=False).apply(conditional_selection)

=======
        expiration_target_dates[i] = current_date
    
    # returns a list of expiration dates considering the non-trading days
    return expiration_target_dates

def filter_index_implied_dividend_yield(df, start_date=START_DATE, end_date=END_DATE ):
    """
    Filters the DataFrame to include only the first two maturities for each third Friday of March, June, September, and December.
    Replaces the days after the third Friday with the expiration date.

    Parameters:
    - df (DataFrame): DataFrame containing implied dividend yield data.
    - start_date (str): Data start date (YYYY-MM-DD)
    - end_date (str): Data end date (YYYY-MM-DD)
    
    Returns:
    - DataFrame containing the filtered implied dividend yield data."""

    # Get all third Fridays & the next day
    # Getting all Saturdays give errors as it is not necessarilly a day after the third Friday

    df_ = df.copy()
    all_third_fridays = pd.DatetimeIndex(get_expiration_dates(start_date, end_date, [3, 6, 9, 12]))
    all_third_fridays_tom = pd.DatetimeIndex([all_third_fridays[i]+ pd.Timedelta(days=1)for i in range(len(all_third_fridays))])
    potential_expiration_dates = list(zip(all_third_fridays_tom.date, all_third_fridays.date))
    df_['expiration'] = df_['expiration'].replace(dict(potential_expiration_dates))

    filtered_df = df_[df_["expiration"].isin(all_third_fridays.date)]
    
    # Can be improved to incorporate roll over
    filtered_df = filtered_df.groupby('date').head(2)
    
>>>>>>> 68ab925d8a3458f392c11bb2a1ac0299986fea2c
    return filtered_df



def _demo():
    """
    Runs a test to pull and load implied dividend yield data for SPX, DJX, and NDX.
    And sanity check if we filtered out the most recent two maturities for each date
    """
    for index_name in INDEX_LIST:
        df_div_yield = pull_index_implied_dividend_yield(
            index_name, start_date=START_DATE, end_date=END_DATE
        )

        df_loaded = load_index_implied_dividend_yield(index_name)
        print(f"\n📊 First 5 Rows for {index_name} (Re-loaded DataFrame):")
        print(df_loaded.head())

<<<<<<< HEAD
        df_filtered = filter_index_implied_dividend_yield(df_loaded)
        all_third_fridays = pd.date_range(start=START_DATE, end=END_DATE, freq='WOM-3FRI')
        all_third_fridays_tom = pd.DatetimeIndex([all_third_fridays[i]+ pd.Timedelta(days=1)for i in range(len(all_third_fridays))])


        # Filter only the months of March (3), June (6), September (9), and December (12)
        filtered_third_fridays = all_third_fridays[all_third_fridays.month.isin([3, 6, 9, 12])]
        filtered_third_fridays = pd.DatetimeIndex(filtered_third_fridays).tz_localize('UTC')

        filtered_third_fridays_tom = all_third_fridays_tom[all_third_fridays_tom.month.isin([3, 6, 9, 12])]
        filtered_third_fridays_tom = pd.DatetimeIndex(filtered_third_fridays_tom).tz_localize('UTC')

        # Convert to a mutable list
        filtered_third_fridays = list(filtered_third_fridays)
        filtered_third_fridays_tom = list(filtered_third_fridays_tom)

        # Get valid market open days
        ushol = mcal.get_calendar("Financial_Markets_US")
        market_open = ushol.valid_days(start_date=START_DATE, end_date=END_DATE)

        # Convert market open dates to a set for faster lookup
        market_open_set = set(market_open)

        # Adjust non-trading days
        for i in range(len(filtered_third_fridays)):
            current_date = filtered_third_fridays[i]
            
            # If the third Friday is not a trading day, shift backward
            while current_date not in market_open_set:
                print(f"Public holiday on {current_date}. Shifting back one day.")
                current_date -= pd.Timedelta(days=1)  # Move one day back
                
            # Update the list with the adjusted date
            filtered_third_fridays[i] = current_date
            filtered_third_fridays_tom[i] = current_date + pd.Timedelta(days=1)
        # Convert back to DatetimeIndex
        filtered_third_fridays = pd.DatetimeIndex(filtered_third_fridays)
        filtered_third_fridays_tom = pd.DatetimeIndex(filtered_third_fridays_tom)


        ###
        temp = df_filtered[df_filtered["date"].isin(filtered_third_fridays.date) | df_filtered["date"].isin(all_third_fridays_tom.date)]
        plt.scatter(temp["date"], temp["expiration"], label=f"Implied Dividend Yield {index_name}", color='blue')
        
        
        for i in range(len(filtered_third_fridays)):
            plt.axvline(filtered_third_fridays[i], color='r', linestyle='--')
            plt.axvline(all_third_fridays_tom[i], color='r', linestyle='--')
        plt.show()

=======
>>>>>>> 68ab925d8a3458f392c11bb2a1ac0299986fea2c


if __name__ == "__main__":
    _demo()
    


    
   