"""
This file is responsible for cleaning the Bloomberg data that was pulled from the API using xbbg
The data is cleaned by selecting the near month and deferred month PX_LAST for each target date
"""

from datetime import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta


def get_adjacent_dates(target_date: datetime.date, date_ranges: list):
    """
    This function returns the nearest expiration date and the next expiration date as a tuple
    If given a target date, it checks if its in the date_ranges and returns the nearest expiration date (end)
    and the next expiration date(next_end) as a tuple
    """
    for i, (start, end) in enumerate(date_ranges):
        if start <= target_date <= end:
            next_end = date_ranges[i + 1][1] if i + 1 < len(date_ranges) else None
            return end, next_end

    return None, None


def roll_over(
    target_date: datetime.date, date_range: tuple, date_ranges: list, days_before: int
):
    """
    If the time difference between the target date and the nearest expiration date is less than days_before,
    it rolls over to the next expiration date
    This takes date_range as a input parameter, output of get_adjacent_dates
    """

    if date_range[0] - target_date <= pd.Timedelta(days=days_before):
        return get_adjacent_dates(
            target_date + relativedelta(days=days_before + 1), date_ranges
        )

    else:
        return date_range


def get_clean_df(df_raw: pd.DataFrame, date_ranges: list, index_pairs: list):
    """
    This takes the bloomberg raw date pulled out and saved a parquet file and returns a clean dataframe
    For each target date it will return the near month PX_LAST and the deferred month PX_LAST
    but on the rollover date it will return the PX_LAST of the deferred month

    """

    df = pd.DataFrame(columns=["Near Month PX_LAST", "Deferred Month PX_LAST"])

    for i in range(len(df_raw.index)):
        target_date = df_raw.index[i]
        range_ = get_adjacent_dates(target_date, date_ranges)
        near_month, deferred_month = roll_over(target_date, range_, date_ranges, 0)

        if range_ == (near_month, deferred_month):
            df.loc[target_date] = [
                df_raw[index_pairs[0][0]]["PX_LAST"].loc[target_date],
                df_raw[index_pairs[0][1]]["PX_LAST"].loc[target_date],
            ]

        else:
            df.loc[target_date] = [
                df_raw[index_pairs[1][0]]["PX_LAST"].loc[target_date],
                df_raw[index_pairs[1][1]]["PX_LAST"].loc[target_date],
            ]
            # print("rolled over")
            # print(target_date,range_,( near_month, deferred_month))
    df.dropna(inplace=True)
    return df


if __name__ == "__main__":
    df_raw = pd.read_parquet("../data_manual/bloomberg_historical_data.parquet")
