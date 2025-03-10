import pandas as pd
import os
import glob
import pytest
from settings import config
import pull_optionm_api_data as pull_optionm
import clean_bloomberg as clean_bbg
from datetime import datetime
from dateutil.relativedelta import relativedelta


DATA_DIR = config("DATA_DIR")
START_DATE = config("START_DATE")
END_DATE = config("END_DATE")
WRDS_USERNAME = config("WRDS_USERNAME")
OUTPUT_DIR = config("OUTPUT_DIR")

def test_pull_bloomberg():
    """
    Test if the bloomberg file is properly pulled with the correct columns and index"""
    
    # for now
    df  = pd.read_parquet('../data_manual/bloomberg_historical_data.parquet')
    assert isinstance(df, pd.DataFrame)

    tuples = [
    ('SPX Index', 'PX_LAST'),('NDX Index', 'PX_LAST'),('INDU Index', 'PX_LAST'),
    ('ES1 Index', 'PX_LAST'),('ES2 Index', 'PX_LAST'),('ES3 Index', 'PX_LAST'),('ES4 Index', 'PX_LAST'),
    ('NQ1 Index', 'PX_LAST'),('NQ2 Index', 'PX_LAST'),('NQ3 Index,PX_LAST'),('NQ4 Index', 'PX_LAST'),
    ('DM1 Index', 'PX_LAST'),('DM2 Index', 'PX_LAST'),('DM3 Index', 'PX_LAST'),('DM4 Index', 'PX_LAST'),
    ('USSOC CMPN Curncy', 'PX_LAST')]
    index= pd.MultiIndex.from_tuples(tuples)
    assert index.isin(df.columns).all()
    
    assert df.index[0] == START_DATE.date()
    assert df.index[-1] == END_DATE.date()


def test_clean_bloomberg():
    df_raw = pd.read_parquet('../data_manual/bloomberg_historical_data.parquet')
    start_date = datetime.strftime(config("START_DATE").date()-relativedelta(years=1),format="%Y-%m-%d") 
    end_date = datetime.strftime(config("END_DATE"),format="%Y-%m-%d")

    expiration_months = [3, 6, 9, 12]
    expiration_dates = pull_optionm.get_expiration_dates(start_date, end_date, expiration_months)
    date_ranges = [(start.date(), end.date()) for start, end in zip(expiration_dates, expiration_dates[1:])]
    
    dt = datetime(2009,12,18)
    date_range = clean_bbg.get_adjacent_dates(dt.date(), date_ranges)
    assert date_range == (datetime(2009,12,18).date(), datetime(2010,3,19).date())
    assert clean_bbg.roll_over(dt.date(),date_range,date_ranges,0) == (datetime(2010,3,19).date(), datetime(2010,6,18).date())
    
    spx = ['ES1 Index','ES2 Index','ES3 Index']
    spx_pairs= list(zip(spx,spx[1:]))
    clean = clean_bbg.get_clean_df(df_raw, date_ranges,spx_pairs)
    expected_columns = ['Near Month PX_LAST','Deferred Month PX_LAST']
    assert all(col in clean.columns for col in expected_columns)


def test_pull_optionm_api_data():
    index_name = "SPX"
    optionm_df = pull_optionm.pull_index_implied_dividend_yield(index_name, start_date=START_DATE, end_date=END_DATE, wrds_username=WRDS_USERNAME)
    expected_columns = ['date','expiration','rate']
    assert all(col in optionm_df.columns for col in expected_columns)
    assert isinstance(optionm_df, pd.DataFrame)
    assert optionm_df.index[0] == START_DATE.date()


def test_get_expiration_dates():
    start_date = "2010-01-01"
    end_date = "2010-12-31"
    expiration_months = [3, 6, 9, 12]
    expiration_dates = pull_optionm.get_expiration_dates(start_date, end_date, expiration_months)
    assert expiration_dates[0] == datetime(2010,3,19)
    assert len(expiration_dates) == 4

def test_filter_index_implied_dividend_yield():
    df = pull_optionm.load_index_implied_dividend_yield("SPX")
    filtered_df = pull_optionm.filter_index_implied_dividend_yield(df)
    assert isinstance(filtered_df, pd.DataFrame)

    grouped = filtered_df.groupby("date")[["expiration", "rate"]].count()
    assert (grouped == 2).all().all(), "Not all groups have a count of 2 for 'expiration' and 'rate'."
    assert filtered_df['expiration'].apply(lambda d: d.weekday() != 5).all(), "Some expiration dates fall on Saturday."





def test_SF_spread_correlation():
    expected = pd.read_excel("../data_manual/spread.xlsx")
    check = pd.read_parquet(OUTPUT_DIR/"total_df.parquet")
    expected.set_index("date", inplace=True)
    expected.index = [expected.index[i].date() for i in range(len(expected.index))]
    ndx = pd.concat([expected['Eq_SF_NDAQ'],check['NDX_Spread']],axis=1, join='inner')
    spx = pd.concat([expected['Eq_SF_SPX'],check['SPX_Spread']],axis=1, join='inner')
    djx = pd.concat([expected['Eq_SF_DJIA'],check['DJX_Spread']],axis=1, join='inner')

    assert ndx.corr().iloc[0,1] > 0
    assert spx.corr().iloc[0,1] > 0
    assert djx.corr().iloc[0,1] > 0

def test_jupyter_notebook():
    src_directory = "src"  # Directory to check for Jupyter Notebook files
    # Use glob to find files with .ipynb extension in the src directory
    notebook_files = glob.glob(os.path.join(src_directory, "*.ipynb"))
    assert notebook_files, f"No Jupyter Notebook files found in {src_directory} directory."

def test_latex_report():
    output_dir = "_output"
    # Find all .tex files in the _output directory
    tex_files = glob.glob(os.path.join(output_dir, "*.tex"))
    if not tex_files:
        pytest.skip("No LaTeX report (.tex file) found in _output directory; skipping test.")
    # If at least one .tex file exists, the test passes
    assert len(tex_files) > 0, f"Found {len(tex_files)} .tex file(s) in _output directory."
