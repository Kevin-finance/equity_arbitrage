import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import calendar
from datetime import datetime
from pathlib import Path

# Dynamically set project root using sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent))

# =============================================================================
# 1. Load and Prepare Data
# =============================================================================
file_path = Path(__file__).resolve().parent.parent / "_data/bloomberg_historical_data.parquet"
df = pd.read_parquet(file_path)
# Ensure columns are named properly (if MultiIndex)
if isinstance(df.columns, pd.MultiIndex):
    df.columns = [' '.join(col).strip() for col in df.columns]
df.index = pd.to_datetime(df.index)

# --- Extract Spot Data ---
spot_df = df[[
    "SPX Index PX_LAST", "SPX Index INDX_GROSS_DAILY_DIV",
    "NDX Index PX_LAST", "NDX Index INDX_GROSS_DAILY_DIV",
    "INDU Index PX_LAST", "INDU Index INDX_GROSS_DAILY_DIV"
]].copy()
spot_df = spot_df.rename(columns={
    "SPX Index PX_LAST": "SPX_Spot",
    "SPX Index INDX_GROSS_DAILY_DIV": "SPX_Div",
    "NDX Index PX_LAST": "NDX_Spot",
    "NDX Index INDX_GROSS_DAILY_DIV": "NDX_Div",
    "INDU Index PX_LAST": "DJI_Spot",
    "INDU Index INDX_GROSS_DAILY_DIV": "DJI_Div"
})
for col in ["SPX_Spot", "SPX_Div", "NDX_Spot", "NDX_Div", "DJI_Spot", "DJI_Div"]:
    spot_df[col] = pd.to_numeric(spot_df[col], errors="coerce")
# Fill missing dividend values with 0
for col in ["SPX_Div", "NDX_Div", "DJI_Div"]:
    spot_df[col] = spot_df[col].fillna(0)

# --- Extract Futures Data ---
futures_cols = [
    "ES1 Index PX_LAST", "ES2 Index PX_LAST", 
    "ES1 Index CURRENT_CONTRACT_MONTH_YR", "ES2 Index CURRENT_CONTRACT_MONTH_YR",
    "NQ1 Index PX_LAST", "NQ2 Index PX_LAST", 
    "NQ1 Index CURRENT_CONTRACT_MONTH_YR", "NQ2 Index CURRENT_CONTRACT_MONTH_YR",
    "DM1 Index PX_LAST", "DM2 Index PX_LAST", 
    "DM1 Index CURRENT_CONTRACT_MONTH_YR", "DM2 Index CURRENT_CONTRACT_MONTH_YR"
]
futures_df = df[futures_cols].copy()
futures_df = futures_df.rename(columns={
    "ES1 Index PX_LAST": "SPX_F1",
    "ES2 Index PX_LAST": "SPX_F2",
    "ES1 Index CURRENT_CONTRACT_MONTH_YR": "SPX_Contract",
    "ES2 Index CURRENT_CONTRACT_MONTH_YR": "SPX_Contract2",
    "NQ1 Index PX_LAST": "NDX_F1",
    "NQ2 Index PX_LAST": "NDX_F2",
    "NQ1 Index CURRENT_CONTRACT_MONTH_YR": "NDX_Contract",
    "NQ2 Index CURRENT_CONTRACT_MONTH_YR": "NDX_Contract2",
    "DM1 Index PX_LAST": "DJI_F1",
    "DM2 Index PX_LAST": "DJI_F2",
    "DM1 Index CURRENT_CONTRACT_MONTH_YR": "DJI_Contract",
    "DM2 Index CURRENT_CONTRACT_MONTH_YR": "DJI_Contract2"
})
for col in ["SPX_F1", "SPX_F2", "NDX_F1", "NDX_F2", "DJI_F1", "DJI_F2"]:
    futures_df[col] = pd.to_numeric(futures_df[col], errors="coerce")
# Replace contract fields equal to ".NA." with NaN
for col in ["SPX_Contract", "SPX_Contract2", "NDX_Contract", "NDX_Contract2", "DJI_Contract", "DJI_Contract2"]:
    futures_df[col] = futures_df[col].replace({".NA.": np.nan})

# --- Extract OIS Data for Multiple Tenors ---
# Assume these Bloomberg fields are available:
ois_1w = df["USSO1Z CMPN Curncy PX_LAST"].copy() / 100
ois_1w.name = "OIS_1W"
ois_1m = df["USSOA CMPN Curncy PX_LAST"].copy() / 100
ois_1m.name = "OIS_1M"
ois_3m = df["USSOC CMPN Curncy PX_LAST"].copy() / 100
ois_3m.name = "OIS_3M"
ois_6m = df["USSOF CMPN Curncy PX_LAST"].copy() / 100
ois_6m.name = "OIS_6M"
ois_1y = df["USSO1 CMPN Curncy PX_LAST"].copy() / 100
ois_1y.name = "OIS_1Y"

# Ensure indices are datetime
spot_df.index = pd.to_datetime(spot_df.index)
futures_df.index = pd.to_datetime(futures_df.index)
for series in [ois_1w, ois_1m, ois_3m, ois_6m, ois_1y]:
    series.index = pd.to_datetime(series.index)

# Merge all OIS tenor series with the spot and futures data.
merged_df = pd.concat([spot_df, futures_df, ois_1w, ois_1m, ois_3m, ois_6m, ois_1y], axis=1)
# Drop rows missing essential data
merged_df = merged_df.dropna(subset=["SPX_Spot", "SPX_F1", "SPX_F2",
                                      "NDX_F1", "NDX_F2",
                                      "DJI_F1", "DJI_F2",
                                      "SPX_Contract", "SPX_Contract2",
                                      "NDX_Contract", "NDX_Contract2",
                                      "DJI_Contract", "DJI_Contract2"])

# =============================================================================
# 2. Define Perfect Foresight Dividend Function
# =============================================================================
def compute_expected_dividend(df, div_col, contract_col):
    """
    Computes the perfect foresight dividend series from a gross daily dividend (in dollars).
    
    Grouping by the contract resets the cumulative dividend at each rollover.
    
    - daily_div: The gross daily dividend (in dollars), used directly.
    - cum_div: The cumulative sum of daily dividends within each contract period.
    - total_div: The total dividend expected over the contract period.
    - exp_tau1: Expected dividend remaining in the current contract = total_div - cum_div.
    - exp_tau2: Expected dividend until the next contract = exp_tau1 plus the full dividend of the next contract.
    
    Returns:
      (exp_tau1, exp_tau2, daily_div) as Series.
    """
    daily_div = df[div_col]
    cum_div = daily_div.groupby(df[contract_col]).cumsum()
    total_div = daily_div.groupby(df[contract_col]).transform("sum")
    exp_tau1 = total_div - cum_div
    unique_contracts = df[contract_col].unique()
    sorted_contracts = sorted(unique_contracts)
    contract_total_map = df.groupby(contract_col)[div_col].apply(lambda x: x.sum())
    next_div = df[contract_col].map({
        c: contract_total_map[sorted_contracts[i+1]] if i < len(sorted_contracts) - 1 else 0 
        for i, c in enumerate(sorted_contracts)
    })
    exp_tau2 = exp_tau1 + next_div
    return exp_tau1, exp_tau2, daily_div, total_div

# =============================================================================
# 3. Define Helper Function: Convert Contract String to Maturity Date
# =============================================================================
def contract_to_maturity(contract_str):
    """
    Converts a contract string (e.g., "DEC2023", "DEC23", or "DEC 10") to a maturity date.
    Assumes the maturity is the third Friday of the specified month.
    """
    month_map = {"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
                 "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12}
    contract_str = str(contract_str).strip().upper()
    if len(contract_str) < 5:
        return pd.NaT
    month_str = contract_str[:3]
    year_str = contract_str[3:].replace(" ", "")
    month = month_map.get(month_str, None)
    if month is None:
        return pd.NaT
    try:
        year = int(year_str) if len(year_str) == 4 else int("20" + year_str)
    except ValueError:
        return pd.NaT
    cal = calendar.Calendar(firstweekday=calendar.MONDAY)
    fridays = [day for day in cal.itermonthdates(year, month) if day.month == month and day.weekday() == 4]
    if len(fridays) >= 3:
        return pd.Timestamp(fridays[2])
    else:
        return pd.Timestamp(fridays[-1])

# =============================================================================
# 4. Compute Time-to-Maturity (TTM) Using Separate Contract Fields for Each Index
# =============================================================================
# For SPX:
merged_df["SPX_TTM1"] = merged_df.apply(lambda row: (contract_to_maturity(row["SPX_Contract"]) - row.name).days, axis=1)
merged_df["SPX_TTM2"] = merged_df.apply(lambda row: (contract_to_maturity(row["SPX_Contract2"]) - row.name).days, axis=1)
# For NDX:
merged_df["NDX_TTM1"] = merged_df.apply(lambda row: (contract_to_maturity(row["NDX_Contract"]) - row.name).days, axis=1)
merged_df["NDX_TTM2"] = merged_df.apply(lambda row: (contract_to_maturity(row["NDX_Contract2"]) - row.name).days, axis=1)
# For DJI:
merged_df["DJI_TTM1"] = merged_df.apply(lambda row: (contract_to_maturity(row["DJI_Contract"]) - row.name).days, axis=1)
merged_df["DJI_TTM2"] = merged_df.apply(lambda row: (contract_to_maturity(row["DJI_Contract2"]) - row.name).days, axis=1)

# Drop rows where TTM2 cannot be computed
for idx in ["SPX", "NDX", "DJI"]:
    merged_df = merged_df.dropna(subset=[f"{idx}_TTM2"])

# =============================================================================
# 5. Compute Perfect Foresight Dividends for All Indexes
# =============================================================================
# For τ₁ and τ₂, we now use the respective contract fields:
# For current contract dividend, we use e.g., SPX_Contract;
# For next contract dividend, we assume it is computed separately.
for idx in ["SPX", "NDX", "DJI"]:
    # τ₁ from the current contract:
    exp_tau1, _, _, _ = compute_expected_dividend(merged_df, div_col=f"{idx}_Div", contract_col=f"{idx}_Contract")
    # τ₂ from the deferred contract:
    _, _, _, total_div = compute_expected_dividend(merged_df, div_col=f"{idx}_Div", contract_col=f"{idx}_Contract2")
    merged_df[f"{idx}_exp_tau1"] = exp_tau1
    merged_df[f"{idx}_exp_tau2"] = exp_tau1 + total_div

# =============================================================================
# 6. Compute Interpolated OIS Rates for Each Observation
# =============================================================================
def interpolate_ois(ttm, ois1w, ois1m, ois3m, ois6m, ois1y):
    """
    Linearly interpolates OIS rates based on TTM (in days).
    """
    if ttm <= 7:
        return ois1w
    elif ttm <= 30:
        return ((30 - ttm) / 23) * ois1w + ((ttm - 7) / 23) * ois1m
    elif ttm <= 90:
        return ((90 - ttm) / 60) * ois1m + ((ttm - 30) / 60) * ois3m
    elif ttm <= 180:
        return ((180 - ttm) / 90) * ois3m + ((ttm - 90) / 90) * ois6m
    elif ttm <= 360:
        return ((360 - ttm) / 180) * ois6m + ((ttm - 180) / 180) * ois1y
    else:
        return np.nan

# For each observation, compute OIS1 based on TTM1 and OIS2 based on TTM2.
# We use row-level values from the corresponding OIS tenor columns.
for idx in ["SPX", "NDX", "DJI"]:
    merged_df[f"{idx}_OIS1"] = merged_df.apply(lambda row: interpolate_ois(
        row[f"{idx}_TTM1"], 
        row["OIS_1W"], row["OIS_1M"], row["OIS_3M"], row["OIS_6M"], row["OIS_1Y"]
    ), axis=1)
    merged_df[f"{idx}_OIS2"] = merged_df.apply(lambda row: interpolate_ois(
        row[f"{idx}_TTM2"], 
        row["OIS_1W"], row["OIS_1M"], row["OIS_3M"], row["OIS_6M"], row["OIS_1Y"]
    ), axis=1)

# =============================================================================
# 7. Compute Compounded Dividends, Implied Forward Rates, and Annualize
# =============================================================================
# Apply the compounding adjustment using the interpolated OIS rates:
for idx in ["SPX", "NDX", "DJI"]:
    if idx == "SPX":
        TTM1 = merged_df["SPX_TTM1"]
        TTM2 = merged_df["SPX_TTM2"]
    elif idx == "NDX":
        TTM1 = merged_df["NDX_TTM1"]
        TTM2 = merged_df["NDX_TTM2"]
    else:  # DJI
        TTM1 = merged_df["DJI_TTM1"]
        TTM2 = merged_df["DJI_TTM2"]
        
    comp_factor_tau1 = (((TTM1 / 2) / 360) * merged_df[f"{idx}_OIS1"] + 1)
    comp_factor_tau2 = (((TTM2 / 2) / 360) * merged_df[f"{idx}_OIS2"] + 1)
    merged_df[f"{idx}_exp_tau1_comp"] = merged_df[f"{idx}_exp_tau1"] * comp_factor_tau1
    merged_df[f"{idx}_exp_tau2_comp"] = merged_df[f"{idx}_exp_tau2"] * comp_factor_tau2
    
    merged_df[f"{idx}_implied_forward_raw"] = (
        (merged_df[f"{idx}_F2"] + merged_df[f"{idx}_exp_tau2_comp"]) /
        (merged_df[f"{idx}_F1"] + merged_df[f"{idx}_exp_tau1_comp"]) - 1
    )
    # Annualize and convert to basis points (multiplier = 10000)
    merged_df[f"{idx}_annualized_forward_bps"] = merged_df[f"{idx}_implied_forward_raw"] * (360/(TTM2 - TTM1)) * 10000
    merged_df[f"{idx}_OIS_bps"] = merged_df[f"{idx}_OIS1"] * 10000  # Using OIS1 as the benchmark
    merged_df[f"{idx}_arb_spread"] = merged_df[f"{idx}_annualized_forward_bps"] - merged_df[f"{idx}_OIS_bps"]

# =============================================================================
# 8. Outlier Cleanup: Remove Extreme Values Using a 45-Day Rolling Window
# =============================================================================
for idx in ["SPX", "NDX", "DJI"]:
    arb_series = merged_df[f"{idx}_arb_spread"]
    rolling_median = arb_series.rolling(window='45D', center=True).median()
    abs_dev = (arb_series - rolling_median).abs()
    rolling_mad = abs_dev.rolling(window='45D', center=True).mean()
    outliers = (abs_dev / rolling_mad) >=5
    merged_df.loc[outliers, f"{idx}_annualized_forward_bps"] = np.nan
    merged_df[f"{idx}_arb_spread"] = merged_df[f"{idx}_annualized_forward_bps"] - merged_df[f"{idx}_OIS_bps"]

# =============================================================================
# 9. Remove All Missing Values to Avoid Discontinuities in the Plot
# =============================================================================
merged_df = merged_df.dropna(subset=["SPX_arb_spread", "NDX_arb_spread", "DJI_arb_spread"])

# =============================================================================
# 10. Plot the Arbitrage Spreads for All Indexes
# =============================================================================
plt.figure(figsize=(8, 6))
plt.rcParams["font.family"] = "Times New Roman"
plt.plot(merged_df.index, merged_df["SPX_arb_spread"], label="SPX", color="blue", linewidth=1)
plt.plot(merged_df.index, merged_df["DJI_arb_spread"], label="DJI", color="orange", linewidth=1)
plt.plot(merged_df.index, merged_df["NDX_arb_spread"], label="NDAQ", color="green", linewidth=1)
plt.xlabel("Dates", fontsize=14)
plt.xlim([datetime(2009, 12, 1), datetime(2020, 3, 1)])
plt.ylim([-58, 150])
plt.yticks(np.arange(-50, 151, 50))
plt.gca().yaxis.set_tick_params(rotation=90, labelsize=12)
plt.xticks(fontsize=12)
plt.gca().xaxis.set_major_locator(mdates.YearLocator(2))
plt.ylabel("Arbitrage Spread (bps)", fontsize=14)
plt.title("(c) Equity-Spot Futures", fontsize=14)
plt.grid(axis="y", linestyle="--", alpha=0.6)
plt.legend(fontsize=10, loc="lower right")
plt.gca().spines["top"].set_visible(False)
plt.gca().spines["right"].set_visible(False)
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%-m/%-d/%Y'))
plt.tight_layout()
plt.show()

# =============================================================================
# 11. (Optional) Inspect a Subset of the Results
# =============================================================================
cols = [
    "SPX_arb_spread", 
    "NDX_arb_spread",
    "DJI_arb_spread"
]

# Give general statistics for the arbitrage spread from 2000 to 2021
merged_df[cols].describe()

