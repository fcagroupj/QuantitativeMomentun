#       Quantitative_Momentun02.py
# This script calculates the High Quality Momentum (HQM) scores for S&P 500 stocks
#   and saves the results to an Excel file.
#   It uses the stock_data_api module to fetch stock prices.
#   The script computes price returns over multiple time frames,
#   calculates momentum percentiles, and determines the number of shares to buy  
#   based on a fixed portfolio size.
#   Finally, it selects the top 50 stocks with the highest HQM scores.
#   It handles data retrieval, calculations, and output formatting.
  
import pandas as pd 
import numpy as np
import math 
from scipy.stats import percentileofscore as score 
import datetime
import stock_data_api as sda    
import logging
logging.getLogger("yfinance").setLevel(logging.CRITICAL)

#Import list of stocks
stocks_sp500 = pd.read_csv('./QuantitativeMomentum/sp_500_stocks.csv')
# print(stocks_sp500)

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]   
## Creating list of symbol strings for batch API calls        
symbol_groups = list(chunks(stocks_sp500['Ticker'], 100))
symbol_strings = []
for i in range(0, len(symbol_groups)):
    symbol_strings.append(','.join(symbol_groups[i]))


hqm_columns = [
                'Ticker', 
                'Price', 
                'Number of Shares to Buy', 
                'One-Year Price Return', 
                'One-Year Return Percentile',
                'Six-Month Price Return',
                'Six-Month Return Percentile',
                'Three-Month Price Return',
                'Three-Month Return Percentile',
                'One-Month Price Return',
                'One-Month Return Percentile',
                'HQM Score'
                ]

# Initialize DataFrame with explicit dtypes to avoid concat/NA dtype warnings
hqm_dtype_map = {
    'Ticker': 'string',
    'Price': 'float64',
    'Number of Shares to Buy': 'Int64',  # nullable integer
    'One-Year Price Return': 'float64',
    'One-Year Return Percentile': 'float64',
    'Six-Month Price Return': 'float64',
    'Six-Month Return Percentile': 'float64',
    'Three-Month Price Return': 'float64',
    'Three-Month Return Percentile': 'float64',
    'One-Month Price Return': 'float64',
    'One-Month Return Percentile': 'float64',
    'HQM Score': 'float64',
}
hqm_dataframe = pd.DataFrame({col: pd.Series(dtype=dtype) for col, dtype in hqm_dtype_map.items()})
## get price in different time frames and populate hqm_dataframe
for symbol_string in symbol_strings:
    #print('symbol_string', symbol_string)
    for symbol in symbol_string.split(','):
        #print('symbol', symbol)
        today = datetime.date.today()
        price_0d, ts_et_0d, used_day_0d = sda.get_latest_price_for_day(symbol, today)
        if(price_0d is None): 
            continue
        # one year return
        one_year_ago = datetime.date.today() - datetime.timedelta(days=365)
        price_1y, used_date_1y = sda.get_daily_close_on_or_before(symbol, one_year_ago)
        # Compute one-year price return: (current / one_year) - 1
        one_year_return = np.nan
        try:
            if price_0d is not None and price_1y is not None and price_1y != 0:
                one_year_return = (price_0d / price_1y) - 1
        except Exception:
            one_year_return = np.nan
        # six month return
        six_months_ago = datetime.date.today() - datetime.timedelta(days=182)
        price_6m, used_date_6m = sda.get_daily_close_on_or_before(symbol, six_months_ago)
        # Compute six-month price return: (current / six_months) - 1
        six_month_return = np.nan
        try:
            if price_0d is not None and price_6m is not None and price_6m != 0:
                six_month_return = (price_0d / price_6m) - 1
        except Exception:
            six_month_return = np.nan
        # three month return
        three_months_ago = datetime.date.today() - datetime.timedelta(days=91)
        price_3m, used_date_3m = sda.get_daily_close_on_or_before(symbol, three_months_ago)
        # Compute three-month price return: (current / three_months) - 1
        three_month_return = np.nan
        try:
            if price_0d is not None and price_3m is not None and price_3m != 0:
                three_month_return = (price_0d / price_3m) - 1
        except Exception:
            three_month_return = np.nan
        # one month return
        one_months_ago = datetime.date.today() - datetime.timedelta(days=30)
        price_1m, used_date_1m = sda.get_daily_close_on_or_before(symbol, one_months_ago)
        # Compute one-month price return: (current / one_months) - 1
        one_month_return = np.nan
        try:
            if price_0d is not None and price_1m is not None and price_1m != 0:
                one_month_return = (price_0d / price_1m) - 1
        except Exception:
            one_month_return = np.nan
        hqm_dataframe.loc[len(hqm_dataframe)] = [
            symbol,
            price_0d,
            pd.NA,
            one_year_return,
            np.nan,
            six_month_return,
            np.nan,
            three_month_return,
            np.nan,
            one_month_return,
            np.nan,
            np.nan
        ]
        if(len(hqm_dataframe) % 20 == 0):
            print(f"Processed {len(hqm_dataframe)} stocks so far...")
        # break       # test only first symbol in first chunk
    # break
#print(hqm_dataframe)
## Calculating Momentum Percentiles
time_periods = [
                'One-Year',
                'Six-Month',
                'Three-Month',
                'One-Month'
                ]

for row in hqm_dataframe.index:
    for time_period in time_periods:
    
        change_col = f'{time_period} Price Return'
        percentile_col = f'{time_period} Return Percentile'
        if pd.isna(hqm_dataframe.loc[row, change_col]):
            hqm_dataframe.loc[row, change_col] = 0.0

for row in hqm_dataframe.index:
    for time_period in time_periods:
    
        change_col = f'{time_period} Price Return'
        percentile_col = f'{time_period} Return Percentile'

        hqm_dataframe.loc[row, percentile_col] = score(hqm_dataframe[change_col], hqm_dataframe.loc[row, change_col])/100
## Calculating the Number of Shares to Buy
portfolio_size = 1000000
position_size = float(portfolio_size) / len(hqm_dataframe.index)
for i in range(0, len(hqm_dataframe['Ticker'])):
    hqm_dataframe.loc[i, 'Number of Shares to Buy'] = math.floor(position_size / hqm_dataframe['Price'][i])
    #print(i, hqm_dataframe.loc[i, 'Number of Shares to Buy'], hqm_dataframe['Price'][i], position_size)

## Calculating the HQM Score
from statistics import mean

for row in hqm_dataframe.index:
    momentum_percentiles = []
    for time_period in time_periods:
        momentum_percentiles.append(hqm_dataframe.loc[row, f'{time_period} Return Percentile'])
    hqm_dataframe.loc[row, 'HQM Score'] = mean(momentum_percentiles)

## Selecting the 50 Best Momentum Stocks
hqm_dataframe.sort_values(by = 'HQM Score', ascending = False,inplace= True)
hqm_dataframe = hqm_dataframe[:51]
hqm_dataframe.reset_index(drop = True, inplace = True)

#Print the entire DataFrame    
# Save hqm_dataframe to an Excel file
output_filename = f"./reports/hqm_dataframe_{datetime.date.today()}g.xlsx"
writer = pd.ExcelWriter(output_filename, engine="xlsxwriter")
hqm_dataframe.to_excel(writer, sheet_name="Data", index=False)
writer.close()
print(f"Saved Excel file: {output_filename}")
