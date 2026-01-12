# Quantitative-Momentum


## Purpose:
To identify stocks on the S&P 500 index with the highest quantitative momentum. This algorithm identifies the stocks with the highest momentum based off 1-year, 6-month, 3-month and 1-month Price return percentiles which have been calculated using the price returns provided via IEX Cloud API. A mean percentile score is calculated, this is the value used to rank stock momentum. The top 100 stocks with the highest mean percentile scores are then sorted in the database and written onto an Excel file which will appear in the program directory.
