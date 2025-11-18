###
### Calculates e1 and e2 for each historical trade.
###

## IMPORTANT: 
## Is recommended to update the BTC trade history database, by running the 
## "build_hist_btc_options_trades_5min.py" script before to execute this script.

## NOTES:
## The 'datetime' and 'unix_time' fields from 'btc_avg_index_price_daily' 
## collection are relative to 'São Paulo -3 GMT local time'. As the Deribit's 
## BTC options (daily, weekly and monthly) contracts expires at 8:00 AM GMT 
## (5:00 AM at São Paulo -3 GMT local time), soon the daily average price is 
## calculed to 5am-5am interval, at local time.


import numpy as np
from itertools import islice
from pymongo import MongoClient, DESCENDING, ASCENDING


##
## Support functions
##

def calculate_e1e2(prices):
    """
    Calculate the slope of the least squares line (E1) and the standard 
    deviation (E2)of the natural logarithms of the average daily prices for a 
    derivative timeseries over the previous 'n-1' days.

    Parameters:
    prices (list or np.ndarray): Average daily prices over the last 
    'n-1' cycles (days).

    Returns:
    tuple: A tuple containing the slope (E1) and the standard deviation (E2).
    """
    # Ensures the prices are in a numpy array for easy calculations.
    prices = np.array(prices)
    
    # Calculates natural logarithms of the prices.
    log_prices = np.log(prices)
    
    # Time period (months).
    months = np.arange(1, len(prices) + 1)
    
    # Calculates the slope of the least squares line.
    slope, _ = np.polyfit(months, log_prices, 1)
    
    # Calculate the standard deviation of the natural logarithms of the prices.
    # Note: Using ddof=1 for sample standard deviation.
    std_dev = np.std(log_prices, ddof=1)  
    
    return slope, std_dev


def get_data_in_windows(data, length_window):
    """
    Gets timeseries data in movable windows.
    """
    for i in range(len(data) - length_window + 1):
        yield data[i:i + length_window]


###
### Main script
###

# DB access.
client = MongoClient('mongodb://localhost:27017/')
db = client['deribit_btc_options']
collection = db['btc_avg_index_price_daily']

# Defines iteration windows length (in days).
windows_len_list = [30, 90]

# Gets all documents that don't have a 'z' field.
e1_filter = {"e1": {"$exists": False}}
cursor = collection.find(e1_filter).sort("datetime", DESCENDING)

# Converts the cursor to a list, a nedeed step to use islice.
documents = list(cursor)

if documents:

for n in windows_len_list:

    print(f"Iterating with {str(n)} days windows:")

    # Iterates movable data windows with 'n' length.
    for window in get_data_in_windows(documents, n):

        # Each window is a list with n docs. 
        for doc in window:

            # Calculates E1 and E2:
            slope, std_dev = calculate_stock_metrics(window)
            e1 = slope      # Measures the trend.
            e2 = std_dev    # Measures the volatility.

            new_fields = {
                          f"e1_{str(n)}d": e1,
                          f"e2_{str(n)}d": e1,
                         }

            # Updates the source collection.
            collection.update_one({"_id": trade["_id"]}, {"$set": new_fields})

            # Shows the job execution on terminal.
            print(f"id: {str(trade['id'])} | z: {str(z)}")

            print("The job is done!")

else:

    print("There is nothing to do...")











