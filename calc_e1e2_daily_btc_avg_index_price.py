###
### Calculates e1 and e2 for each daily average BTC price.
###

## IMPORTANT: 
## Is recommended to update the daily average BTC price database, by running the 
## "build_hist_btc_daily_avg_index_price.py" script before to execute this script.

## NOTES:
## The 'datetime' fields from 'btc_avg_index_price_daily' collection are 
## relative to 'São Paulo -3 GMT local time'. As the Deribit's BTC options 
## (daily, weekly and monthly) contracts expires at 8:00 AM GMT (5:00 AM at 
## São Paulo -3 GMT local time), soon the daily average price is calculed to 
## 5am-5am interval, at local time.


import numpy as np
from pymongo import MongoClient, DESCENDING, ASCENDING
from datetime import datetime, timedelta


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
    
    # Time period (days).
    days = np.arange(1, len(prices) + 1)
    
    # Calculates the slope of the least squares line.
    slope, _ = np.polyfit(days, log_prices, 1)
    
    # Calculate the standard deviation of the natural logarithms of the prices.
    # Note: Using ddof=1 for sample standard deviation.
    std_dev = np.std(log_prices, ddof=1)  
    
    return slope, std_dev


###
### Main script
###

# DB access.
client = MongoClient('mongodb://localhost:27017/')
db = client['deribit_btc_options']
collection = db['btc_avg_index_price_daily']

# Defines iteration windows length (in days).
windows_len_list = [30, 90,]

# Gets all documents.
cursor = collection.find().sort("datetime", ASCENDING)
documents = list(cursor)

if documents:

    for n in windows_len_list:

        print(f"Iterating with {str(n)} days windows:")

        # Starts on the 'n - 1' to have 'n' previous days.
        for i in range((n - 1), len(documents)):

            # Gets the data window.
            window_documents = documents[(i - (n - 1)):i+1]
            prices = [doc["avg_index_price_daily"] for doc in window_documents]

            #print(prices)

            # Calculates E1 and E2.
            # Note:
            # - A positive slope indicate that the price is rising over time;
            # - A negative slope indicate that the price is falling over time;
            # - A slope of zero would suggest that the price is stable.
            slope, std_dev = calculate_e1e2(prices)
            e1 = slope      # Measures the trend.
            e2 = std_dev    # Measures the volatility.

            new_fields = {
                          f"e1_{str(n)}d": e1,
                          f"e2_{str(n)}d": e2,
                         }

            # Updates the source collection.
            collection.update_one(
                                  {"_id": documents[i]["_id"]}, 
                                  {"$set": new_fields}
                                 )

            # Shows the job execution on terminal.
            id_doc = str(documents[i]["_id"])
            print(
                  f"id: {id_doc}" + 
                  f" | e1_{str(n)}: {str(e1)} | e2_{str(n)}: {str(e2)}"
                 )

    print("The job is done!")

else:

    print("The data source returned empty!")




