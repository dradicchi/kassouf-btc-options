###
### Inspects for opportunities in the BTC day options the order book.
###

## IMPORTANT: 
## Is recommended to update all the historic databases, at least once time a 
## day (running "run_build_scripts.py"), before to execute this script.

import datetime
import time
import requests
import json
from pymongo import MongoClient, DESCENDING, ASCENDING
import math
import numpy as np
from mpmath import mp
from scipy.stats import linregress


# Sets a timer to control script performance.
start_time = time.time()
# Sets todat date to control database querying.
td = datetime.datetime.now()


# Sets z model:
z_model = [-6.11354395e+03,
           7.09803277e+05, 
           1.98758286e+04, 
           2.41836199e+02, 
           6.73891565e+03, 
           -6.07046185e-04]


## API definitions
prod_base_url = "https://deribit.com/api/v2/public/"
order_book_endpoint = f"get_order_book"
index_price_endpoint = f"get_index_price"


## DB access.
client = MongoClient('mongodb://localhost:27017/')
db = client['deribit_btc_options']
index_price_collection = db['btc_index_price_hourly']
index_price_d_avg_collection = db['btc_avg_index_price_daily']
instrument_collection = db['btc_day_inverse_options_offer']
order_book_opportunities_collection = db['btc_day_options_order_book_opportunities']


##
## Calculating BTC trend (E1) and volatility (E2) indexes
##

## DEPRECATED
# Gets the last 90 values of the index price 90-day moving average, including 
# today's partial values.
# First, gets the today's partial index price average.
# pipeline = [
#     {'$match': 
#         {'datetime': {'$gte': td, 
#                       '$lt': (td + datetime.timedelta(days=1))}}},
#     {'$group': {
#         '_id': None,
#         'sum_price': {'$sum': '$index_price'},
#         'count': {'$sum': 1}
#     }}
# ]
# result = list(index_price_collection.aggregate(pipeline))
# td_sum_price = float(result[0]['sum_price'])
# td_count = int(result[0]['count'])
# td_partial_average = sum_price / count

# Gets the current index price:
index_price_params = {
                      'index_name': "btc_usd",                 
                     }

response = requests.get(f"{prod_base_url}{index_price_endpoint}", 
                        params=index_price_params)

if response.status_code == 200:
    data = response.json()
    cur_index_price = float(data['result']['index_price'])

# Gets the 179 most recent documents and retrieves only the daily price average
# field.
avg_prices_list = index_price_d_avg_collection.find({}, 
                                        {'_id': 0, 'avg_index_price_daily': 1}
                                        ).sort('datetime', -1).limit(179)
prices_list = [doc['avg_index_price_daily'] for doc in avg_prices_list]
prices_ascending = prices_list[::-1]

# A list with the 180 most recent index price daily average.
prices_ascending.append(cur_index_price)

# The 90 most recent values (in ascending order) to the index price 90-day 
# moving average.
mov_avg_90d_list = np.convolve(prices_ascending, np.ones(90)/90, mode='valid')
mov_avg_90d = mov_avg_90d_list[-90:]

# Computes the natural logarithms of average daily prices.
log_prices = np.log(mov_avg_90d)
days = np.arange(1, 91)

# Computes the linear regression to find the slope (E1 - Trendline Slope).
slope, intercept, r_value, p_value, std_err = linregress(days, log_prices)
e1 = slope

# Computes the standard deviation of the logarithms of the daily average prices 
# (E2 - Standard Deviation of Price Logarithms - Sazonality). 
e2 = np.std(log_prices)


##
## Starting the opportunities inspection.
##

# Gets the active BTC day options offer:
active_instruments = instrument_collection.find(
                    {'is_active': True, 'option_type': "call"}).sort("expiration_datetime", ASCENDING)

for instrument in active_instruments:

    ##
    ## Calculating the instrument fair price
    ##

    # Gets t (time left).
    td_unix = int(time.mktime(td.timetuple()) * 1000)
    t = instrument['expiration_unix_timestamp'] - td_unix

    # Gets life-time status (in milliseconds):
    life_time_portion = ((td_unix - instrument['creation_unix_timestamp']) / 
                         (instrument['expiration_unix_timestamp'] - 
                          instrument['creation_unix_timestamp']))

    # Gets logXdivXavg:
    logXdivXavg = math.log(cur_index_price / (sum(mov_avg_90d) / 90))

    # Gets x:
    x = cur_index_price / instrument['strike']

    # Gets z:
    z = mp.mpf(z_model[0] + 
               (z_model[1] * (1 / t)) + 
               (z_model[2] * e1) + 
               (z_model[3] * e2) + 
               (z_model[4] * x) + 
               (z_model[5] * instrument['strike']))

    # Gets y = ((1 + (x**z))**(1/z)) - 1):
    try:
        term = mp.power(x, z)  # x**z
        term = mp.power(1 + term, 1 / z)  # (1 + (x**z))**(1 / z)
        y = term - 1
    except: 
        y = None

    # The calculed fair price to the instrument.
    if y:
        Y = y * instrument['strike']
    else:
        Y = None


    ##
    ## Exploring the order book
    ##

    # Gets the current order book:
    order_book_params = {
                         'instrument_name': instrument['instrument_name'],
                         'depth': 100 # [1, 5, 10, 20, 50, 100, 1000, 10000]                      
                        }

    response = requests.get(f"{prod_base_url}{order_book_endpoint}", 
                            params=order_book_params)

    if response.status_code == 200:
        current_order_book = response.json()

    # Gets the current open bids:
    bids = current_order_book['result']['bids']

    if bids:
        for bid in bids:

            if Y:
                # Gets the ratio between the observed price (for the instruemnt) and
                # the calculed fair price.
                YdivY = float(bid[0]) / Y
            else:
                YdivY = None

            if YdivY and (YdivY >= 100.0):

                # Inits the historical opportunities database.
                last_document = order_book_opportunities_collection.find_one(
                                                        sort=[('datetime', -1)])
                if last_document is None:
                    order_book_opportunities_collection.create_index(
                                                    [('datetime', DESCENDING)])
                    order_book_opportunities_collection.create_index(
                                                    [('datetime', ASCENDING)])

                new_opp = {
                             'datetime': td,
                             'unix_datetime': td_unix,
                             'instrument' : instrument['instrument_name'],
                             'z_model' : "001",
                             'adj_model' : "YdivY>=380.0",
                             'vars' : {
                                 't' : (1 / t),
                                 'e1' : e1,
                                 'e2' : e2,
                                 'x' : x,
                                 'strike' : instrument['strike'],
                                 'life_time_portion' : life_time_portion,
                                 'logXdivXavg' : logXdivXavg,
                                 'iv' : current_order_book['result']['mark_iv'],
                                 'YdivY' : str(YdivY),
                                 'z' : str(z),
                                 },
                            'index_price': cur_index_price,
                            'instrument_fair_price' : str(Y),
                            'bid' : bid[0],
                            'amount' : bid[1],
                            }

                # Insert the document into the collection
                order_book_opportunities_collection.insert_one(new_opp)

                # Shows the job execution on terminal.
                print(td, 
                      instrument['instrument_name'],
                      life_time_portion,
                      YdivY,
                      bid[1],
                      bid[0])  

# Logs the execution time.
end_time = time.time()
execution_time = end_time - start_time
print(f"A new order book analysis cycle has been completed! - Exec Time: {execution_time}")

















