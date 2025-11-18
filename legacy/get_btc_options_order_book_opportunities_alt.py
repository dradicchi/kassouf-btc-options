####
#### Gets BTC (daily and weekly) options pportunities in the order book.
####

### IMPORTANT: 
### Is recommended to update all the historic local databases, at least once 
### time a day (running "run_build_scripts.py"), before to execute this script.


###
### Libraries & Requeriments
###

import datetime
import time
import requests
import json
from pymongo import MongoClient, DESCENDING, ASCENDING
import math
import numpy as np
from mpmath import mp
from scipy.stats import linregress


## Starts the runtime control.
start_time = time.time()


###
### External I/O sources definitions
###

## Deribit API.
prod_base_url = "https://deribit.com/api/v2/public/"
# Endpoints:
order_book_endpoint = f"get_order_book"
index_price_endpoint = f"get_index_price"

## Local DB access.
client = MongoClient('mongodb://localhost:27017/')
db = client['deribit_btc_options']
# Collections:
index_price_collection = db['btc_index_price_hourly']
index_price_d_avg_collection = db['btc_avg_index_price_daily']
instrument_collection = db['btc_day_inverse_options_offering']
z_model_collection = db['btc_day_z_models']
proposed_trades_collection = db['btc_day_options_proposed_trades_alt']


### Gets today date.
td = datetime.datetime.now()
td_00h = datetime.datetime(td.year, td.month, td.day, 0, 0, 0)


### Inits the proposed trades database.
have_documents = proposed_trades_collection.find_one(sort=[('datetime', -1)])
if have_documents is None:
    proposed_trades_collection.create_index([('datetime', DESCENDING)])
    proposed_trades_collection.create_index([('datetime', ASCENDING)])


###
### Calculating BTC trend (E1) and volatility (E2) indexes
###

# Calculates today's partial average BTC index price.
pipeline = [
    {'$match': 
        {'datetime': {'$gte': td_00h, 
                      '$lt': (td_00h + datetime.timedelta(days=1))}}},
    {'$group': {
        '_id': None,
        'sum_price': {'$sum': '$index_price'},
        'count': {'$sum': 1}
    }}
]
result = list(index_price_collection.aggregate(pipeline))
td_sum_price = float(result[0]['sum_price'])
td_count = int(result[0]['count'])
td_partial_average = td_sum_price / td_count

# Gets the 179 last values to the BTC index price daily average database.
avg_prices_list = index_price_d_avg_collection.find({}, 
                                        {'_id': 0, 'avg_index_price_daily': 1}
                                        ).sort('datetime', -1).limit(179)
prices_list = [doc['avg_index_price_daily'] for doc in avg_prices_list]
prices_ascending = prices_list[::-1]

# A list with the 180 most recent index price daily average.
prices_ascending.append(td_partial_average)

# The 90 last values (in ascending order) to the index price 90-day moving 
# average.
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


###
### Structuring the spread trade
###

## Initializes the control vars to best trade pair.
best_deal = 0
best_trade = ""

## Gets the base index price.
index_price_params = {'index_name': "btc_usd"}
response = requests.get(f"{prod_base_url}{index_price_endpoint}", 
                                                      params=index_price_params)

if response.status_code == 200:
    data = response.json()
    base_index_price = float(data['result']['index_price'])

# Gets the active BTC day options offering.
active_instruments = instrument_collection.find({
                    'is_active': True, 
                    'option_type': "call", 
                    'strike': {"$gt": base_index_price},
                    'instrument_name': {"$regex": "^BTC-18JUL24-"},
                    }).sort("expiration_datetime", ASCENDING)

for instrument in active_instruments:

    inst_exp_time = datetime.datetime.fromtimestamp(
                            (instrument['expiration_unix_timestamp'] / 1000))


    inst_regex = (f"^{instrument['quote_currency']}-" +
                  f"{inst_exp_time.day}" +
                  f"{inst_exp_time.strftime('%b').upper()}" +
                  f"{inst_exp_time.strftime('%y')}-")

    ## Tests if the instrument was considered to trade.
    was_allocated = proposed_trades_collection.find_one(
                                {'instrument': {"$regex" : inst_regex}})

    if was_allocated is None:

        ### Defines the instruments for the spread trade.
        s_instrument = instrument   # Short instrument.

        l_inst_name = (inst_regex + 
                       str(int(s_instrument['strike'] + 500)) + "-C")

        # Long instrument.
        l_instrument = instrument_collection.find_one(
                                {'instrument_name': {"$regex" : l_inst_name}})
 

        # Continues only if a shor & long pair exists.          
        if l_instrument is not None:

            print(f"Looking: {s_instrument['instrument_name']} | {l_instrument['instrument_name']}")

            ###
            ### Calculating the short instrument fair price
            ###

            ## Gets the current index price.
            index_price_params = {'index_name': "btc_usd"}
            response = requests.get(f"{prod_base_url}{index_price_endpoint}", 
                                    params=index_price_params)

            if response.status_code == 200:
                data = response.json()
                cur_index_price = float(data['result']['index_price'])


            # Gets t (the time left to expiration).
            td_unix = int(time.mktime(td.timetuple()) * 1000)
            t = s_instrument['expiration_unix_timestamp'] - td_unix

            # Gets life-time ratio (in milliseconds):
            life_time_portion = (
                (td_unix - s_instrument['creation_unix_timestamp']) / 
                (s_instrument['expiration_unix_timestamp'] - 
                 s_instrument['creation_unix_timestamp']))

            # Gets x:
            x = cur_index_price / s_instrument['strike']

            # Gets the z model parameters.
            z_model = z_model_collection.find_one(
                                         {'model_name': "001-day-all-e1-e2"})

            # Calculates z.
            z = mp.mpf(z_model['k1'] + 
                       (z_model['k2'] * (1 / t)) + 
                       (z_model['k3'] * e1) + 
                       (z_model['k4'] * e2) + 
                       (z_model['k5'] * x) + 
                       (z_model['k6'] * s_instrument['strike']))

            # Calculates y = ((1 + (x**z))**(1/z)) - 1).
            try:
                term = mp.power(x, z)  # x**z
                term = mp.power(1 + term, 1 / z)  # (1 + (x**z))**(1 / z)
                y = term - 1
            except: 
                y = None

            # Gets the calculed fair price to the instrument.
            if y:
                Y = y * s_instrument['strike']

                # Note: For the Deribit's options offering, the lowest possible 
                # tradable value for Y is "0.0001".
                if Y <= 0.0001:
                    Ycalc = Y
                    Y = 0.0001
            else:
                Y = None


            ###
            ### Exploring the order book
            ###

            ### Gets the current order book:

            # Gets the current order book to short trade.
            s_order_book_params = {
                        'instrument_name': s_instrument['instrument_name'],
                        'depth': 1 # [1, 5, 10, 20, 50, 100, 1000, 10000]                      
                        }

            s_response = requests.get(f"{prod_base_url}{order_book_endpoint}", 
                                    params=s_order_book_params)

            if s_response.status_code == 200:
                s_current_order_book = s_response.json()

            # Gets the current open bids to short trade:
            s_bid = s_current_order_book['result']['bids'][0]


            # Gets the current order book to long trade.
            l_order_book_params = {
                        'instrument_name': l_instrument['instrument_name'],
                        'depth': 1 # [1, 5, 10, 20, 50, 100, 1000, 10000]                      
                        }

            l_response = requests.get(f"{prod_base_url}{order_book_endpoint}", 
                                    params=l_order_book_params)

            if l_response.status_code == 200:
                l_current_order_book = l_response.json()

            # Gets the current open asks to long trade:
            l_ask = l_current_order_book['result']['asks'][0]

            # Continues only if a shor & long bids/asks exists.
            if s_bid and l_ask:

                if Y:
                    # Gets the ratio between the observed price (for the 
                    # instrument) and the computed fair price.
                    YdivY = float(s_bid[0]) / Y
                else:
                    YdivY = None

                deal_yield = s_bid[0] - l_ask[0]

                # Filters spread trades.
                if ((YdivY is not None) and 
                    (YdivY >= 1.5) and 
                    (x <= 0.98)):
                    #(deal_yield > best_deal)):

                    #best_deal = deal_yield

                    best_trade = {
                        'datetime': td,
                        'unix_datetime': td_unix,
                        # Short trade data.
                        's_instrument': s_instrument['instrument_name'],
                        's_strike': s_instrument['strike'],
                        's_instrument_mark_price': s_current_order_book['result']['mark_price'],
                        's_bid': s_bid[0],
                        's_amount': s_bid[1],
                        # Short trade data.
                        'l_instrument': l_instrument['instrument_name'],
                        'l_strike': l_instrument['strike'],
                        'l_instrument_mark_price': l_current_order_book['result']['mark_price'],
                        'l_ask': l_ask[0],
                        'l_amount': l_ask[1],
                        # Computing vars
                        's_vars': {
                            'index_price': cur_index_price,
                            'z_model': z_model['model_name'],
                            't': (1 / t),
                            'e1': e1,
                            'e2': e2,
                            'x': x,
                            'life_time_portion': life_time_portion,
                            'iv': s_current_order_book['result']['mark_iv'],
                            'instrument_fair_price': str(Y),
                            'YdivY': str(YdivY),
                            'z': str(z),
                            #'Ycalc': str(Ycalc),
                            },
                        }

                    # Insert the document into the collection
                    proposed_trades_collection.insert_one(best_trade)

                    # Shows the job execution on terminal.
                    print("--- SPREAD TRADE ---")
                    print("Date: ", best_trade['datetime'], 
                          " | YdivY: ", best_trade['s_vars']['YdivY'],
                          " | x: ", best_trade['s_vars']['x'],
                          " | Deal: ", best_deal)

                    print("--- SHORT ---")
                    print("Inst: ", best_trade['s_instrument'],
                          " | Bid: ", best_trade['s_bid'])

                    print("--- LONG ---")
                    print("Inst: ", best_trade['l_instrument'],
                          " | Ask: ", best_trade['l_ask'])

### Stops the runtime control.
end_time = time.time()
execution_time = end_time - start_time

print("The order book searching has been completed! " + 
      f"- Exec Time: {execution_time}")

















