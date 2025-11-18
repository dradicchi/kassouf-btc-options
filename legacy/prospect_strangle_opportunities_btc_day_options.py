####
#### Prospects short strangle opportunities with BTC options on Deribit.
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
from os import system, name


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
opp_strangle_collection = db['btc_day_options_opportunities_strangle']


### Gets today date.
td = datetime.datetime.now()
td_00h = datetime.datetime(td.year, td.month, td.day, 0, 0, 0)
td_unix = int(time.mktime(td.timetuple()) * 1000) # In miliseconds
td_00h_unix = int(td_00h.timestamp() * 1000) # In miliseconds


### Gets the base index price.
index_price_params = {'index_name': "btc_usd"}
response = requests.get(f"{prod_base_url}{index_price_endpoint}", 
                        params=index_price_params)

if response.status_code == 200:
    data = response.json()
    base_index_price = float(data['result']['index_price'])


### Inits the proposed trades database.
# have_documents = opp_strangle_collection.find_one(sort=[('datetime', -1)])
# if have_documents is None:
#     opp_strangle_collection.create_index([('datetime', DESCENDING)])
#     opp_strangle_collection.create_index([('datetime', ASCENDING)])


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
    }}]

result = list(index_price_collection.aggregate(pipeline))

# Handles with absence of data to get today's partial average BTC index price.
try:
    td_sum_price = float(result[0]['sum_price'])
    td_count = int(result[0]['count'])
    td_partial_average = td_sum_price / td_count
    limit = 179
except:
    td_partial_average = None
    limit = 180

# Gets the 179 last values to the BTC index price daily average database.
avg_prices_list = index_price_d_avg_collection.find({}, 
                                        {'_id': 0, 'avg_index_price_daily': 1}
                                        ).sort('datetime', -1).limit(limit)
prices_list = [doc['avg_index_price_daily'] for doc in avg_prices_list]
prices_ascending = prices_list[::-1]

# A list with the 180 most recent index price daily average.
if td_partial_average is not None:
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
### Combining the CALL/PUT pair instruments
###

# Gets the next expiration dates.
expiration_time_list = [int((i * 86.4e6) + (td_00h_unix + 18.0e6)) 
                                                           for i in range(1, 4)]

for exp_time in expiration_time_list:

    # Initializes a empty list of opportunities.
    strangle_opp_list = []

    # Gets the active BTC day CALLs offering.
    c_active_instruments = instrument_collection.find({
                    'is_active': True, 
                    'option_type': "call",
                    'strike': {"$gte": (base_index_price * 1.07)}, 
                    'expiration_unix_timestamp': exp_time,
                    }).sort([('expiration_unix_timestamp', 1), ('strike', -1)])

    for c_instrument in c_active_instruments:

        # Gets the active BTC day PUTs offering.
        p_active_instruments = instrument_collection.find({
                'is_active': True,
                'option_type': "put",
                'expiration_unix_timestamp': exp_time,
                'strike': {"$lte": (base_index_price * 0.93)},
                }).sort([('strike', 1)])   

        for p_instrument in p_active_instruments:

            print("Looking: ",
                  c_instrument['instrument_name'], " | ",
                  p_instrument['instrument_name'])


            ###
            ### Searching for CALL/PUT bids in the order book
            ###

            ## Gets the current order book to CALL instrument.
            c_order_book_params = {
                        'instrument_name': c_instrument['instrument_name'],
                        'depth': 1 # [1, 5, 10, 20, 50, 100, 1000, 10000]                      
                        }

            c_response = requests.get(f"{prod_base_url}{order_book_endpoint}", 
                                    params=c_order_book_params)

            if c_response.status_code == 200:
                c_current_order_book = c_response.json()

            ## Gets the current opened bid to CALL instrument:
            try:
                c_markp = float(c_current_order_book['result']['mark_price'])
            except:
                c_markp = None
            try:
                c_bid = c_current_order_book['result']['bids'][0]
            except:
                c_bid = None


            ## Gets the current opened bid to PUT instrument:
            p_order_book_params = {
                        'instrument_name': p_instrument['instrument_name'],
                        'depth': 1 # [1, 5, 10, 20, 50, 100, 1000, 10000]                      
                        }

            p_response = requests.get(f"{prod_base_url}{order_book_endpoint}", 
                                    params=p_order_book_params)

            if p_response.status_code == 200:
                p_current_order_book = p_response.json()

            try:
                p_markp = float(p_current_order_book['result']['mark_price'])
            except:
                p_markp = None
            try:
                p_bid = p_current_order_book['result']['bids'][0]
            except:
                p_bid = None


            ###
            ### Evaluating the short strangle opportunities
            ###

            # Continues only if there are a valid pair.
            if c_bid and p_bid and c_markp and p_markp:

                # Gets the strike amplitude.
                amplitude = c_instrument['strike'] - p_instrument['strike']
         
                ## Calculates the transaction fees.
                c_fee = math.ceil((min((c_bid[0] * 0.125), 0.0003)) * 1e8 ) / 1e8
                p_fee = math.ceil((min((p_bid[0] * 0.125), 0.0003)) * 1e8 ) / 1e8
               
                ## Calculates the transaction yields.
                # Shorting CALL.
                c_yield = math.floor((c_bid[0] - c_fee) * 1e8 ) / 1e8
                # Shorting PUT.
                p_yield = math.floor((p_bid[0] - p_fee) * 1e8 ) / 1e8
                # Total premium.
                max_yield = (math.floor((c_yield + p_yield) * 1e8 ) / 1e8)

                ## Calculates the breakeven limits.
                # Superior limit.
                break_sup_lim = ((max_yield * base_index_price) + 
                                 c_instrument['strike'])
                # Inferior limit.
                break_inf_lim =  (p_instrument['strike'] - 
                                  (max_yield * base_index_price))

                ## Computes the margin requirements.
                # Shorting CALL.
                if (c_instrument['strike'] > base_index_price):
                    c_initial_margin = (max((0.15 - ((c_instrument['strike'] - 
                                 base_index_price) / base_index_price)), 0.1) + 
                                 c_markp)
                else:
                    c_initial_margin = 0.15 + c_markp
                # Shorting PUT.
                mm = (max(0.075, (0.075 * p_markp)) + p_markp) # Maintenance margin
                if (p_instrument['strike'] < base_index_price):
                    p_initial_margin = max(
                        (max((0.15 - ((base_index_price - p_instrument['strike']) / 
                                      base_index_price)), 0.1) + p_markp), mm)
                else:
                    p_initial_margin = max((0.15 + p_markp), mm)
                # Total initial margin.
                total_margin = c_initial_margin + p_initial_margin

                # Builds the strangle opportunity.
                opp = {
                     'c_instrument': c_instrument['instrument_name'],
                     'p_instrument': p_instrument['instrument_name'],
                     'c_yield': c_yield,
                     'p_yield': p_yield,
                     'c_strike_div_index': (c_instrument['strike'] / 
                                            base_index_price),
                     'p_index_div_strike': (base_index_price / 
                                            p_instrument['strike']),
                     'max_yield': max_yield,
                     'initial_margin': total_margin,
                     'roi': ((max_yield * 100) / total_margin),
                     'amplitude': amplitude,
                     'inf_break': break_inf_lim,
                     'sup_break': break_sup_lim,
                     'index': base_index_price,
                     'e1': e1,
                     'e2': e2,
                     }

                strangle_opp_list.append(opp)
                # Insert the document into the collection
                # opp_strangle_collection.insert_one(opp)


    ord_strangle_opp_list = sorted(strangle_opp_list,
                                   key=lambda x: x['roi'],
                                   reverse=True)

    _ = system('clear') # clear the terminal screen.
    print(
         "=========================================================", "\n",
         "EXPIRATION TIME: ", datetime.datetime.fromtimestamp(exp_time / 1000), "\n",
         "=========================================================", "\n",
         )

    for opp in ord_strangle_opp_list:

        print(
             "---------------------------------------------------------", "\n\n",

             ">>", opp['c_instrument'], " | ", opp['p_instrument'], "\n\n",

             " Index (USD):\t\t", "{:.2f}".format(opp['index']), "\n",
             " Volatility (E2):\t", "{:.4f}".format(opp['e1']), "\n",
             " Trend (E1):\t\t", "{:.4f}".format(opp['e2']), "\n\n",

             " CALL", "\n",
             "-------------------------------------------------", "\n",
             " Shorting call yield:\t", "{:.8f}".format(opp['c_yield']), "\n",
             " strike/index:\t\t", "{:.4f}".format(opp['c_strike_div_index']), "\n\n",

             " PUT", "\n",
             "-------------------------------------------------", "\n",             
             " Shorting put yield:\t", "{:.8f}".format(opp['p_yield']), "\n",
             " index/strike:\t\t", "{:.4f}".format(opp['p_index_div_strike']), "\n\n",

             " P&L", "\n",
             "-------------------------------------------------", "\n",  
             " Strangle yield (max):\t", "{:.8f}".format(opp['max_yield']), "\n",
             " Initial margin:\t", "{:.8f}".format(opp['initial_margin']), "\n",
             " ROI (%):\t\t", "{:.2f}".format(opp['roi']), "\n\n",

             " Risk analysis", "\n",
             "-------------------------------------------------", "\n", 
             " Amplitude (USD):\t", "{:.2f}".format(opp['amplitude']), "\n",
             " Inf break lim (USD):\t", "{:.2f}".format(opp['inf_break']), "\n",
             " Sup break lim (USD):\t", "{:.2f}".format(opp['sup_break']), "\n\n",

             )
        print("---------------------------------------------------------", "\n")

        input("Press Enter to continue...")
        

    input("Press Enter to continue...")

### Stops the runtime control.
end_time = time.time()
execution_time = end_time - start_time

print(">>>>>")
print("The order book searching has been completed! " + 
      f"- Exec Time: {execution_time}")

















