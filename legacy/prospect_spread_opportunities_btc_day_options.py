####
#### Prospects bear/bull spreads opportunities with BTC calls on Deribit.
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
opp_spread_collection = db['btc_day_options_opportunities_spread']


### Gets the z model parameters.
# "001-day-all-e1-e2"
# "002-day-call-e1-e2"
z_model = z_model_collection.find_one({'model_name': "001-day-all-e1-e2"})


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
have_documents = opp_spread_collection.find_one(sort=[('datetime', -1)])
if have_documents is None:
    opp_spread_collection.create_index([('datetime', DESCENDING)])
    opp_spread_collection.create_index([('datetime', ASCENDING)])


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
### Preparing the bi-directional spread trades
###

# Gets the active BTC day options offering.
active_instruments = instrument_collection.find({
                'is_active': True, 
                'option_type': "call",
                #'strike': {"$gte": base_index_price}, 
                'expiration_unix_timestamp': {"$gte": (86.4e6 + td_00h_unix)},
                }).sort([('expiration_unix_timestamp', 1), ('strike', 1)])


for instrument in active_instruments:

    ###
    ### Getting the pair candidate instruments
    ###

    ## The main instrument.
    m_instrument = instrument

    ## The subsequent instrument (strike_main < strike_subsequent).
    s_instrument = instrument_collection.find_one({
        'is_active': True,
        'expiration_unix_timestamp': instrument['expiration_unix_timestamp'],
        'strike': {'$gt': m_instrument['strike']},
        }, sort=[('strike', 1)])   

    # Shows the instrument pair.
    if s_instrument is not None:
        print("Looking: ",
              m_instrument['instrument_name'],
              " | ",
              s_instrument['instrument_name'])
        print("==============================================================")
    else:
        print("Looking: ",
              m_instrument['instrument_name'],
              " | ",
              s_instrument)
        print("==============================================================")

    ###
    ### Calculating the main instrument fair price
    ###

    # Gets t (the time left to expiration).
    m_t = m_instrument['expiration_unix_timestamp'] - td_unix

    # Gets life-time ratio (in milliseconds):
    m_lifetime_ratio = (
        (td_unix - m_instrument['creation_unix_timestamp']) / 
        (m_instrument['expiration_unix_timestamp'] - 
         m_instrument['creation_unix_timestamp']))

    # Gets x:
    # Note: For bear spread, m_x < 1.0 is desirable (0.95 or less).
    m_x = base_index_price / m_instrument['strike']

    # Calculates z.
    m_z = mp.mpf(z_model['k1'] + 
               (z_model['k2'] * (1 / m_t)) + 
               (z_model['k3'] * e1) + 
               (z_model['k4'] * e2) + 
               (z_model['k5'] * m_x) + 
               (z_model['k6'] * m_instrument['strike']))

    # Calculates y = ((1 + (x**z))**(1/z)) - 1).
    try:
        m_term = mp.power(m_x, m_z)  # x**z
        m_term = mp.power(1 + m_term, 1 / m_z)  # (1 + (x**z))**(1 / z)
        m_y = m_term - 1
    except: 
        m_y = None

    # Gets the calculed fair price to the instrument.
    if m_y:
        m_Ycalc = m_y * m_instrument['strike']
        m_Y = math.floor(m_Ycalc * 1e4 ) / 1e4 # Rounds Ycalc to 4 decimals.

        # Note: For the Deribit's options offering, the lowest possible 
        # tradable value for Y is "0.0001".
        if m_Y < 0.0001:
            m_Y = 0.0001
    else:
        m_Y = None
        m_Ycalc = None


    ###
    ### Calculating the subsequent instrument fair price
    ###

    if s_instrument is not None:

        # Gets t (the time left to expiration).
        s_t = s_instrument['expiration_unix_timestamp'] - td_unix

        # Gets life-time ratio (in milliseconds):
        s_lifetime_ratio = (
            (td_unix - s_instrument['creation_unix_timestamp']) / 
            (s_instrument['expiration_unix_timestamp'] - 
             s_instrument['creation_unix_timestamp']))

        # Gets x:
        # Note: For bull spread, s_x > 1.0 is desirable (1.05 or more).
        s_x = base_index_price / s_instrument['strike']

        # Calculates z.
        s_z = mp.mpf(z_model['k1'] + 
                   (z_model['k2'] * (1 / s_t)) + 
                   (z_model['k3'] * e1) + 
                   (z_model['k4'] * e2) + 
                   (z_model['k5'] * s_x) + 
                   (z_model['k6'] * s_instrument['strike']))

        # Calculates y = ((1 + (x**z))**(1/z)) - 1).
        try:
            s_term = mp.power(s_x, s_z)  # x**z
            s_term = mp.power(1 + s_term, 1 / s_z)  # (1 + (x**z))**(1 / z)
            s_y = s_term - 1
        except: 
            s_y = None

        # Gets the calculed fair price to the instrument.
        if s_y:
            s_Ycalc = s_y * s_instrument['strike']
            s_Y = math.floor(s_Ycalc * 1e4 ) / 1e4 # Rounds Ycalc to 4 decimals.

            # Note: For the Deribit's options offering, the lowest possible 
            # tradable value for Y is "0.0001".
            if s_Y < 0.0001:
                s_Y = 0.0001
        else:
            s_Y = None
            s_Ycalc = None

    else:
        s_t = None
        s_lifetime_ratio = None
        s_x = None
        s_z = None
        s_Y = None
        s_Ycalc = None


    ###
    ### Searching for bids/asks in the order book
    ###

    ## Gets the current order book to main instrument.
    m_order_book_params = {
                'instrument_name': m_instrument['instrument_name'],
                'depth': 1 # [1, 5, 10, 20, 50, 100, 1000, 10000]                      
                }

    m_response = requests.get(f"{prod_base_url}{order_book_endpoint}", 
                            params=m_order_book_params)

    if m_response.status_code == 200:
        m_current_order_book = m_response.json()

    ## Gets the current opened bid and ask data to main instrument:
    try:
        m_markp = float(m_current_order_book['result']['mark_price'])
    except:
        m_markp = None
    try:
        m_bid = m_current_order_book['result']['bids'][0]
    except:
        m_bid = None
    try:
        m_ask = m_current_order_book['result']['asks'][0]
    except:
        m_ask = None


    ## Gets the current opened bid and ask data to subsequent instrument:
    if s_instrument is not None:
        s_order_book_params = {
                    'instrument_name': s_instrument['instrument_name'],
                    'depth': 1 # [1, 5, 10, 20, 50, 100, 1000, 10000]                      
                    }

        s_response = requests.get(f"{prod_base_url}{order_book_endpoint}", 
                                params=s_order_book_params)

        if s_response.status_code == 200:
            s_current_order_book = s_response.json()

        try:
            s_markp = float(s_current_order_book['result']['mark_price'])
        except:
            s_markp = None
        try:
            s_bid = s_current_order_book['result']['bids'][0]
        except:
            s_bid = None
        try:
            s_ask = s_current_order_book['result']['asks'][0]
        except:
            s_ask = None
    else:
        s_markp = None
        s_bid = None
        s_ask = None


    ###
    ### Evaluating the bear spread opportunities
    ###

    # Continues only if the short trade will start OTM:
    if m_bid and s_ask and (m_x < 1.0):

        ## Calculates the YdivY.
        # Note:
        # For short trades, YdivY < 1.0 is desirable (instrument is expensive);
        # And, for long trades, YdivY > 1.0 is desirable (instrument is cheap).
        if m_Y:
            # Gets the ratio between the observed price (for the instrument) and
            # the computed fair price.
            bear_m_YdivY = math.floor((float(m_bid[0]) / m_Y) * 1e8 ) / 1e8
        else:
            bear_m_YdivY = None

        if s_Y:
            # Gets the ratio between the observed price (for the instrument) and
            # the computed fair price.
            bear_s_YdivY = math.floor((float(s_ask[0]) / s_Y) * 1e8 ) / 1e8
        else:
            bear_s_YdivY = None

 
        ###
        ### Computing the P&l 
        ###

        ## Calculates the transaction fees.
        bear_m_fee = math.ceil(( min((m_bid[0] * 0.125), 0.0003)) * 1e8 ) / 1e8
        bear_s_fee = math.ceil((min((s_ask[0] * 0.125), 0.0003)) * 1e8 ) / 1e8
       
        ## Calculates the transaction yields.
        # Short.
        bear_m_yield = math.floor((m_bid[0] - bear_m_fee) * 1e8 ) / 1e8
        # Long.
        bear_s_yield = math.ceil((s_ask[0] + bear_s_fee) * 1e8 ) / 1e8
        bear_spread_yield = (math.floor((bear_m_yield - bear_s_yield) * 1e8 ) / 
                             1e8)

        ## Calculates the maximum loss with the spread trade.
        # Note: that the value is calculated in USD and converted to the 
        # delivery price in BTC. Therefore the real value in BTC will be 
        # slightly lower because of the BTC appreciation.
        bear_loss_del = ((m_instrument['strike'] - s_instrument['strike']) /
                         base_index_price)
        bear_max_loss = (math.ceil((bear_loss_del + bear_spread_yield) * 1e8) / 
                        1e8)

        ## Computes the margin requirements 
        if (m_instrument['strike'] > base_index_price):
            bear_initial_margin = (max((0.15 - ((m_instrument['strike'] - 
                         base_index_price) / base_index_price)), 0.1) + m_markp)
        else:
            bear_initial_margin = 0.15 + m_markp

        if bear_spread_yield > 0:
            print("BEAR SPREAD")
            print("--------------------------------------------------------------")
            print(
                 "  Short YdivY (main):\t", "{:.8f}".format(bear_m_YdivY), "\n",
                 " Long YdivY (sub):\t", "{:.8f}".format(bear_s_YdivY), "\n",
                 " x main (< 1):\t\t", "{:.8f}".format(m_x), "\n\n",  
                 " Short bid (main):\t", "{:.8f}".format(m_bid[0]), "\n",
                 " Long ask (sub):\t", "{:.8f}".format(s_ask[0]), "\n\n",
                 " Short premium (main):\t", "{:.8f}".format(bear_m_yield), "\n",
                 " Long cost (sub):\t", "{:.8f}".format(bear_s_yield), "\n",
                 " Spread yield:\t\t", "{:.8f}".format(bear_spread_yield), "\n\n",
                 " Maximum loss:\t\t", "{:.8f}".format(bear_max_loss), "\n",
                 " Initial margin (m):\t", "{:.8f}".format(bear_initial_margin), "\n",
                 )
            print("--------------------------------------------------------------")


    ###
    ### Evaluating the bull spread opportunities
    ###

    # Continues only if the long trade will start ITM:
    if m_ask and s_bid and s_x and (s_x > 1.0):

        ## Calculates the YdivY.
        # Note:
        # For short trades, YdivY < 1.0 is desirable (instrument is expensive);
        # And, for long trades, YdivY > 1.0 is desirable (instrument is cheap).
        if m_Y:
            # Gets the ratio between the observed price (for the instrument) and
            # the computed fair price.
            bull_m_YdivY = math.floor((float(m_ask[0]) / m_Y) * 1e8 ) / 1e8
        else:
            bull_m_YdivY = None

        if s_Y:
            # Gets the ratio between the observed price (for the instrument) and
            # the computed fair price.
            bull_s_YdivY = math.floor((float(s_bid[0]) / s_Y) * 1e8 ) / 1e8
        else:
            bull_s_YdivY = None

 
        ###
        ### Computing the P&l 
        ###

        ## Calculates the transaction fees.
        bull_m_fee = math.ceil(( min((m_ask[0] * 0.125), 0.0003)) * 1e8 ) / 1e8
        bull_s_fee = math.ceil((min((s_bid[0] * 0.125), 0.0003)) * 1e8 ) / 1e8
       
        ## Calculates the transaction yields.
        # Long.
        bull_m_yield = math.floor((m_ask[0] + bull_m_fee) * 1e8 ) / 1e8
        # Short.
        bull_s_yield = math.ceil((s_bid[0] - bull_s_fee) * 1e8 ) / 1e8
        bull_spread_yield = (math.floor((bull_s_yield - bull_m_yield) * 1e8 ) / 
                             1e8)

        ## Calculates the maximum loss with the spread trade.
        # Note: that the value is calculated in USD and converted to the 
        # delivery price in BTC. Therefore the real value in BTC will be 
        # slightly lower because of the BTC appreciation.
        bull_profit_del = ((s_instrument['strike'] - m_instrument['strike']) /
                         base_index_price)
        bull_max_profit = (math.ceil((bull_profit_del + bull_spread_yield) * 1e8) / 
                        1e8)

        ## Computes the margin requirements 
        if (s_instrument['strike'] > base_index_price):
            bull_initial_margin = (max((0.15 - ((s_instrument['strike'] - 
                         base_index_price) / base_index_price)), 0.1) + s_markp)
        else:
            bull_initial_margin = 0.15 + s_markp

        if bull_max_profit > 0:
            print("BULL SPREAD")
            print("--------------------------------------------------------------")
            print(
                 "  Long YdivY (main):\t", "{:.8f}".format(bull_m_YdivY), "\n",
                 " Short YdivY (sub):\t", "{:.8f}".format(bull_s_YdivY), "\n",
                 " x sub (> 1):\t\t", "{:.8f}".format(s_x), "\n\n",  
                 " Long ask (main):\t", "{:.8f}".format(m_ask[0]), "\n",
                 " Short bid (sub):\t", "{:.8f}".format(s_bid[0]), "\n\n",
                 " Long cost (main):\t", "{:.8f}".format(bull_m_yield), "\n",
                 " Short premium (sub):\t", "{:.8f}".format(bull_s_yield), "\n",
                 " Spread yield (cost):\t", "{:.8f}".format(bull_spread_yield), "\n\n",
                 " Maximum profit:\t", "{:.8f}".format(bull_max_profit), "\n",
                 " Initial margin (s):\t", "{:.8f}".format(bull_initial_margin), "\n",
                 )
            print("--------------------------------------------------------------")


    elif m_bid and m_x and (m_x < 1):

        ## Calculates the YdivY.
        # Note:
        # For short trades, YdivY < 1.0 is desirable (instrument is expensive);
        # And, for long trades, YdivY > 1.0 is desirable (instrument is cheap).
        if m_Y:
            # Gets the ratio between the observed price (for the instrument) and
            # the computed fair price.
            bear_m_YdivY = math.floor((float(m_bid[0]) / m_Y) * 1e8 ) / 1e8
        else:
            bear_m_YdivY = None

        ###
        ### Computing the P&l 
        ###

        ## Calculates the transaction fees.
        bear_m_fee = math.ceil(( min((m_bid[0] * 0.125), 0.0003)) * 1e8 ) / 1e8
       
        ## Calculates the transaction yields.
        # Short.
        bear_m_yield = math.floor((m_bid[0] - bear_m_fee) * 1e8 ) / 1e8


        ## Computes the margin requirements 
        if (m_instrument['strike'] > base_index_price):
            bear_initial_margin = (max((0.15 - ((m_instrument['strike'] - 
                         base_index_price) / base_index_price)), 0.1) + m_markp)
        else:
            bear_initial_margin = 0.15 + m_markp

        if (bear_m_YdivY > 1.3):

            print("SIMPLE SHORT")
            print("--------------------------------------------------------------")
            print(
                 "  Short YdivY (main):\t", "{:.8f}".format(bear_m_YdivY), "\n",
                 " x main (< 1):\t\t", "{:.8f}".format(m_x), "\n\n",  
                 " Short bid (main):\t", "{:.8f}".format(m_bid[0]), "\n",
                 " Short premium (main):\t", "{:.8f}".format(bear_m_yield), "\n",
                 " Initial margin (s):\t", "{:.8f}".format(bear_initial_margin), "\n",
                 )
            print("--------------------------------------------------------------")








        #     opp = {
        #         'datetime': td,
        #         'unix_datetime': td_unix,
        #         # Short trade data.
        #         's_instrument': s_instrument['instrument_name'],
        #         's_strike': s_instrument['strike'],
        #         's_instrument_mark_price': s_current_order_book['result']['mark_price'],
        #         's_bid': s_bid[0],
        #         's_amount': s_bid[1],
        #         # Short trade data.
        #         'l_instrument': l_instrument['instrument_name'],
        #         'l_strike': l_instrument['strike'],
        #         'l_instrument_mark_price': l_current_order_book['result']['mark_price'],
        #         'l_ask': l_ask[0],
        #         'l_amount': l_ask[1],
        #         # Computing vars
        #         's_vars': {
        #             'index_price': base_index_price,
        #             'z_model': z_model['model_name'],
        #             't': (1 / t),
        #             'e1': e1,
        #             'e2': e2,
        #             'x': x,
        #             'life_time_portion': life_time_portion,
        #             'iv': s_current_order_book['result']['mark_iv'],
        #             'instrument_fair_price': str(Y),
        #             'YdivY': str(YdivY),
        #             'z': str(z),
        #             #'Ycalc': str(Ycalc),
        #             },
        #         }

        #     # Insert the document into the collection
        #     opp_spread_collection.insert_one(opp)


### Stops the runtime control.
end_time = time.time()
execution_time = end_time - start_time

print(">>>>>>>>>>>>>>>>>>")
print("The order book searching has been completed! " + 
      f"- Exec Time: {execution_time}")

















