###
### Builds the BTC day options historical trades database.
###

## IMPORTANT: 
## Is recommended to update the BTC options instruments offer database, by 
## running the "build_hist_btc_options_instruments_offer.py" script before to 
## execute this script. This script only considers expired instrument offers.

import datetime
import time
import requests
import json
from pymongo import MongoClient, DESCENDING, ASCENDING
import sys


# API definitions.
hist_base_url = "https://history.deribit.com/api/v2/public/"
endpoint = f"get_last_trades_by_instrument_and_time"

# DB access.
client = MongoClient('mongodb://localhost:27017/')
db = client['deribit_btc_options']
instrument_collection = db['btc_day_inverse_options_offer']
trade_collection = db['btc_day_options_trades']

# If the options trades database is empty, set it up.
if trade_collection.find_one() is None:
        trade_collection.create_index([('datetime', DESCENDING)])
        trade_collection.create_index([('datetime', ASCENDING)])

## Gets instrument names, ordered by expiration_datetime in descending order.
cursor = instrument_collection.find({'is_active': False},
                                    {'instrument_name': 1,
                                     'creation_unix_timestamp': 1,
                                     'expiration_unix_timestamp': 1,
                                     '_id': 0}
                                   ).sort("expiration_datetime", ASCENDING)

instrument_list = [{'instrument_name': doc['instrument_name'],
                    'creation_timestamp': doc['creation_unix_timestamp'],
                    'expiration_timestamp': doc['expiration_unix_timestamp']
                   } 
                   for doc in cursor]

# Iterates until find an already processed instrument.
for instrument in instrument_list:

    # To test if the instrument already was processed.
    is_proc_doc = trade_collection.find_one(
                        {'instrument_name': instrument['instrument_name']})

    if is_proc_doc is None:

        # Search interval (Unix time timestamp in milliseconds).
        i_timestamp = instrument['creation_timestamp']    # Initial.
        f_timestamp = instrument['expiration_timestamp']  # Final.
        
        # walking step (in milliseconds).
        step_timestamp = 3600000    # one hour.

?????????
        









# Iteration interval - Final value.
td = datetime.datetime.now()
td -= datetime.timedelta(hours=1)
f_date = datetime.datetime(td.year, td.month, td.day, td.hour, 0, 0)

# Iterates historical daily orders to extract the hourly index price.
while i_date <= f_date:

    # Query interval (in milliseconds).
    i_ts_unix = int(time.mktime(i_date.timetuple()) * 1000)
    f_ts_unix = i_ts_unix + 3599000 

    params = {
                'currency': "BTC",               
                'kind': "option",               
                #'start_timestamp': i_ts_unix,      # Optional.
                'end_timestamp': f_ts_unix,        
                'count': 1,                         
                }

    response = requests.get(f"{hist_base_url}{endpoint}", params=params)
    
    if response.status_code == 200:

        data = response.json()

        # Handles errors if there aren't trades within the time range.
        try:
            index_price = data['result']['trades'][0]['index_price']
        except:
            index_price = None
    
    document = {
                'datetime': i_date,
                'unix_time': i_ts_unix,
                'index_price': float(index_price)
                }

    # Insert the document into the collection
    collection.insert_one(document)

    # Shows the job execution on terminal.
    print(i_date, index_price)

    # Sets a new hourly cycle.
    i_date += datetime.timedelta(hours=1)

print("The database is up to date!")
