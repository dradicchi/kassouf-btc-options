###
### Builds the BTC trade history database.
###

## NOTES:
## The 'datetime' fields from 'btc_trade_history_5min' collection are 
## relative to 'São Paulo -3 GMT local time'. As the Deribit's BTC options 
## (daily, weekly and monthly) contracts expires at 8:00 AM GMT (5:00 AM at 
## São Paulo -3 GMT local time). Unix time are in miliseconds.

import datetime
import time
import requests
import json
from pymongo import MongoClient, DESCENDING, ASCENDING


# API definitions.
hist_base_url = "https://history.deribit.com/api/v2/public/"
endpoint_a = f"get_last_trades_by_currency_and_time"
# endpoint_b = f"get_last_trades_by_instrument" # An alternative source.

# DB access.
client = MongoClient('mongodb://localhost:27017/')
db = client['deribit_btc_options']
collection = db['btc_trade_history_5min']

# Finds the document with the most recent date value.
last_document = collection.find_one(sort=[('date_time', -1)])

# Iteration interval - Initial value.
if last_document is None:
    # To (re)build the entire database.
    collection.create_index([('date_time', ASCENDING)])
    collection.create_index([('date_time', DESCENDING)])
    i_date = datetime.datetime(2017, 1, 1, 0, 0, 0)
else:
    # To update the database.
    i_date = (last_document['dt_control'] + datetime.timedelta(minutes=5))

# Iteration interval - Final value.
td = datetime.datetime.now()
td -= datetime.timedelta(hours=1)
f_date = datetime.datetime(td.year, td.month, td.day, td.hour, 55, 0)

# Iterates historical daily orders to extract the hourly index price.
while i_date <= f_date:

    # Query interval (in miliseconds).
    i_ts_unix = int(time.mktime(i_date.timetuple()) * 1000)
    f_ts_unix = i_ts_unix + 299999 #  + 4min59.999sec

    params_a = {
                'currency': "BTC",               
                'kind': "option",               
                'start_timestamp': i_ts_unix,      
                'end_timestamp': f_ts_unix,        
                'count': 10000,                         
                }

    # Parameters to an alternative endpoint.
    # params_b = {
    #             'instrument_name': "BTC-PERPETUAL", 
    #             #'start_timestamp': i_ts_unix,      
    #             'end_timestamp': f_ts_unix,          
    #             'count': 1,                           
    #             }

    response = requests.get(f"{hist_base_url}{endpoint_a}", params=params_a)
    
    if response.status_code == 200:

        data = response.json()

        for t_num in range(10000):

            # Handles errors if there aren't trades within the time range.
            try:
                trade = data['result']['trades'][t_num]

                trade_dt = datetime.datetime.fromtimestamp(
                                                    trade['timestamp'] / 1000)
                try:
                    liquidation = trade['direction']
                except:
                    liquidation = None

                try:
                    block_trade_id = trade['block_trade_id']
                except:
                    block_trade_id = None

                document = {
                            'id': trade['trade_id'],
                            'trade_seq': trade['trade_seq'],
                            'dt_control': i_date,
                            'date_time': trade_dt,
                            'unix_time': trade['timestamp'],
                            'instrument_name': trade['instrument_name'],
                            'price': trade['price'],
                            'mark_price': trade['mark_price'],
                            'amount': trade['amount'],
                            'direction': trade['direction'],
                            'tick_direction': trade['tick_direction'],
                            'liquidation': liquidation,
                            'block_trade_id': block_trade_id,
                            'index_price': trade['index_price'],
                            'iv': trade['iv'],
                            }

                # Insert the trade into the collection
                #print(trade['trade_id'])
                collection.insert_one(document)

            except:
                break
    
    # Shows the job execution on terminal.
    print(i_date)

    # Sets a new hourly cycle.
    i_date += datetime.timedelta(minutes=5)

print("The database is up to date!")
