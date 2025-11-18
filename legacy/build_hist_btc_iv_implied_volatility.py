###
### Builds the BTC IV implied volatility database.
###

## Note: the IV index is the standard deviation the underlying asset's returns.

import datetime
import time
import requests
import json
from pymongo import MongoClient, DESCENDING


# API definitions.
hist_base_url = "https://history.deribit.com/api/v2/public/"
endpoint = f"get_last_trades_by_currency_and_time"

# DB access.
client = MongoClient('mongodb://localhost:27017/')
db = client['deribit_btc_options']
collection = db['btc_iv_implied_volatility']

# Finds the document with the most recent date value.
last_document = collection.find_one(sort=[('datetime', -1)])

# Iteration interval - Initial value.
if last_document is None:
    # To (re)build the entire database.
    collection.create_index([('datetime', DESCENDING)])
    i_date = datetime.datetime(2017, 1, 1, 0, 0, 0)
else:
    # To update the database.
    i_date = last_document['datetime'] + datetime.timedelta(hours=1)

# Iteration interval - Final value.
td = datetime.datetime.now()
td -= datetime.timedelta(hours=1)
f_date = datetime.datetime(td.year, td.month, td.day, td.hour, 0, 0)

# Iterates historical daily orders to extract the IV value.
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
            iv = float(data['result']['trades'][0]['iv'])
        except:
            iv = None
    
    document = {
                'datetime': i_date,
                'unix_time': i_ts_unix,
                'iv': iv
                }

    # Insert the document into the collection
    collection.insert_one(document)

    # Shows the job execution on terminal.
    print(i_date, iv)

    # Sets a new hourly cycle.
    i_date += datetime.timedelta(hours=1)

print("The database is up to date!")
