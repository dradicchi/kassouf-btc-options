###
### Builds the BTC delivery price daily database.
###

## NOTES:
## The 'datetime' fields from 'btc_delivery_price_daily' collection are 
## relative to 'São Paulo -3 GMT local time'. As the Deribit's BTC options 
## (daily, weekly and monthly) contracts expires at 8:00 AM GMT (5:00 AM at 
## São Paulo -3 GMT local time). Unix time are in miliseconds.


from datetime import datetime, timedelta
import time
import requests
import json
from pymongo import MongoClient, DESCENDING, ASCENDING
import sys


# API definitions.
prod_base_url = "https://deribit.com/api/v2/public/"
endpoint = f"get_delivery_prices"

# DB access.
client = MongoClient('mongodb://localhost:27017/')
db = client['deribit_btc_options']
collection = db['btc_delivery_price_daily']

# Finds the document with the most recent date value.
last_document = collection.find_one(sort=[('datetime', -1)])
first_document = collection.find_one(sort=[('datetime', 1)])

# Sets the oldest control date.
# Note: This script gets delivery prices from newest to oldest.
if last_document is None:
    # To (re)build the entire database.
    collection.create_index([('datetime', DESCENDING)])
    collection.create_index([('datetime', ASCENDING)])
    i_date = datetime(2017, 1, 6, 0, 0, 0)
elif first_document['datetime'] != datetime(2017, 1, 6, 0, 0, 0):
    # To complete the database.
    i_date = datetime(2017, 1, 6, 0, 0, 0)
else:
    # To update the database.
    i_date = last_document['datetime'] + timedelta(days=1)

# Initial pagination to API querying.
offset = 0

# Query parameters.
params = {
          'count': 1,
          'index_name': "btc_usd",
          'offset': offset,                      
         }

# The first API query to serve the "while" clause.
response = requests.get(f"{prod_base_url}{endpoint}", params=params)
if response.status_code == 200:
    data = response.json()
    delivery_price_list = data['result']['data']

# Iterates while there are non-empty results.
while delivery_price_list:

    for item in delivery_price_list:

        # Parses the date string.
        dt = datetime.strptime(item['date'], "%Y-%m-%d")
        date_time = datetime(dt.year, dt.month, dt.day, 0, 0, 0)

        existing_document = collection.find_one({'datetime': date_time})

        # Note: This script gets delivery prices from newest to oldest.
        if (date_time >= i_date) and (existing_document is None):

            delivery_price = float(item['delivery_price'])

            # Gets the Unix date (in milliseconds).
            date_unix = int(time.mktime(date_time.timetuple()) * 1000)

            document = {
                        'datetime': date_time,
                        'unix_time': date_unix,
                        'index_price': delivery_price,
                        }        

            # Insert the document into the collection
            collection.insert_one(document)

            # Shows the job execution on terminal.
            print(offset, date_time, delivery_price)

        # The document is already saved in the database.
        elif date_time >= i_date:
            continue

        # If all work has been done... 
        else:
            print("The database is updated!")
            sys.exit()  # This will stop the script entirely


    # Updates the pagination and query parameters.
    offset += 1
    params = {
              'count': 1,
              'index_name': "btc_usd",
              'offset': offset,                      
             }

    # Updates query to new "while" iteration.
    response = requests.get(f"{prod_base_url}{endpoint}", params=params)
    if response.status_code == 200:
        data = response.json()
        delivery_price_list = data['result']['data']
