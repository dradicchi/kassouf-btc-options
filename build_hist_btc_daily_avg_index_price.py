###
### Builds the BTC index price daily average database.
###

## IMPORTANT: 
## Is recommended to update the BTC index price database, by running the 
## "build_hist_btc_index_price_5min.py" script before to execute this script.

## NOTES:
## The 'datetime' fields from 'btc_avg_index_price_daily' collection are 
## relative to 'São Paulo -3 GMT local time'. As the Deribit's BTC options 
## (daily, weekly and monthly) contracts expires at 8:00 AM GMT (5:00 AM at 
## São Paulo -3 GMT local time), soon the daily average price is calculed to 
## 5am-5am interval, at local time.

import datetime
import time
from pymongo import MongoClient, DESCENDING, ASCENDING
import sys
import numpy as np

# DB access.
client = MongoClient('mongodb://localhost:27017/')
db = client['deribit_btc_options']
in_collection = db['btc_index_price_5min'] # source DB.
out_collection = db['btc_avg_index_price_daily'] # target BD.

# Finds the document with the most recent date value.
last_document = out_collection.find_one(sort=[('datetime', -1)])

# Iteration interval - Initial value.
if last_document is None:
    # To (re)build the entire database.
    out_collection.create_index([('datetime', ASCENDING)])
    out_collection.create_index([('datetime', DESCENDING)])
    i_date = datetime.datetime(2017, 1, 1, 5, 0, 0)
else:
    # To update the database.
    ls_dt = last_document['datetime']
    i_date = ls_dt + datetime.timedelta(days=1)

# Iteration interval - Final value.
td = datetime.date.today()
f_date = datetime.datetime(td.year, td.month, td.day, 5, 0, 0)
f_date -= datetime.timedelta(days=1)

# Iterates historical hourly index prices to calculate the average daily price.
while i_date <= f_date:

    # Aggregation pipeline to match documents within the specific day, sum the 
    # price values, and count the documents.
    pipeline = [
        {'$match': 
            {'datetime': {'$gte': i_date, 
                          '$lt': (i_date + datetime.timedelta(days=1))}}},
        {'$group': {
            '_id': None,
            'prices': {'$push': '$index_price'},
            'sum_price': {'$sum': '$index_price'},
            'max_price': {'$max': '$index_price'},
            'min_price': {'$min': '$index_price'},
            'count': {'$sum': 1},
        }}
    ]

    # Executes the aggregation pipeline.
    result = list(in_collection.aggregate(pipeline))

    if result:
    
        # Gets the Unix date.
        date_unix = int(time.mktime(i_date.timetuple()) * 1000)

        # Calculates the daily average to index prices.
        sum_price = float(result[0]['sum_price'])
        count = int(result[0]['count'])
        avg_index_price_daily = sum_price / count

        # Calculates the daily standard deviation.
        prices = result[0]['prices']
        # Note: uses "ddof=1" for a data sample, and "ddof=0" for the population
        std_dev = np.std(prices, ddof=1)

        document = {
                   'datetime': i_date,
                   'unix_time': date_unix,
                   'avg_index_price_daily': avg_index_price_daily,
                   'std_dev_index_price_daily' : std_dev,
                   'max_index_price_daily' : float(result[0]['max_price']),
                   'min_index_price_daily' : float(result[0]['min_price']),
                   'prices' : prices,
                    }

        # Insert the document into the target collection
        out_collection.insert_one(document)

        # Shows the job execution on terminal.
        print(i_date, avg_index_price_daily)

        # Sets a new hourly cycle.
        i_date += datetime.timedelta(days=1)

    else:
        print("Run 'build_..._index_price_5min.py' to update the source db.")
        sys.exit()  # This will stop the script entirely

print("The database is updated!")


