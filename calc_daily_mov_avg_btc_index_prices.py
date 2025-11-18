###
### Calculates the daily BTC moving averages index price.
###

## IMPORTANT: 
## Is recommended to update the BTC trade history database, by running the 
## "build_hist_btc_daily_avg_index_price.py" script before to execute this 
## script.

## NOTES:
## The 'datetime' fields from 'btc_avg_index_price_daily' collection are 
## relative to 'São Paulo -3 GMT local time'. As the Deribit's BTC options 
## (daily, weekly and monthly) contracts expires at 8:00 AM GMT (5:00 AM at 
## São Paulo -3 GMT local time), soon the daily average price is calculed to 
## 5am-5am interval, at local time.

import math
import pandas as pd
from datetime import datetime
from pymongo import MongoClient, DESCENDING, ASCENDING


##
## Support functions
##

def is_number(num):
    """
    Tests if 'num' is a float/integer number.
    If true, returns the number. If not, returns 'None'.
    """
    if isinstance(num, (int, float)) and math.isnan(num):
        return None
    else:
        return num


##
## Main script
##

# DB access.
client = MongoClient('mongodb://localhost:27017/')
db = client['deribit_btc_options']
collection = db['btc_avg_index_price_daily']

cursor = collection.find().sort("datetime", ASCENDING)
documents = list(cursor)

if documents:

    # Loads the data to a Pandas DataFrame.
    data = []
    for doc in documents:
        data.append({
            '_id': doc['_id'],
            'datetime': doc['datetime'],
            'avg_index_price_daily': doc['avg_index_price_daily'],
        })

    df = pd.DataFrame(data)

    # Converts the datetime column to Pandas datetime format.
    df['datetime'] = pd.to_datetime(df['datetime'])

    # Calculates the moving averages.
    df['moving_avg_30d'] = df['avg_index_price_daily'].rolling(window=30).mean()
    df['moving_avg_60d'] = df['avg_index_price_daily'].rolling(window=60).mean()
    df['moving_avg_90d'] = df['avg_index_price_daily'].rolling(window=90).mean()
    df['moving_avg_120d'] = df['avg_index_price_daily'].rolling(window=120).mean()
    df['moving_avg_180d'] = df['avg_index_price_daily'].rolling(window=180).mean()
    # 200d - To Meyer's Multiple calculation.
    df['moving_avg_200d'] = df['avg_index_price_daily'].rolling(window=200).mean()
    df['moving_avg_365d'] = df['avg_index_price_daily'].rolling(window=365).mean()


    # Updates each document.
    for index, row in df.iterrows():    

        collection.update_one(
            {'_id': row['_id']}, 
            {'$set': {
                'moving_avg_30d': is_number(row['moving_avg_30d']),
                'moving_avg_60d': is_number(row['moving_avg_60d']),
                'moving_avg_90d': is_number(row['moving_avg_90d']),
                'moving_avg_120d': is_number(row['moving_avg_120d']),
                'moving_avg_180d': is_number(row['moving_avg_180d']),
                'moving_avg_200d': is_number(row['moving_avg_200d']),
                'moving_avg_365d': is_number(row['moving_avg_365d']),
            }}
        )

        print(f"id: {row['_id']} | mov_avg_30: {row['moving_avg_30d']}")

    print("The job is done!")

else:

    print("The data source returned empty!")






