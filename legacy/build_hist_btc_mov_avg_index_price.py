###
### Builds the BTC index price moving average database.
###

## IMPORTANT: 
## Is recommended to update the BTC index price daily average database, by 
## running the "build_hist_btc_daily_avg_index_price.py" before to execute this 
## script.

import datetime
import time
from pymongo import MongoClient, DESCENDING

# DB access.
client = MongoClient('mongodb://localhost:27017/')
db = client['deribit_btc_options']
in_collection = db['btc_avg_index_price_daily'] # source DB.
out_collection = db['btc_mov_avg_index_price'] # target DB.

# Finds the document with the most recent date value.
last_document = out_collection.find_one(sort=[('datetime', -1)])

# Iteration interval - Initial value.
if last_document is None:
    # To (re)build the entire database.
    out_collection.create_index([('datetime', DESCENDING)])
    i_date = datetime.datetime(2017, 1, 1, 0, 0, 0)
else:
    # To update the database.
    ls_dt = last_document['datetime']
    i_date = datetime.datetime(ls_dt.year, ls_dt.month, ls_dt.day, 0, 0, 0)
    i_date += datetime.timedelta(days=1)

# Iteration interval - Final value.
td = datetime.date.today()
f_date = datetime.datetime(td.year, td.month, td.day, 0, 0, 0)
f_date -= datetime.timedelta(days=1)

# Moving averages steps (in days).
mv_avg_steps = [30, 60, 90, 120, 180, 240, 365]

# Iterates historical hourly index prices to calculate the average daily price.
while i_date <= f_date:

    for step in mv_avg_steps:

        # Aggregation pipeline to match documents within the specific day, sum 
        # the price values, and count the documents.
        pipeline = [
            {'$match': 
                {'datetime': {'$gte': i_date, 
                              '$lt': (i_date + datetime.timedelta(days=1))}}},
            {'$group': {
                '_id': None,
                'sum_price': {'$sum': '$index_price'},
                'count': {'$sum': 1}
            }}
        ]

        # Executes the aggregation pipeline
        result = list(in_collection.aggregate(pipeline))

        if result:
        
            # Gets the Unix date.
            date_unix = int(time.mktime(i_date.timetuple()) * 1000)

            # Calculates the daily average to index prices.
            sum_price = float(result[0]['sum_price'])
            count = int(result[0]['count'])
            avg_index_price_daily = sum_price / count

            document = {
                       'datetime': i_date,
                       'unix_time': date_unix,
                       'avg_index_price_daily': avg_index_price_daily,
                       'mv_avg_30d_index_price': mv_avg_30d_index_price,
                       'mv_avg_60d_index_price': mv_avg_60d_index_price,
                       'mv_avg_90d_index_price': mv_avg_90d_index_price,
                       'mv_avg_120d_index_price': mv_avg_120d_index_price,
                       'mv_avg_180d_index_price': mv_avg_180d_index_price,
                       'mv_avg_240d_index_price': mv_avg_240d_index_price,
                       'mv_avg_365d_index_price': mv_avg_365d_index_price,
                        }

            # Insert the document into the target collection
            out_collection.insert_one(document)

            # Shows the job execution on terminal.
            print(i_date, avg_index_price_daily, mv_avg_30d_index_price)

            # Sets a new hourly cycle.
            i_date += datetime.timedelta(days=1)

        else:
            print("Run 'build_hist_btc_daily_avg_index_price.py' " + 
                  "to update the source database.")
            break




