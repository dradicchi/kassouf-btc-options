###
### Plots the BTC max/min daily price ratio distribution.
###

## IMPORTANT: 
## Is recommended to update the BTC index price database, by running the 
## "build_hist_btc_daily_avg_index_price.py" script before to execute this 
## script.

## NOTES:
## The 'datetime' and 'unix_time' fields from 'btc_avg_index_price_daily' 
## collection are relative to 'São Paulo -3 GMT local time'. As the Deribit's 
## BTC options (daily, weekly and monthly) contracts expires at 8:00 AM GMT 
## (5:00 AM at São Paulo -3 GMT local time), soon the daily average price is 
## calculed to 5am-5am interval, at local time.


import pymongo
import matplotlib.pyplot as plt

# DB access.
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client['deribit_btc_options']
collection = db['btc_avg_index_price_daily']

# Retrieves the data.
documents = collection.find()

# An empty list to store the computed ratios.
ratios = []

# Calculates the BTC max/min daily price ratio.
for doc in documents:
    max_price = doc['max_index_price_daily']
    min_price = doc['min_index_price_daily']
    ratio = max_price / min_price
    ratios.append(ratio)

# Plots the histogram.
plt.hist(ratios, bins=20, edgecolor='black')
plt.title('Distribuição das Razões Max/Min Diário')
plt.xlabel('Razão (Max/Min)')
plt.ylabel('Frequência')
plt.show()