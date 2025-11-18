###
### Computes yields to a prospective test with BTC day options.
###

## IMPORTANT: 
## Is recommended to update all the historic databases, at least once time a 
## day (running "run_build_scripts.py"), before to execute this script. Is 
## especially important update the expired instruments status and the delivery
## price database.

import datetime
import requests
from pymongo import MongoClient, DESCENDING, ASCENDING

# API definitions.
prod_base_url = "https://deribit.com/api/v2/public/"
index_price_endpoint = f"get_index_price"
order_book_endpoint = f"get_order_book"

# DB access.
client = MongoClient('mongodb://localhost:27017/')
db = client['deribit_btc_options']
delivery_price_collection = db['btc_delivery_price_daily']
instrument_collection = db['btc_day_inverse_options_offer']
prospec_test_collection = db['btc_day_prospective_test']
z_model_collection = db['btc_day_z_models']
adj_model_collection = db['btc_day_adj_models']

td = datetime.datetime.now()
td_adj = datetime.datetime(td.year, td.month, td.day, 0, 0, 0)

# Gets the current index price:
index_price_params = {
                      'index_name': "btc_usd",                 
                     }

response = requests.get(f"{prod_base_url}{index_price_endpoint}", 
                        params=index_price_params)

if response.status_code == 200:
    data = response.json()
    cur_index_price = float(data['result']['index_price'])

# Gets the current delivery price:
delivery_price_doc = delivery_price_collection.find_one({'datetime': td_adj})
delivery_price = delivery_price_doc['index_price']

# Gets the datetime for the most oldest test trade:
last_document = prospec_test_collection.find_one(sort=[('datetime', 1)])
initial_datetime = last_document['datetime'] - datetime.timedelta(days=30)

# Gets the z-models and adj_models list.
z_models = z_model_collection.find()
adj_models = adj_model_collection.find()

# Iterates pairs of model-z / model-adj.
for z_model in z_models:
    for adj_model in adj_models:

        # Starts counters.
        count_yield_positive = 0
        count_yield_negative = 0

        # Starts balances.
        n_yield_balance = 0         # normalized amount.
        b_yield_balance = 0         # book amount.

        # Gets the expired instruments list.
        expired_instruments = instrument_collection.find(
                 {"expiration_datetime": {"$gt": initial_datetime}}).sort("expiration_datetime", DESCENDING)

        for instrument in expired_instruments:

            # Gets the current order book:
            order_book_params = {
                                 'instrument_name': instrument['instrument_name'],
                                 'depth': 100 # [1, 5, 10, 20, 50, 100, 1000, 10000]                      
                                 }

            response = requests.get(f"{prod_base_url}{order_book_endpoint}", 
                                                           params=order_book_params)

            if response.status_code == 200:
                current_order_book = response.json()

            # Gets the current open bids:
            try:
                first_bid_value = current_order_book['result']['bids'][0][0]
            except:
                first_bid_value = None

            # Checks for expired trades.
            expired_trades = prospec_test_collection.find(
                                {'instrument': instrument['instrument_name'],
                                 'z_model' : z_model['model_name'],
                                 'adj_model' : adj_model['model_name'],}
                                ).sort("datetime", DESCENDING)

            # Calculates the trade yield.
            for trade in expired_trades:

                normalized_amount = 2
                fee = 0.99    # An "inversed" fee to gets the net yield.

                if first_bid_value:
                    trade_yield = ((normalized_amount * 
                                         (trade['bid'] * 0.99 * cur_index_price)) - 
                                   (normalized_amount * 
                                         (first_bid_value * 1.01 * cur_index_price)) - 
                                   (trade['index_price'] * 1.01) + 
                                   (cur_index_price * 0.99))
                else:
                    trade_yield = 0.0

                # Updates counters.
                if trade_yield >= 0:
                    count_yield_positive += 1
                else:
                    count_yield_negative += 1

                # Updates balances:
                n_yield_balance = n_yield_balance + trade_yield                  
                b_yield_balance = b_yield_balance + (trade_yield * 
                                                     trade['amount'])

                # Shows the work in processing.
                print(z_model['model_name'], adj_model['model_name'], 
                        instrument['instrument_name'], trade['datetime'], 
                        trade['bid'], trade['amount'], trade['vars']['YdivY'], 
                        trade_yield)

                # Stores yield with trade data.

        # Shows the report:
        print("\n\n\n============================================")
        print(f"z-model: {z_model['model_name']}")
        print(f"adj-model: {adj_model['model_name']}")
        print(f"Yield positive trades: {count_yield_positive}")
        print(f"Yield negative trades: {count_yield_negative}")
        print(f"Yield balance - Normalized amount: {n_yield_balance}")
        print(f"Yield balance - Book amount: {b_yield_balance}")
        print("============================================\n\n\n")

print(f"End of computations!")




