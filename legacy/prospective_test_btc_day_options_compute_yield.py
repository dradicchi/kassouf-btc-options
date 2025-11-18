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
mark_price_endpoint = f"get_mark_price_history"

# DB access.
client = MongoClient('mongodb://localhost:27017/')
db = client['deribit_btc_options']
delivery_price_collection = db['btc_delivery_price_daily']
instrument_collection = db['btc_day_inverse_options_offering']
prospec_test_collection = db['btc_day_prospective_test']
z_model_collection = db['btc_day_z_models']
adj_model_collection = db['btc_day_adj_models']

# td = datetime.datetime.now()
# td_adj = datetime.datetime(td.year, td.month, td.day, 0, 0, 0)

# Gets the current index price:
# index_price_params = {
#                       'index_name': "btc_usd",                 
#                      }

# response = requests.get(f"{prod_base_url}{index_price_endpoint}", 
#                         params=index_price_params)

# if response.status_code == 200:
#     data = response.json()
#     cur_index_price = float(data['result']['index_price'])

# Gets the datetime for the most oldest test trade:
last_document = prospec_test_collection.find_one(sort=[('datetime', 1)])
initial_datetime = last_document['datetime'] - datetime.timedelta(days=30)

# Gets the z-models and adj_models list.
z_models = z_model_collection.find()

# Iterates pairs of model-z / model-adj.
for z_model in z_models:

    print(z_model['model_name'], "\n\n===")

    adj_models = adj_model_collection.find()

    for adj_model in adj_models:

        # Starts counters.
        count_yield_positive = 0
        count_yield_negative = 0

        # Starts balances.
        n_yield_balance = 0         # normalized amount.
        b_yield_balance = 0         # book amount.

        # Gets the expired instruments list.
        expired_instruments = instrument_collection.find(
                 {'is_active': False,
                  #'option_type': "call",
                  'expiration_datetime': {'$gt': initial_datetime},
                  'instrument_name': { "$regex": "^BTC-15JUL24-" },
                 }).sort("expiration_datetime", DESCENDING)

        for instrument in expired_instruments:

            # Gets the current delivery price:
            td = instrument['expiration_datetime']
            td_adj = datetime.datetime(td.year, td.month, td.day, 0, 0, 0)
            delivery_price_doc = delivery_price_collection.find_one(
                                                           {'datetime': td_adj})
            delivery_price = delivery_price_doc['index_price']

            # Checks for expired trades.
            expired_trades = prospec_test_collection.find(
                                {'instrument': instrument['instrument_name'],
                                 'z_model' : z_model['model_name'],
                                 'adj_model' : adj_model['model_name'],
                                 #'bid': {'$gt': 0.0001} #################
                                }).sort("datetime", DESCENDING)

            # Iterates trades to calculate profits and losses.
            for trade in expired_trades:

                # the option / underlying ratio. 
                normalized_amount = 2

                ##
                ## Deribit transactions fees (taker).
                ## Source: https://www.deribit.com/kb/fees
                ##
                
                # BTC options trading (in BTC).
                fee_option_trade = min((trade['bid'] * 0.125), 0.0003)
                
                # BTC options delivery fee.
                if instrument['settlement_period'] == "day":
                    fee_option_delivery = 0.00
                else:
                    fee_option_delivery = min((trade['bid'] * 0.125), 0.00015)

                # BTC spot trading.
                fee_spot_trade = 0.0

                ##
                ## Calculating the trade yield.
                ##

                call_yield = (normalized_amount * ((trade['bid'] - fee_option_trade))) # * 
                               # trade['index_price']))

                # spot_yield = ((delivery_price - trade['index_price']) -
                #               (2 * fee_spot_trade))

                if delivery_price >= trade['vars']['strike']:
                    # The option was expired at/in the money.
                    delivery_loss = ((normalized_amount * ((trade['vars']['strike'] - 
                                     (delivery_price + (fee_option_delivery * 
                                                        delivery_price))))) / delivery_price)
                else:
                    # The option was expired out the money.
                    delivery_loss = 0.0

                # The total trade yield.
                trade_yield = call_yield + delivery_loss # + spot_yield

                ##
                ## Calculating margin requirement.
                ##

                # mark_p_instr = {
                #                 'instrument_name': instrument['instrument_name'],
                #                 'start_timestamp': ???,
                #                 'end_timestamp': ???,
                #                }

                # response = requests.get(f"{prod_base_url}{mark_price_endpoint}", 
                #                          params= mark_p_instr)

                # # Gets current instrument mark price.
                # if response.status_code == 200:
                #     data = response.json()
                #     cur_mark_p_instr = float(data['result'][0][1])

                # Handles with legacy trades (without 'instrument_mark_price' data).
                # try:
                #     instrument_mark_price = trade['instrument_mark_price']
                # except:
                #     margin_req = None
                # else:

                #     # Calculates the margin requirement.
                #     if instrument['strike'] > trade['index_price']:
                #         margin_req = (max((0.15 - ((instrument['strike'] - trade['index_price'])/trade['index_price'])), 0.1) + trade['instrument_mark_price'])
                #     else:
                #          margin_req = (0.15 + trade['instrument_mark_price'])
            
                #     if margin_req & (trade['instrument_mark_price'] >= trade['bid']):
                #         margin_req = margin_req + (margin_req - trade['instrument_mark_price'])

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
                print("| instr: ", instrument['instrument_name'], 
                      "| trade_dt: ", trade['datetime'], 
                      "| bid: ", "{:.4f}".format(trade['bid']), 
                      #"| qtd: ", "{:05.2f}".format(trade['amount']),
                      "| x: ", "{:.4f}".format(round(float(
                                                   trade['vars']['x']), 4)),
                      "| YdivY: ", "{:.4f}".format(round(float(
                                                   trade['vars']['YdivY']), 4)), 
                      #"| margin: ", margin_req,
                      "| yld_call: BTC ", round(call_yield, 3),
                      f"(USD {round((call_yield * delivery_price), 3)})",
                      "| lss_call: BTC ",  round(delivery_loss, 3),
                      f"(USD {round((delivery_loss * delivery_price), 3)})",
                      #"| yld_spt: ",  round(spot_yield, 3),
                      "| yld_trade: BTC ",  round(trade_yield, 3),
                      f"(USD {round((trade_yield * delivery_price), 3)})",)

                # Stores yield with trade data.

        # Shows the report:
        print("\n\n\n============================================")
        print(f"z-model: {z_model['model_name']}")
        print(f"adj-model: {adj_model['model_name']}")
        print(f"Yield positive trades: {count_yield_positive}")
        print(f"Yield negative trades: {count_yield_negative}")
        print(f"Yield balance - Normalized amount: BTC {n_yield_balance} (USD {round((n_yield_balance * delivery_price), 3)})")
        print(f"Yield balance - Book amount: BTC {b_yield_balance} (USD {round((b_yield_balance * delivery_price), 3)})")
        print("============================================\n\n\n")
        input("Press Enter to continue...")

print(f"End of computations!")




