###
### Calculates the inverse of the instrument life-cycle moment to each 
### historical trade
###

## IMPORTANT: 
## Before to execute this script is recommended to update the BTC trade history 
## database and the instrument historical offering database, by running 
## respectively:
## (1) "build_hist_btc_options_trades_5min.py"
## (2) "build_hist_btc_inverse_options_offering.py"

## NOTES:
## All 'datetime' fields from 'btc_trade_history_5min' and 
##'btc_inverse_options_offering' collections are relative to 'São Paulo -3 GMT 
## local time'. As the Deribit's BTC options (daily, weekly and monthly) 
## contracts expires at 8:00 AM GMT (5:00 AM at São Paulo -3 GMT local time). 
## All Unix time fields are in miliseconds.


import datetime
from pymongo import MongoClient, DESCENDING, ASCENDING


# DB access.
client = MongoClient('mongodb://localhost:27017/')
db = client['deribit_btc_options']
trades_collection = db['btc_trade_history_5min']
instr_collection = db['btc_inverse_options_offering']

# Gets all documents that don't have a 't' field.
invt_filter = {"inv_t": {"$exists": False}}
trades = trades_collection.find(invt_filter)

if trades:

    # Computes and stores 't' to each historical trade.
    for trade in trades:

        # Gets the trade instant as unix time.
        t_trade = trade['unix_time'] 

        # Gets the creation and expiration unix time.
        ins_name = trade["instrument_name"]
        instrument = instr_collection.find_one(
                                               {"instrument_name": ins_name}, 
                                               {"expiration_unix_timestamp": 1,
                                                "creation_unix_timestamp": 1},
                                              )
        if instrument:

            t_expiration = instrument["expiration_unix_timestamp"]
            t_creation = instrument["creation_unix_timestamp"]

            # Instrument life (in miliseconds):
            instr_life = t_expiration - t_creation

            # Calculates 'inv_t' as the inverse of the time remaining before the 
            # instrument expires divided by the instrument life. 
            # Note: z is directly correlated with 1/t.
            inv_t = (1 / ((t_expiration - t_trade) / instr_life))

            new_fields = {
                          "inv_t": inv_t,
                         }

            # Updates the trade collection.
            trades_collection.update_one(
                                         {"_id": trade["_id"]}, 
                                         {"$set": new_fields},
                                        )

            # Shows the job execution on terminal.
            print(f"id: {str(trade['id'])} | inv_t: {str(inv_t)}")
        
    print("The job is done!")

else:

    print("There is nothing to do...")








