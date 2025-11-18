###
### Transponds data between trade historical databases.
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


from pymongo import MongoClient, DESCENDING, ASCENDING


# DB access.
client = MongoClient('mongodb://localhost:27017/')
db = client['deribit_btc_options']
origin_collection = db['fsolve_btc_trade_history_5min']
recipient_collection = db['d_annealing_btc_trade_history_5min']

# Gets all recipient documents.
tr_filter = {"inv_t": {"$exists": False}}
trades = recipient_collection.find(tr_filter)

if trades:

    for trade in trades:

        # Gets the origin data to transpond.
        id_trade = trade['id']
        ut_trade = trade["unix_time"] 
        origin_data = origin_collection.find_one(
                                                 {
                                                  "id": id_trade,
                                                  "unix_time": ut_trade,
                                                 }, 
                                                 {
                                                  "inv_t": 1,
                                                  "settlement_period": 1,
                                                  "option_type": 1,
                                                 },
                                                )

        new_fields = {
                      "inv_t": origin_data['inv_t'],
                      "settlement_period": origin_data['settlement_period'],
                      "option_type": origin_data['option_type'],
                     }

        # Updates the trade collection.
        recipient_collection.update_one(
                                     {"_id": trade["_id"]}, 
                                     {"$set": new_fields},
                                    )

        # Shows the job execution on terminal.
        print(f"id: {str(trade['id'])} updated")
        
    print("The job is done!")

else:

    print("There is nothing to do...")








