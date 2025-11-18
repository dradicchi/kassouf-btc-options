###
### Fills some respective instrument data in a historical trade database.
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
trades_collection = db['btc_trade_history_5min']
instr_collection = db['btc_inverse_options_offering']

# Gets all documents that don't have a 'instr_settlement_period' field.
instr_filter = {"settlement_period": {"$exists": False}}
trades = trades_collection.find(instr_filter)

if trades:

    # Retrieves and stores respective instrument data to each historical trade.
    for trade in trades:

        # Gets the instrument data.
        instr_name = trade['instrument_name']
        instr_data = instr_collection.find_one(
                                               {"instrument_name": instr_name}, 
                                               {
                                                "settlement_period": 1,
                                                "option_type": 1,
                                                },
                                              )
        if instr_data:
            
            new_fields = {
                          "settlement_period": instr_data['settlement_period'],
                          "option_type": instr_data['option_type'],
                         }

            # Updates the trade collection.
            trades_collection.update_one(
                                         {"_id": trade["_id"]}, 
                                         {"$set": new_fields},
                                        )

            # Shows the job execution on terminal.
            print(f"id: {str(trade['id'])}")
        
    print("The job is done!")

else:

    print("There is nothing to do...")








