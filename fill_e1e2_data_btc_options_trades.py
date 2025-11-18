###
### Fills trend and volatility data in a historical trade database.
###

## IMPORTANT: 
## Before to execute this script is recommended to update the BTC trade history 
## database and the average BTC prices database, by running 
## respectively:
## (1) "build_hist_btc_options_trades_5min.py"
## (2) "calc_e1e2_hourly_btc_avg_index_price.py"
## (3) "calc_e1e2_daily_btc_avg_index_price.py"

## NOTES:
## All 'datetime' fields are relative to 'São Paulo -3 GMT local time'. As the 
## Deribit's BTC options (daily, weekly and monthly) contracts expires at 8:00 
## AM GMT (5:00 AM at São Paulo -3 GMT local time), soon the daily average price
## is calculed to 5am-5am interval, at local time. All Unix time fields are in 
## miliseconds.


from datetime import datetime, timedelta
from pymongo import MongoClient, DESCENDING, ASCENDING


##
## Support functions
##

def round_to_nearest_hour(dt):
    """
    Rounds a datetime data to the nearest full hour.
    """
    # Calculates the remaining minutes in the hour.
    remaining_minutes = dt.minute + dt.second / 60 + dt.microsecond / 60_000_000

    # If it exceeds 30 minutes, round up.
    if remaining_minutes >= 30:
        return (dt + timedelta(hours=1)).replace(minute=0, 
                                                 second=0, 
                                                 microsecond=0)
    # Otherwise round down.
    else:
        return dt.replace(minute=0, second=0, microsecond=0)


def round_to_nearest_day(dt):
    """
    Rounds a datetime data to the nearest full day.
    """
    # Calculates the remaining hours in the day.
    remaining_hours = (dt.hour + 
                       dt.minute / 60 + 
                       dt.second / 3600 + 
                       dt.microsecond / 3_600_000_000)

    # If it exceeds 12 hours, round up to the next day.
    if remaining_hours >= 12:
        return (dt + timedelta(days=1)).replace(hour=0, 
                                                minute=0, 
                                                second=0, 
                                                microsecond=0)

    # Otherwise, round to the start of the current day.
    else:
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)


##
## Main script
##

# DB access.
client = MongoClient('mongodb://localhost:27017/')
db = client['deribit_btc_options']
trades_collection = db['btc_trade_history_5min']
hourly_e1e2_collection = db['btc_avg_index_price_hourly']
daily_e1e2_collection = db['btc_avg_index_price_daily']


# Gets all trades that don't have a 'e1_...' field.
# NOTES: Daily instruments will work with 24h or 72h moving average e1/e2 values;
# Weekly and monthly contracts will work with 30d or 90d moving average e1/e2 
# values. The daily cycle is defined as the 5am-5am interval.
instr_filter = {"$and": [
                           {"e1_24h": {"$exists": False}},
                           {"e1_72h": {"$exists": False}},
                           {"e1_30d": {"$exists": False}},
                           {"e1_90d": {"$exists": False}},
                        ]}
trades = trades_collection.find(instr_filter)

if trades:

    period = "settlement_period" # A short alias. 

    # Retrieves and stores the respective enviromental data to each historical 
    # trade.
    for trade in trades:

        new_fields = None

        # Processes a daily contract.
        if (period in trade) and (trade[period] == "day"):

            
            # Daily window filter.
            trade_dt = trade["date_time"]
            rounded_dt = round_to_nearest_hour(trade_dt)
            avg_filter = {"datetime": rounded_dt}

            # Gets the trade window's average data.
            avg_data = hourly_e1e2_collection.find_one(avg_filter,
                                                       {
                                                          "e1_24h": 1,
                                                          "e1_72h": 1,
                                                          "e2_24h": 1,
                                                          "e2_72h": 1,
                                                       })
            
            if avg_data and (("e1_24h" and not "e1_72h") in avg_data):
                new_fields = {
                                "e1_24h": avg_data["e1_24h"],
                                "e1_72h": None,
                                "e2_24h": avg_data["e2_24h"],
                                "e2_72h": None,
                             }

            elif avg_data and (("e1_24h" and "e1_72h") in avg_data):
                    new_fields = {
                                    "e1_24h": avg_data["e1_24h"],
                                    "e1_72h": avg_data["e1_72h"],
                                    "e2_24h": avg_data["e2_24h"],
                                    "e2_72h": avg_data["e2_72h"],
                                 }

            else:
                continue            

            if new_fields:
                # Updates the trade collection.
                trades_collection.update_one(
                                             {"_id": trade["_id"]}, 
                                             {"$set": new_fields},
                                            )

                # Shows the job execution on terminal.
                print(f"id: {str(trade['id'])} | e1_24h: {str(new_fields['e1_24h'])}")


        # Processes a weekly or monthly contract.
        elif period in trade and ((trade[period] == "month") or 
                                  (trade[period] == "week")):

            # Monthly / Weekly window filter.
            trade_dt = trade["date_time"]
            rounded_dt = round_to_nearest_day(trade_dt)
            adj_rounded_dt = rounded_dt + timedelta(hours=5)
            avg_filter = {"datetime": adj_rounded_dt}

            # Gets the trade window's average data.
            avg_data = daily_e1e2_collection.find_one(avg_filter,
                                                      {
                                                        "e1_30d": 1,
                                                        "e1_90d": 1,
                                                        "e2_30d": 1,
                                                        "e2_90d": 1,
                                                      })

            if avg_data and (("e1_30d" and not "e1_90d") in avg_data):
                new_fields = {
                                "e1_30d": avg_data["e1_30d"],
                                "e1_90d": None,
                                "e2_30d": avg_data["e2_30d"],
                                "e2_90d": None,
                             }
            elif avg_data and (("e1_30d" and "e1_90d") in avg_data):
                    new_fields = {
                                    "e1_30d": avg_data["e1_30d"],
                                    "e1_90d": avg_data["e1_90d"],
                                    "e2_30d": avg_data["e2_30d"],
                                    "e2_90d": avg_data["e2_90d"],
                                 }

            else:
                continue

            if new_fields:
                # Updates the trade collection.
                trades_collection.update_one(
                                             {"_id": trade["_id"]}, 
                                             {"$set": new_fields},
                                            )

                # Shows the job execution on terminal.
                print(f"id: {str(trade['id'])} | e1_30d: {str(new_fields['e1_30d'])}")


        # If the contract doesn't have a 'period' field.
        else:
            continue

        
    print("The job is done!")

else:

    print("There is nothing to do...")








