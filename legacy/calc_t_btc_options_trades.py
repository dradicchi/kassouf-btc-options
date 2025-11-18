###
### Calculates 1/(t_expiration - t_trade) for each historical trade.
###

## IMPORTANT: 
## Is recommended to update the BTC trade history database, by running the 
## "build_hist_btc_options_trades_5min.py" script before to execute this script.

## NOTES:
## The 'datetime' fields from 'btc_trade_history_5min' collection are 
## relative to 'São Paulo -3 GMT local time'. As the Deribit's BTC options 
## (daily, weekly and monthly) contracts expires at 8:00 AM GMT (5:00 AM at 
## São Paulo -3 GMT local time). Unix time are in miliseconds.

import re
import datetime
from pymongo import MongoClient, DESCENDING, ASCENDING
from scipy.optimize import fsolve


##
## Support functions
##

def extract_date(option_string):
    """
    Takes a string formatted as "DMMMAA" or "DDMMMAA" and returns the date 
    elements separately.
    """
    # Defines a dictionary to map month names with numbers.
    month_map = {
        "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04",
        "MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08",
        "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12",
    }

    # Defines a REGEX pattern to match the date format (DMMMAA or DDMMMAA).
    date_pattern = r'-(\d{1,2})([A-Z]{3})(\d{2})-'

    # Applies the REGEX to find date information.
    match = re.search(date_pattern, option_string)
    
    if match:
        # Extracts day, month and year.
        day = match.group(1)
        month_str = match.group(2)
        year_short = match.group(3)
        
        # Converts the month name to the respective number.
        month = month_map.get(month_str.upper())
        
        # Appends the '20' prefix to the year name (formating as AAAA).
        year = "20" + year_short
        
        # Retuns tha date elements.
        return day, month, year
    else:
        return None


##
## Main script
##

# DB access.
client = MongoClient('mongodb://localhost:27017/')
db = client['deribit_btc_options']
trades_collection = db['btc_trade_history_5min']
#ins_collection = db['btc_inverse_options_offering']

# Gets all documents that don't have a 't' field.
t_filter = {"t": {"$exists": False}}
trades = trades_collection.find(t_filter)

if trades:

    # Computes and stores 't' to each historical trade.
    for trade in trades:

        # Gets the trade instant as unix time.
        t_trade = trade['unix_time'] 

        # Gets expiration unix time.
        ins_name = trade["instrument_name"]
        # instrument = ins_collection.find_one(
        #                                      {"instrument_name": ins_name}, 
        #                                      {"expiration_unix_timestamp": 1},
        #                                     )

        # t_expiration = instrument.get("expiration_unix_timestamp")
        day, month, year = extract_date(ins_name)
        # Local time (GMT -3).
        dt_exp = datetime.datetime(int(year), int(month), int(day), 5, 0, 0)
        # In miliseconds.
        t_expiration = int(dt_exp.timestamp() * 1000)

        # Calculates 't' as the inverse of the time remaining before the 
        # instrument expires, in hours. Note: z is directly correlated with 1/t.
        inv_t = (1 / ((t_expiration - t_trade) / 3600000))

        new_fields = {
                      "inv_t": inv_t,
                     }

        # Updates the trade collection.
        trades_collection.update_one(
                                     {"_id": trade["_id"]}, 
                                     {"$set": new_fields},
                                    )

        # Shows the job execution on terminal.
        print(f"id: {str(trade['id'])} | inv_t: {str(t)}")
        
    print("The job is done!")

else:

    print("There is nothing to do...")








