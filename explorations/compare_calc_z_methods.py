###
### Compares results to different 'z' colculation methods.
###

from pymongo import MongoClient
from datetime import datetime


# Connects to DB.
client = MongoClient('mongodb://localhost:27017/')
db = client['deribit_btc_options']

# Lists collections to different 'z' colculation methods.
coll_lists = [
              "fsolve_btc_trade_history_5min", 
              "brentq_btc_trade_history_5min", 
              "bisect_btc_trade_history_5min", 
              "d_annealing_btc_trade_history_5min",
             ]

for col in coll_lists:

    # Sets the aimed collection.
    collection = db[col]

    # Defines the cut-off date as 01 January 2020.
    cutoff_dt = datetime(2022, 12, 31, 21, 0, 0)
    cutoff_ut = int(cutoff_dt.timestamp() * 1000)

    # Counts docs with 'date_time' >= 1/jan/2020 (ut >= 1577836800000).
    count_total = collection.count_documents({'unix_time': {'$gte': cutoff_ut}})

    # Counts docs with 'z' == 'None' and 'date_time' >= 1/jan/2020.
    count_z_none = collection.count_documents({
                                               'z': {'$ne': None},
                                               'unix_time': {'$gte': cutoff_ut}
                                             })

    # Counts docs with 'fz' <= 0.0000001 e 'date_time' >= 1/jan/2020.
    count_fz_small = collection.count_documents({
                                                'fz': {'$lte': 1.0e-4, '$gte': -01.0e-4,},
                                                'unix_time': {'$gte': cutoff_ut}
                                               })

    # Prints the report.
    print("\n\n==============================================")
    print(f"Collection: {col.upper()}")
    print("----------------------------------------------")
    print(f"Cut off date: \t{cutoff_dt} (GMT -3)")
    print("----------------------------------------------")
    print(f"Total docs: \t\t{count_total}")
    print(f"Total 'z' = 'None': \t{count_z_none}")
    print(f"Total 'fz' <= 1e-6: \t{count_fz_small}")
    print("==============================================")



