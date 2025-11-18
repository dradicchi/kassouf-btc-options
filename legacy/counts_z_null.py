from pymongo import MongoClient
from datetime import datetime

# Connects DB.
client = MongoClient('mongodb://localhost:27017/')
db = client['deribit_btc_options']
collection = db['btc_trade_history_5min']

# An inferior limit date to query.
inf_limit = datetime(2017, 1, 1)

# Counts docs with z = 'null'.
count = collection.count_documents({
                                    "z": None,
                                    "date_time": {"$gte": inf_limit},
                                  })

# Shows the result.
print(f"Qty docs z = null with dt >= {inf_limit}: {count}")