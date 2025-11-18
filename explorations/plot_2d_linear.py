###
### Plots a 2d (x, y) scatter graph.
###


import pymongo
import matplotlib.pyplot as plt

# Connects DB.
client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client['deribit_btc_options']
#collection = db['d_annealing_btc_trade_history_5min']
collection = db['btc_trade_history_5min']

# Defines the variables to be plotted.
x_name = 'inv_t'
y_name = 'z_d_annealing'

# Sets a query to filter documents.
query = {
         #"inv_t": {"$gte": 0.0, "$lte": 500.0},
         #"inv_t": {"$exists": True,},
         #"x": {"$gte": 0.95, "$lte": 1.05},
         #"z_d_annealing": {"$gte": 1.0},
         #"fz": {"$lte": 0.0000001, "$gt": 0.0},
         "option_type": "call", 
         "settlement_period": "day",
         #"strike": 95500,
         #"direction": "buy",
         #"unix_time": {"$gte": 1730419200000}, # 2023.01.01
         #"unix_time": {"$gte": 1696118400000, "$lte": 1698710400000}, # 2023.01.01 - 2023.01.30
         #"instrument_name": "BTC-26JUL24-44000-C", # Disperse data
         #"instrument_name": "BTC-15OCT24-67000-C", # Linear data
         #"instrument_name": "BTC-28NOV24-96500-C", # Linear data
         "instrument_name": "BTC-23DEC24-95500-C",
         #"$or": [{"instrument_name": "BTC-15OCT24-67000-C"}, {"instrument_name": "BTC-28NOV24-96500-C"}]
        }

# Extracts data to plot.
data = collection.find(query, {x_name: 1, y_name: 1})

# Inits lists to variables x and y.
x = []
y = []

# Counts (x, y) pairs that will be plotted.
c = 0

# Fills lists x and y with paired values.
for document in data:

    if document[x_name] and document[y_name]:

        x.append(document[x_name])
        y.append(document[y_name])

        # Updates the counter.
        c = c + 1

# Plots a 2D scatter graph
plt.scatter(x, y)
plt.title(f"Gráfico: {x_name} vs {y_name}")
plt.xlabel(f"{x_name}")
plt.ylabel(f"{y_name}")
plt.grid(True)
plt.show()

# Shows the number of plotted points.
print(c)