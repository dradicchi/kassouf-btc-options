
import numpy as np
import statsmodels.api as sm
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed

def process_instrument(instrument_name, trades_collection):
    """Processes a single instrument to calculate regression model parameters."""
    trades = list(trades_collection.find({"instrument_name": instrument_name}))
    if not trades:
        return instrument_name, None  # Return None if no transactions are found

    # Transform trades into a NumPy array (adjust as needed for your data)
    data = np.array([[t.get('x1', 0), t.get('x2', 0), t.get('y', 0)] for t in trades])

    # Ensure there's sufficient data to perform regression
    if data.shape[0] < 2 or data.shape[1] < 2:
        return instrument_name, None  # Not enough data

    # Calculate model parameters
    X = data[:, :-1]
    y = data[:, -1]
    X_with_constant = sm.add_constant(X)
    model = sm.OLS(y, X_with_constant).fit()

    return instrument_name, {
        "params": model.params.tolist(),
        "r_squared": model.rsquared,
        "standard_error": np.sqrt(model.scale),
    }

# MongoDB configuration
client = MongoClient('mongodb://localhost:27017/')
db = client['deribit_btc_options']
trades_collection = db['btc_trade_history_5min']
instr_collection = db['btc_inverse_options_offering']
model_collection = db['btc_da_model_option_price_by_instrument']

# Fetching unmapped instruments
expired_instr = instr_collection.find({"is_active": False})
modeled_instr = model_collection.find({}, {"instrument_name": 1})
modeled_instr_names = {doc["instrument_name"] for doc in modeled_instr}

unmodeled_instr = [
    instr["instrument_name"] for instr in expired_instr
    if instr["instrument_name"] not in modeled_instr_names
]

# Process instruments in parallel
results = []
with ThreadPoolExecutor(max_workers=8) as executor:
    futures = {
        executor.submit(process_instrument, instr, trades_collection): instr
        for instr in unmodeled_instr
    }
    for future in as_completed(futures):
        try:
            instrument_name, result = future.result()
            if result:
                results.append({"instrument_name": instrument_name, **result})
        except Exception as e:
            print(f"Error processing {futures[future]}: {e}")

# Insert results into MongoDB
if results:
    model_collection.insert_many(results)
