###
### Calculates z for each historical trade using 'bisect' approach.
###

## IMPORTANT: 
## Is recommended to update the BTC trade history database, by running the 
## "build_hist_btc_options_trades_5min.py" script before to execute this script.

import math
from pymongo import MongoClient, DESCENDING, ASCENDING
from scipy.optimize import bisect


##
## Support functions
##

def calculate_z_bisect(x, y, z_lower=1, z_upper=1000, tolerance=1e-10):
    """
    Calculates the value of z for the given x and y based on the equation:
        y(z) = (1 + x^z)^(1/z) - 1

    Parameters:
    x (float): the value of x in the equation
    y (float): the target value of y(z)
    z_initial (float): an initial guess for the value of z

    Returns:
    float: the calculated value of z

    Example usage:
    x = 1
    y = 3
    z = calculate_z_bisect(x, y)
    """

    # Defines the object function to find its root.
    #
    # Y = option price
    # X = BTC price
    # S = Strike price
    #
    # y = Y/S
    # x = X/S
    #
    # y(z) = (1 + x^z)^(1/z) - 1
    #
    # z = k1/t + k2E1 + k3E2 + k4x + k5 + error
    #
    # t = time to expiration (days)
    #
    # E1 = E1 is the slope of least squares line fitted to logarithms of the 
    # monthly mean price for common stock for the previus eleven months.
    #
    # E2 = E2 is the standard deviation of natural logarithms of the monthly 
    # mean price for the commos stock for the previous eleven months.
    def func(z, x, y):
        # Use logarithm to avoid errors due to calculations with very large 
        # numbers.
        try:
            log_term = z * math.log(x)
            exp_term = math.exp(log_term)
            log_expression = math.log(1 + exp_term)
            result = math.exp(log_expression / z) - 1 - y
            return result
        except:
            # If there is a big number.
            return float('inf') 

    # Tests if there is a signal change between z_lower and z_upper. If there is
    # no signal change, expands the interval.
    if func(z_lower, x, y) * func(z_upper, x, y) > 0:
        
        # Expands the interval.
        while func(z_lower, x, y) * func(z_upper, x, y) > 0 and z_lower > 1:
            z_lower -= 10  # Decrements it until to find a signal change.
            if z_lower <= 1:
                z_lower = 1
                break

        # Returns 'None' if a valid interval is not found.
        if func(z_lower, x, y) * func(z_upper, x, y) > 0:
            return None

    # Applies the bisection method to the valid interval.
    root = bisect(func, z_lower, z_upper, args=(x, y), xtol=tolerance)
    return root


def funz(z, x, y):
    """
    Returns the computed 'zero' to given x, y and z values.
    """
    if z:
        return ((1 + x**z)**(1/z)) - 1 - y
    else:
        return None


def extract_strike(string):
    """
    Extracts the strike price from a Deribit's instrument name.
    """
    parts = string.split('-')
    if len(parts) >= 3:
        return parts[-2]  # Returns the second-to-last item.
    else:
        return None


##
## Main script
##

# DB access.
client = MongoClient('mongodb://localhost:27017/')
db = client['deribit_btc_options']
collection = db['bisect_btc_trade_history_5min']

# Gets all documents that don't have a 'z' field.
z_filter = {"$or": [{"z": {"$exists": False}}, {"z": None}]}
trades = collection.find()

if trades:

    # Computes and stores 'z' to each historical trade.
    for trade in trades:

        # Extracts the strike price.
        strike = float(extract_strike(trade['instrument_name']))
        
        # Calculates 'z'.
        x = (trade['index_price'] / strike)
        y = ((trade['price'] * trade['index_price']) / strike) # Note: In USD.
        z = calculate_z_bisect(x, y)
        fz = funz(z, x, y)

        new_fields = {
                      "strike": strike,
                      "x": x,
                      "y": y,
                      "z": z,
                      "fz": fz,
                     }

        # Updates the source collection.
        collection.update_one({"_id": trade["_id"]}, {"$set": new_fields})

        # Shows the job execution on terminal.
        print(f"id: {str(trade['id'])} | z: {str(z)} | f: {str(fz)}")

    print("The job is done!")

else:

    print("There is nothing to do...")











