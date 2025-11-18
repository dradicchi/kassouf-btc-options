###
### Calculates z for each historical trade using 'brentq' approach.
###

## IMPORTANT: 
## Is recommended to update the BTC trade history database, by running the 
## "build_hist_btc_options_trades_5min.py" script before to execute this script.

from pymongo import MongoClient, DESCENDING, ASCENDING
from scipy.optimize import brentq


##
## Support functions
##

def calculate_z_brentq(x, y):
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
    z = calculate_z_brentq(x, y)
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
        return ((1 + x**z)**(1/z)) - 1 - y


    def find_interval(x, y, z_init=1.0, step=1.0, max_limit=1000000.0):
        """
        Finds a valid interval to use Brent's method.
        """
        z1 = z_init
        z2 = z1 + step

        while z2 <= max_limit:
            # Change of sign indicates root in the interval.
            if func(z1, x, y) * func(z2, x, y) < 0:
                return z1, z2

            z1 = z2
            z2 += step

        # returns 'None' if it does not find a valid interval.
        z1 = None
        z2 = None
        return z1, z2


    # Uses Brent's method. It is more flexible and works well for finding roots 
    # of non-linear functions even if the derivative is not available.
    try:
        z1, z2 = find_interval(x, y)
        return brentq(func, z1, z2, args=(x, y), maxiter=10000)
    except:
        return None


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
collection = db['brentq_btc_trade_history_5min']

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
        z = calculate_z_brentq(x, y)
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











