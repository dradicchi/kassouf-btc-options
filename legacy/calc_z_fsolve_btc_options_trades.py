###
### Calculates z for each historical trade using 'fsolve' approach.
###

## IMPORTANT: 
## Is recommended to update the BTC trade history database, by running the 
## "build_hist_btc_options_trades_5min.py" script before to execute this script.

from pymongo import MongoClient, DESCENDING, ASCENDING
from scipy.optimize import fsolve


##
## Support functions
##

def calculate_z_fsolve(x, y, z_initial, tolerance=1e-10, ):
    """
    Calculates the value of z for the given x and y based on the equation:
        y(z) = (1 + x^z)^(1/z) - 1

    Parameters:
    x (float): the value of x in the equation
    y (float): the target value of y(z)
    z_initial (float): an initial guess for the value of z
    tolerance: a value to accept an approximation of zero

    Returns:
    float: the calculated value of z

    Example usage:
    x = 1
    y = 3
    z = calculate_z_fsolve(x, y)
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

    # Uses 'fsolve' with the generalized Newton-Raphson method and the 
    # Levenberg-Marquardt algorithm.
    root, info, ier, msg = fsolve(func,
                                  z_initial,
                                  args=(x, y), 
                                  xtol=tolerance, 
                                  full_output=True)
    
    # Check if the solution has converged (ier = 1)
    if ier == 1:
        return root[0]
    else:
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
collection = db['fsolve_btc_trade_history_5min']

# An initial guess for z. Note: z is in 1 <= z < infinite interval.
z_initial = 1.0 

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
        z = calculate_z_fsolve(x, y, z_initial)
        # Tries to calculate 'z' with 'z_initial = 1.0'.
        if not z:
            z = calculate_z_fsolve(x, y, 1.0)
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

        # Sets a new value to 'z_initial'.
        if z:
            z_initial = z

    print("The job is done!")

else:

    print("There is nothing to do...")











