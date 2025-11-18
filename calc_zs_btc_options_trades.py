###
### Calculates z for each historical trade using four different approaches.
###

## IMPORTANT: 
## Is recommended to update the BTC trade history database, by running the 
## "build_hist_btc_options_trades_5min.py" script before to execute this script.

import math # bisect
import numpy as np # dual annealing
from pymongo import MongoClient, DESCENDING, ASCENDING
from scipy.optimize import fsolve # fsolve
from scipy.optimize import brentq # brentq
from scipy.optimize import bisect # bisect
from scipy.optimize import dual_annealing # dual annealing


##
## Support functions
##

def calculate_z_fsolve(x, y, z_initial=1.0, tolerance=1e-10):
    """
    FSOLVE APPROACH

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


def calculate_z_brentq(x, y):
    """
    BRENTQ APPROACH

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


def calculate_z_bisect(x, y, z_lower=1, z_upper=1000, tolerance=1e-10):
    """
    BISECT APPROACH

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


def calculate_z_d_annealing(x, y):
    """
    DUAL ANNEALING APPROACH

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
    z = calculate_z_d_annealing(x, y)
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
        """
        The object function to find the 'z' value.
        """
        try:
            return (((1 + x**z)**(1/z)) - 1 - y) ** 2
        except:
            print(f"Overflow encountered with x={x}, z={z}")
            return np.inf

    # Uses 'dual_annealing' method.
    try:
        bounds = [(1, 1000)]
        result = dual_annealing(func, bounds, args=(x, y))
        return result.x[0] if result.success else None
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
collection = db['btc_trade_history_5min']

# An initial guess for z_fsolve. Note: z is in 1 <= z < infinite interval.
z_init_fsolve = 1.0 

# Gets all documents that don't have a 'z_fsolve' field.
#z_filter = {"$or": [{"z_": {"$exists": False}}, {"z": None}]}
z_filter = {"z_fsolve": {"$exists": False}}
trades = collection.find(z_filter)

if trades:

    # Computes and stores 'z' to all approaches to each historical trade.
    for trade in trades:

        # Extracts the strike price.
        strike = float(extract_strike(trade['instrument_name']))
        
        # Gets x and y.
        x = (trade['index_price'] / strike)
        y = ((trade['price'] * trade['index_price']) / strike) # Note: In USD.

        # FSOLVE - Calculates z.
        z_fsolve = calculate_z_fsolve(x, y, z_init_fsolve)
        # Tries to calculate 'z_fsolve' with 'z_init_fsolve = 1.0'.
        if (not z_fsolve) and (z_init_fsolve != 1.0):
            z_init_fsolve = 1.0
            z_fsolve = calculate_z_fsolve(x, y, z_init_fsolve)
        fz_fsolve = funz(z_fsolve, x, y)

        # BRENTQ - Calculates z.
        z_brentq = calculate_z_brentq(x, y)
        fz_brentq = funz(z_brentq, x, y)

        # BISECT - Calculates z.
        z_bisect = calculate_z_bisect(x, y)
        fz_bisect = funz(z_bisect, x, y)

        # DUAL ANNEALING - Calculates z.
        z_d_annealing = calculate_z_d_annealing(x, y)
        fz_d_annealing = funz(z_d_annealing, x, y)

        new_fields = {
                      "strike": strike,
                      "x": x,
                      "y": y,
                      "z_fsolve": z_fsolve,
                      "fz_fsolve": fz_fsolve,
                      "z_brentq": z_brentq,
                      "fz_brentq": fz_brentq,
                      "z_bisect": z_bisect,
                      "fz_bisect": fz_bisect,
                      "z_d_annealing": z_d_annealing,
                      "fz_d_annealing": fz_d_annealing,
                     }

        # Updates the source collection.
        collection.update_one({"_id": trade["_id"]}, {"$set": new_fields})

        # Shows the job execution on terminal.
        print(f"id: {str(trade['id'])} | z_da: {str(z_d_annealing)} | f_da: {str(fz_d_annealing)}")

        # Sets a new value to 'z_init_fsolve'.
        if z_fsolve:
            z_init_fsolve = z_fsolve

    print("The job is done!")

else:

    print("There is nothing to do...")














