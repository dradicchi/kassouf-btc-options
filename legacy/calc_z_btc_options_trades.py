###
### Calculates z for each historical trade.
###

## IMPORTANT: 
## Is recommended to update the BTC trade history database, by running the 
## "build_hist_btc_options_trades_5min.py" script before to execute this script.

from pymongo import MongoClient, DESCENDING, ASCENDING
from scipy.optimize import fsolve
from scipy.optimize import brentq
from scipy.optimize import bisect
from scipy.optimize import dual_annealing

##
## Support functions
##

# Função para encontrar a raiz utilizando o método da bissecção
def calculate_z_bis(x, y, z_lower=1, z_upper=1000, tolerance=1e-10):
    def func_bis(z, x, y):
        return ((1 + x**z)**(1/z)) - 1 - y
    # Testa se há mudança de sinal entre z_lower e z_upper
    if func_bis(z_lower, x, y) * func_bis(z_upper, x, y) > 0:
        #print(f"Nenhuma mudança de sinal encontrada no intervalo inicial [{z_lower}, {z_upper}]. Expandindo o intervalo...")
        # Expande o intervalo até encontrar uma mudança de sinal
        while func_bis(z_lower, x, y) * func_bis(z_upper, x, y) > 0 and z_lower > 1:
            z_lower -= 10  # Decrementa em passos até encontrar a mudança de sinal
            if z_lower <= 1:
                z_lower = 1
                break
        if func_bis(z_lower, x, y) * func_bis(z_upper, x, y) > 0:
            #print("Não foi possível encontrar um intervalo com mudança de sinal.")
            return None
    # Aplica o método da bissecção no intervalo com mudança de sinal
    root = bisect(func_bis, z_lower, z_upper, args=(x, y), xtol=tolerance)
    return root


def calculate_z_sim(x, y):
    def func_sim(z):
        return abs(((1 + x**z)**(1/z)) - 1 - y)

    try:
        bounds = [(1, 1000)]
        result = dual_annealing(func_sim, bounds,)
        return result.x[0] if result.success else None
    except:
        return None



def calculate_z(x, y, tolerance=1e-10):
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
    z = calculate_z(x, y)
    """

    # An initial guess for z.
    z_initial = max(1, min(10, y/x))

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

    # Combines Brent's method (better with non-linear functions) with 'fsolve', 
    # using the generalized Newton-Raphson method with the Levenberg-Marquardt 
    # algorithm (good for smooth functions). It is more flexible and works well 
    # for finding roots of nonlinear functions even if the derivative is not 
    # available.
    try:
        # First, tries to solve with Brent's method.
        return brentq(func, 1, 1000, args=(x, y))

    except:

        # Fall back to fsolve if brentq fails.
        root, info, ier, msg = fsolve(func,
                                      z_initial,
                                      args=(x, y), 
                                      xtol=tolerance, 
                                      full_output=True)
    
        # Check if the solution has converged (ier = 1)
        if ier == 1:
            return root[0]
        else:
            root_bis = calculate_z_bis(x, y)
            if root_bis:
                return root_bis
            else:
                calculate_z_sim(x, y)













def funz(z, x, y):
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

# Gets all documents that don't have a 'z' field.
z_filter = {"$or": [{"z": {"$exists": False}}, {"z": None}]}
trades = collection.find()

try:
    # Tests if 'trades' is not null.
    first_trade = next(trades)

    # Computes and stores 'z' to each historical trade.
    for trade in trades:

        # Extracts the strike price.
        strike = float(extract_strike(trade['instrument_name']))
        
        # Calculates 'z'.
        x = (trade['index_price'] / strike)
        y = ((trade['price'] * trade['index_price']) / strike) # Note: In USD.
        z = calculate_z_sim(x, y)
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

except StopIteration:

    print("There is nothing to do...")











