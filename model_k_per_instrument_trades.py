###
### Calculates the "kj" parameters to each individual instrument modeling.
###

## IMPORTANT: 
## Before to execute this script is recommended to update the BTC trade history 
## database and the instrument historical offering database, by running 
## respectively:
## (1) "build_hist_btc_options_trades_5min.py"
## (2) "build_hist_btc_inverse_options_offering.py"

import numpy as np
import statsmodels.api as sm
from pymongo import MongoClient, DESCENDING, ASCENDING


##
## Support functions
##

def least_squares(data):
    """
    Applies least squares to calculate the kj parameters for a linear model as:

    y = k0 + k1*x1 + k2*x2 + ... kj*xj
    
    Note: The function needs to receive the experimental data as a Numpy array 
    similar to:

        [[x11, x12, x13, .... x1j, y1],
         [x21, x22, x23, .... x2j, y2],
                     ...
         [xi1, xi2, xi3, .... xij, yi]]

    Where xij are the explanatory variable's values and yi is the respective
    response variable's value for i observations.
    
    Returns the kj's modeled set of parameters and a relative data summary.
    """

    # Separating independent variables (x1, x2, x3) and dependent variable (y).
    X = data[:, :-1]  # All columns except the last
    y = data[:, -1]   # Only the last column

    # Adding a column of 1s to calculate the intercept term.
    # ATENTION: Column added at the zero position.
    X_with_constant = sm.add_constant(X)  # Adds a column of ones for the 
                                          # intercept.

    # Fitting the regression model using OLS.
    model = sm.OLS(y, X_with_constant).fit()

    # Printing the summary of the model.
    #print(model.summary())

    # If you want specific values like R-squared or the standard error of the 
    # regression
    r_squared = model.rsquared
    standard_error = np.sqrt(model.scale)
    #print(f"R-squared: {r_squared}")
    #print(f"Standard Error of the Regression: {standard_error}")

    return model.params, r_squared, standard_error, model.summary()


##
## Main script
##

# DB access.
client = MongoClient('mongodb://localhost:27017/')
db = client['deribit_btc_options']
trades_collection = db['btc_trade_history_5min']
instr_collection = db['btc_inverse_options_offering']


# Gets all new unmodeled instruments.
unmodel_instr = instr_collection.find({"is_active": False,
                                       "k_modeled": {"$exists": False}
                                       #"settlement_period": "day",
                                       #"option_type": "call",
                                       #"creation_unix_timestamp": {
                                       #                 "$gte": 1580732035657},
                                      },)


# Builds the array of data observations and calculates "kj" parameters.
if unmodel_instr:

    for instr in unmodel_instr:

        # Sets a control stamp to modeled instruments.
        new_fields = {
                      "k_modeled": True,
                     }    

        # Gets all trades for the unmodeled instrument.
        trades = list(trades_collection.find(
                                {"instrument_name": instr["instrument_name"],}))

        if trades:

            # Builds the arrays of data observations - Dual Annealing.
            data_da = [
              [
                  trade["inv_t"],
                  trade["e1_72h"] if trade["settlement_period"] == "day" 
                                    else trade["e1_90d"],
                  trade["e2_72h"] if trade["settlement_period"] == "day" 
                                    else trade["e2_90d"],
                  trade["x"],
                  trade["z_d_annealing"]
              ]
              for trade in trades
              if trade["settlement_period"] in ["day", "week", "month"] and
                all(
                    trade.get(key) is not None
                    for key in (["inv_t", "e1_72h", "e2_72h", "x",
                                                             "z_d_annealing"]
                        if trade["settlement_period"] == "day" 
                        else ["inv_t", "e1_90d", "e2_90d", "x", 
                                                            "z_d_annealing"])
                )
            ]

            # Builds the arrays of data observations - Fsolve.
            data_fsolve = [
              [
                  trade["inv_t"],
                  trade["e1_72h"] if trade["settlement_period"] == "day" 
                                    else trade["e1_90d"],
                  trade["e2_72h"] if trade["settlement_period"] == "day" 
                                    else trade["e2_90d"],
                  trade["x"],
                  trade["z_fsolve"]
              ]
              for trade in trades
              if trade["settlement_period"] in ["day", "week", "month"] and
                all(
                    trade.get(key) is not None
                    for key in (["inv_t", "e1_72h", "e2_72h", "x", "z_fsolve"]
                        if trade["settlement_period"] == "day" 
                        else ["inv_t", "e1_90d", "e2_90d", "x", "z_fsolve"])
                )
            ]

            # Counts valid observations.
            obs_da_counter = len(data_da)
            obs_fsolve_counter = len(data_fsolve)

            # Calculates the "kj" parameters with Dual Annealing z.
            if obs_da_counter >= 2:

                print(f"Modeling DA for: {instr["instrument_name"]}")

                data_da_array = np.array(data_da)

                (da_model_params, 
                da_r_squared, 
                da_standard_error, 
                da_model_summary) = least_squares(data_da_array)

                new_fields.update({
                              "da_obs_counter": obs_da_counter ,
                              "da_obs_array": data_da,
                              "da_model_params": da_model_params.tolist(),
                              "da_r_squared": da_r_squared,
                              "da_standard_error": da_standard_error,
                              "da_model_summary": str(da_model_summary),
                             })
            

            ## Calculates the "kj" parameters with FSolve z.
            if obs_fsolve_counter >= 2:

                print(f"Modeling FSOLVE for: {instr["instrument_name"]}")

                data_fsolve_array = np.array(data_fsolve)

                (fsolve_model_params, 
                fsolve_r_squared, 
                fsolve_standard_error, 
                fsolve_model_summary) = least_squares(data_fsolve_array)

                new_fields.update({
                              "fsolve_obs_counter": obs_fsolve_counter,
                              "fsolve_obs_array": data_fsolve,
                              "fsolve_model_params": fsolve_model_params.tolist(),
                              "fsolve_r_squared": fsolve_r_squared,
                              "fsolve_standard_error": fsolve_standard_error,
                              "fsolve_model_summary": str(fsolve_model_summary),
                              })

            # Updates the source collection.
            instr_collection.update_one({"_id": instr["_id"]}, 
                                        {"$set": new_fields})


        else:
            print(f"There aren't trades for: {instr["instrument_name"]}")

            # Updates the source collection.
            instr_collection.update_one({"_id": instr["_id"]}, 
                                        {"$set": new_fields})


else:
    print("There is nothing to do...")

