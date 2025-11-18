###
### Computes global models for multiple permuted parameter sets
###

import numpy as np
import statsmodels.api as sm
from itertools import product, combinations
from pymongo import MongoClient


# DB connection settings.
client = MongoClient("mongodb://localhost:27017")
db = client["deribit_btc_options"]
trades_collection = db["btc_trade_history_5min"]
instrs_collection = db["btc_inverse_options_offering"]
models_collection = db["btc_inverse_options_global_models"]


# Defines a time interval to consider the observations.
initial_utime = 1672542000000       # 2023-01-01 00:00:00.000
final_utime = 1704078000000         # 2024-01-01 00:00:00.000


###
### Short Term Instruments - Parameters to permute.
###

# Lists of instrument parameters.
settlement_period = ["day"]
option_type = ["call", "put"]

# List of 'f(z) = 0' tolerances to get valid 'z' values.
tol = [1e-1, 1e-3, 1e-5, 1e-7]

# List of approaches to calculating 'z'.
z_calc_models = ["fsolve" "d_annealing"]

# Parameter lists for individual instrument modeling.
min_r_squared = [0.0, 0.67, 0.77, 0.87, 0.97]
min_valid_observs = [2, 10, 40, 160, 640]

## Generates all combination sets to explanatory vars.
exp_vars = ["inv_t", "x", "e1_72h", "e2_72h", "e1_24h", "e1_24h", 
                                           "iv", "strike", "mark_price"]
exp_vars_comb = []
for r in range(1, len(exp_vars) + 1):
    exp_vars_comb.extend(list(combinations(exp_vars, r)))

exp_vars_c = [list(comb) for comb in exp_vars_comb] # Converts tuples to lists.











## Gets instruments with a best potential to modeling.
instrs_query = {
                "settlement_period": "day",
                "option_type": "call",
                "fsolve_r_squared": {"$gte": 0.87},
                "fsolve_obs_counter": {"$gte": 160},
                }

instrs_projection = {
                        "_id": 0,
                        "instrument_name": 1,
                        }

instrs_results = instrs_collection.find(instrs_query, instrs_projection)
potential_instrs = [doc["instrument_name"] for doc in instrs_results]

# Filtra os documentos para incluir apenas trades válidos e instrumentos com pelo menos 80 trades
query = {
    "settlement_period": "day",
    "option_type": "call",
    "z_fsolve": {"$gte": 1},
    "fz_fsolve": {"$gte": -0.001, "$lte": 0.001},
    "unix_time": {"$gte": 1672542000000, "$lt": 1704078000000},
    #"unix_time": {"$gte": 1704078000000},
    "instrument_name": {"$in": list(potential_instrs)},
    # "$expr": {
    #     "$and": [
    #         {"$gte": ["$strike", {"$multiply": ["$index_price", 0.9]}]},
    #         {"$lte": ["$strike", {"$multiply": ["$index_price", 1.1]}]}
    #     ]
    # }
}

# Campos necessários para a regressão
projection = {
    "_id": 0,
    "inv_t": 1,  # 1 / T
    "x": 1,      # X / S
    "iv": 1, # IV
    "e2_72h": 1, # V
    "e1_72h": 1, # F
    "z_fsolve": 1  # z calculado com dual annealing
}

# Obtém os dados do MongoDB
data = list(trades_collection.find(query, projection))

# Verifica se há dados suficientes para a regressão
if len(data) < 2:
    print("Dados insuficientes para realizar a regressão.")
else:
    # Prepara os dados para a regressão
    X = []  # Variáveis explanatórias
    y = []  # Resposta (z)

    for doc in data:
        if all(key in doc for key in ["inv_t", "x", "e1_72h", "iv", "z_fsolve"]):
            X.append([doc["inv_t"], doc["x"], doc["e1_72h"], doc["iv"]])
            y.append(doc["z_fsolve"])
        else:
            print(f"Documento ignorado devido a campos ausentes: {doc}")

    # Converte para arrays do NumPy
    X = np.array(X)
    y = np.array(y)

    # Adiciona uma constante para o termo independente (k0)
    X_with_constant = sm.add_constant(X)

    # Ajusta o modelo de regressão linear
    model = sm.OLS(y, X_with_constant).fit()

    # Exibe os resultados
    print("Resumo do modelo:")
    print(model.summary())

    # Salva os parâmetros ajustados no MongoDB para referência futura
    params = model.params.tolist()
    r_squared = model.rsquared
    collection = db["regression_results"]
    collection.update_one(
        {"model": "daily_calls_kn_strike_near_index_post_dec_2022_min_80_trades"},
        {"$set": {
            "model": "daily_calls_kn_strike_near_index_post_dec_2022_min_80_trades",
            "params": {
                "k0": params[0],
                "k1": params[1],
                "k2": params[2],
                "k3": params[3],
                "k4": params[4],
            },
            "r_squared": r_squared,
            "num_observations": len(data)
        }},
        upsert=True
    )

    print("Parâmetros salvos no MongoDB.")