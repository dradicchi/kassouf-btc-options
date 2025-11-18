import numpy as np
import statsmodels.api as sm
from pymongo import MongoClient

# Conexão com o MongoDB
client = MongoClient("mongodb://localhost:27017")
db = client["deribit_btc_options"]
collection = db["btc_trade_history_5min"]

# Obtém a contagem de trades por instrumento
trade_counts = collection.aggregate([
    {"$match": {
        "settlement_period": "month",
        "option_type": "call",
        "z_d_annealing": {"$gte": 1, "$lt": float("inf")},
        "fz_d_annealing": {"$gte": -0.001, "$lte": 0.001},
        #"unix_time": {"$gte": 1672444800000}  # 1º de janeiro de 2023 em milissegundos
    }},
    {"$group": {
        "_id": "$instrument_name",
        "trade_count": {"$sum": 1}
    }},
    {"$match": {"trade_count": {"$gte": 2000}}}
])

# Extrai os nomes dos instrumentos que atendem ao critério
valid_instruments = {doc["_id"] for doc in trade_counts}

# Filtra os documentos para incluir apenas trades válidos e instrumentos com pelo menos 80 trades
query = {
    "settlement_period": "month",
    "option_type": "call",
    "z_d_annealing": {"$gte": 1, "$lt": float("inf")},
    "fz_d_annealing": {"$gte": -0.001, "$lte": 0.001},
    #"unix_time": {"$gte": 1672444800000},  # 1º de janeiro de 2023 em milissegundos
    "instrument_name": {"$in": list(valid_instruments)},
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
    "e2_90d": 1, # V
    "e1_90d": 1, # F
    "z_d_annealing": 1  # z calculado com dual annealing
}

# Obtém os dados do MongoDB
data = list(collection.find(query, projection))

# Verifica se há dados suficientes para a regressão
if len(data) < 2:
    print("Dados insuficientes para realizar a regressão.")
else:
    # Prepara os dados para a regressão
    X = []  # Variáveis explanatórias
    y = []  # Resposta (z)

    for doc in data:
        if all(key in doc for key in ["inv_t", "x", "e2_90d", "e1_90d", "z_d_annealing"]):
            X.append([doc["inv_t"], doc["x"], doc["e2_90d"], doc["e1_90d"]])
            y.append(doc["z_d_annealing"])
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
        {"model": "daily_calls_month_trades"},
        {"$set": {
            "model": "daily_calls_month_trades",
            "params": {
                "k0": params[0],
                "k1": params[1],
                "k2": params[2],
                "k3": params[3],
                "k4": params[4]
            },
            "r_squared": r_squared,
            "num_observations": len(data)
        }},
        upsert=True
    )

    print("Parâmetros salvos no MongoDB.")