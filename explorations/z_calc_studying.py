import pymongo
import numpy as np
import matplotlib.pyplot as plt
from pymongo import MongoClient

# Conectar ao MongoDB
client = MongoClient("mongodb://localhost:27017")
db = client["deribit_btc_options"]

# Acessar a collection "btc_trade_history_5min"
collection = db["btc_trade_history_5min"]

# Extrair os valores de fz_d_annealing e fz_fsolve
fz_d_annealing = []
fz_fsolve = []

for doc in collection.find():
    if 'fz_d_annealing' in doc and 'fz_fsolve' in doc:
        # Garantir que os valores sejam convertidos para float
        fz_d_annealing.append(float(doc['fz_d_annealing']) if doc['fz_d_annealing'] is not None else np.nan)
        fz_fsolve.append(float(doc['fz_fsolve']) if doc['fz_fsolve'] is not None else np.nan)

# Convertendo para arrays numpy para facilitar a manipulação
fz_d_annealing = np.array(fz_d_annealing)
fz_fsolve = np.array(fz_fsolve)

# Filtrando apenas os valores não nulos ou inválidos (NaN)
fz_d_annealing = fz_d_annealing[~np.isnan(fz_d_annealing)]
fz_fsolve = fz_fsolve[~np.isnan(fz_fsolve)]

# Plotando as distribuições de fz_d_annealing e fz_fsolve
plt.figure(figsize=(12, 6))

# Histograma para fz_d_annealing
plt.subplot(1, 2, 1)
plt.hist(fz_d_annealing, bins=30, color='blue', alpha=0.7, label="fz_d_annealing")
plt.title('Distribuição de fz_d_annealing')
plt.xlabel('fz_d_annealing')
plt.ylabel('Frequência')

# Histograma para fz_fsolve
plt.subplot(1, 2, 2)
plt.hist(fz_fsolve, bins=30, color='green', alpha=0.7, label="fz_fsolve")
plt.title('Distribuição de fz_fsolve')
plt.xlabel('fz_fsolve')
plt.ylabel('Frequência')

# Exibindo os gráficos
plt.tight_layout()
plt.show()

# Calculando estatísticas principais
mean_fz_d_annealing = np.mean(fz_d_annealing)
std_fz_d_annealing = np.std(fz_d_annealing)

mean_fz_fsolve = np.mean(fz_fsolve)
std_fz_fsolve = np.std(fz_fsolve)

print(f"fz_d_annealing - Média: {mean_fz_d_annealing:.4f}, Desvio Padrão: {std_fz_d_annealing:.4f}")
print(f"fz_fsolve - Média: {mean_fz_fsolve:.4f}, Desvio Padrão: {std_fz_fsolve:.4f}")