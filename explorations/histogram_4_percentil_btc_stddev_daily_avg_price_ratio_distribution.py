import numpy as np
import pymongo
import matplotlib.pyplot as plt

# Conexão ao MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client['deribit_btc_options']
collection = db['btc_avg_index_price_daily']

# Buscar todos os documentos da collection
documents = collection.find()

# Lista para armazenar as razões
ratios = []

# Calcular a razão entre o maior e o menor valor diário
for doc in documents:
    std_dev = doc['std_dev_index_price_daily']
    avg_price = doc['avg_index_price_daily']
    ratio = std_dev / avg_price
    ratios.append(ratio)

# Converter a lista para um array numpy
ratios = np.array(ratios)

# Calcular o percentil de 96%
percentile_96 = np.percentile(ratios, 96)

# Separar os dados em dois conjuntos
conjunto_96 = ratios[ratios <= percentile_96]
conjunto_4 = ratios[ratios > percentile_96]

# Exibir o valor máximo para estar dentro dos 4% menos frequentes
valor_max_4_percent = min(conjunto_4)

print(f"O valor máximo de razão que está dentro dos 4% menos frequentes é: {valor_max_4_percent}")

# Criar o histograma para visualização
plt.hist(ratios, bins=20, edgecolor='black')
plt.axvline(percentile_96, color='r', linestyle='dashed', linewidth=2, label=f'Percentil 96% = {percentile_96:.2f}')
plt.title('Distribuição das Razões')
plt.xlabel('Razão')
plt.ylabel('Frequência')
plt.legend()
plt.show()