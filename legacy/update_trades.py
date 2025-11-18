from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor

# Conecte ao MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client.deribit_btc_options
primeira_collection = db.btc_trade_history_5min
segunda_collection = db.fsolve_btc_trade_history_5min

# Função para atualizar um único documento
def update_document(doc):
    print(f"id: {doc["id"]}")
    primeira_collection.update_one(
        { "id": doc["id"] },
        { "$set": { "inv_t": doc["inv_t"], "z_fsolve": doc["z"], "fz_fsolve": doc["fz"], "option_type": doc["option_type"], "settlement_period": doc["settlement_period"] } }
    )

# Obtenha os documentos da segunda collection
documentos = segunda_collection.find({}, { "id": 1, "inv_t": 1, "z": 1, "fz": 1, "option_type": 1, "settlement_period": 1, })

# Use ThreadPoolExecutor para realizar a atualização em paralelo
with ThreadPoolExecutor(max_workers=8) as executor:
    executor.map(update_document, documentos)

client.close()