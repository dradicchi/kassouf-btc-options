from pymongo import MongoClient
from datetime import datetime, timedelta

# Função para arredondar o datetime para o múltiplo de 5 minutos imediatamente inferior
def round_down_to_5_minutes(dt):
    return dt - timedelta(minutes=dt.minute % 5, seconds=dt.second, microseconds=dt.microsecond)

# Configurar conexão com o MongoDB
client = MongoClient("mongodb://localhost:27017/")  # Substitua pelo URI do seu MongoDB
db = client["deribit_btc_options"]  # Substitua pelo nome do seu banco de dados
collection = db["btc_trade_history_5min"]  # Substitua pelo nome da sua collection

# Atualizar documentos
for document in collection.find({}, {"_id": 1, "date_time": 1}):
    if "date_time" in document:
        original_date = document["date_time"]
        if isinstance(original_date, datetime):
            rounded_date = round_down_to_5_minutes(original_date)

            # Atualizar ou criar o campo "dt_control"
            print(f"orig: {original_date} | round: {rounded_date}")
            collection.update_one(
                {"_id": document["_id"]},
                {"$set": {"dt_control": rounded_date}}
            )
        else:
            print(f"O campo 'date_time' no documento {document['_id']} não é um datetime válido.")
    else:
        print(f"O campo 'date_time' está ausente no documento {document['_id']}.")

print("Atualização concluída.")
