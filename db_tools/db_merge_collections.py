###
### Merges pre-defined data from two related MongoDB collections to a new collection.
###

from pymongo import MongoClient, InsertOne
from itertools import islice

# Função para dividir a lista de IDs em lotes
def batch(iterable, size):
    iterator = iter(iterable)
    while True:
        batch = list(islice(iterator, size))
        if not batch:
            break
        yield batch

# Conecta ao MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["deribit_btc_options"]
collection1 = db["NEW_NEW_NEW_btc_trade_history_5min"]  # Collection de origem
collection2 = db["d_annealing_btc_trade_history_5min"]
new_collection = db["final_btc_trade_history_5min"]

# Controls where it should start.
unix_time = 0

# Define parâmetros para operações de "bulk write"
oper = []
batch_size = 10000  # Tamanho do lote de operações de escrita
id_batch_size = 10000  # Tamanho do lote para consultas $in

# Inicia uma sessão explícita
with client.start_session() as session:
    # Carregar todos os IDs da collection1
    ids = [doc["id"] for doc in collection1.find({"unix_time": {"$gt": unix_time}}, {"id": 1})]
    
    # Carregar documentos relacionados de collection2 em lotes
    docs_collection2 = {}
    for id_batch in batch(ids, id_batch_size):
        for doc in collection2.find({"id": {"$in": id_batch}}, session=session, projection={
                "id": 1, "z": 1, "fz": 1}):
            docs_collection2[doc["id"]] = doc

    # Cursor com no_cursor_timeout para evitar timeout em operações longas
    cursor = collection1.find({"unix_time": {"$gt": unix_time}}, no_cursor_timeout=True, session=session).sort("unix_time", 1)

    try:
        for doc_collection1 in cursor:
            doc_id = doc_collection1["id"]
            doc_collection2 = docs_collection2.get(doc_id)  # Obter documento pré-carregado

            if doc_collection2:
                # Mesclar dados
                new_data = {
                    **doc_collection1,  # Descompacta todos os dados do documento da collection1
                    "z_d_annealing": doc_collection2["z"],
                    "fz_d_annealing": doc_collection2["fz"],                    
                }

                # Adicionar à operação de inserção em lote
                oper.append(InsertOne(new_data))
                
                # Executar o bulk_write quando o tamanho do lote for atingido
                if len(oper) >= batch_size:
                    new_collection.bulk_write(oper, session=session)
                    oper = []

        # Inserir as operações restantes
        if oper:
            new_collection.bulk_write(oper, session=session)

    finally:
        # Garantir que o cursor seja fechado
        cursor.close()