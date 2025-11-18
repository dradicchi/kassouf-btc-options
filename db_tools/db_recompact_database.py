###
### Recompacts and re-indexes all collections from the Deribit MongoDB database.
###


from pymongo import MongoClient
from pymongo.errors import OperationFailure

##
## Support functions.
##

def compact_and_reindex_all_collections(database_name):
    """
    Compacts and indexes all collections to a given MongoDB database.
    """
    # Connects DB.
    client = MongoClient("mongodb://localhost:27017/")
    db = client[database_name]

    # Lists all collections.
    collections = db.list_collection_names()

    print("\n--------------------------------------------------")
    print(f"Defragmenting and sanitizing: {database_name.upper()}")
    print("--------------------------------------------------\n")

    for collection_name in collections:
        collection = db[collection_name] # Sets the collection.
        
        try:
            # Compacts the collection.
            print(f"Compacting: '{collection_name.upper()}'...")
            db.command({"compact": collection_name})
            print(f"Collection '{collection_name.upper()}' was compacted!")

            # Indexes the collection.
            print(f"Indexing: '{collection_name.upper()}'...")
            db.command("reIndex", collection_name)
            print(f"Collection '{collection_name.upper()}' was indexed.\n")

        except OperationFailure as e:
            print(f"There was an error on processing '{collection_name.upper()}': {e}\n")


##
## Main script.
##

database_name = "deribit_btc_options"
compact_and_reindex_all_collections(database_name)

