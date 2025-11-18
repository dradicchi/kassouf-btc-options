###
### An interactive toll to unset a field (key) and respective data to a given
### collection.
###

from pymongo import MongoClient

##
## Support functions
##

def unset_key(db, source_coll, key):
    """
    Unsets the key for a given collection.
    """
    try:
        # Unsets data.
        db[source_coll].update_many({}, {"$unset": { key: "",}})
        print(f"Sucess! The key '{key.upper()}' was removed of '{source_coll.upper()}'!")
    except Exception as e:
        print(f"Error: {e}")



##
## Main script
##

def main():
    """
    Runs a intercative prompt to unset fields (keys) and data.
    """
    # Connects with DB.
    client = MongoClient('mongodb://localhost:27017/')
    #db_name = input("Enter the DB name: ")
    db = client['deribit_btc_options']

    while True:

        # Lists the DB collections.
        print("\nListing the DB Collections:")
        collections = db.list_collection_names()
        for i, col in enumerate(collections, 1):
            print(f"{i}. {col}")

        # Asks for a collection to unset data.
        source_coll = input("\nType a collection name to unset data (or 'exit' to quit): ")

        if source_coll.lower() == "exit":
            print("Leaving the script.")
            break
        elif source_coll not in collections:
            print(f"The collection '{source_coll.upper()}' it doesn't exist. Try another name.")
        else:
            # Asks for the key to unset data.
            key = input(f"Type a key for unset data in {source_coll.upper()}: ")
            unset_key(db, source_coll, key)

if __name__ == "__main__":
    main()
