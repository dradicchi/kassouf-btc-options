###
### An interactive toll to copy / duplicate collections.
###

from pymongo import MongoClient

##
## Support functions
##

def duplicate_collection(db, source_coll, new_coll):
    """
    Duplicates a collection.
    """
    try:
        # Checks if the new collection name already exists.
        if new_coll in db.list_collection_names():
            print(f"The collection '{new_coll.upper()}' already exists! Choose other name.")
        else:
            # Copies documents from the source collection to new collection.
            pipeline = [{"$match": {}}, {"$out": new_coll}]
            db[source_coll].aggregate(pipeline)
            print(f"Sucess! The collection '{source_coll.upper()}' was copied to '{new_coll.upper()}'!")
    except Exception as e:
        print(f"Error: {e}")


##
## Main script
##

def main():
    """
    Runs a intercative prompt to duplicate collections.
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

        # Asks for a collection to duplicate.
        source_coll = input("\nType a collection name to duplicate it (or 'exit' to quit): ")

        if source_coll.lower() == "exit":
            print("Leaving the script.")
            break
        elif source_coll not in collections:
            print(f"The collection '{source_coll.upper()}' it doesn't exist. Try another name.")
        else:
            # Asks for the new collection name and creates a copy.
            new_coll = input(f"Type a name for the new collection, copied from {source_coll.upper()}: ")
            duplicate_collection(db, source_coll, new_coll)

if __name__ == "__main__":
    main()
