
from pymongo import InsertOne


def copyDocumentsToNewCollection(integration_id, process_id, collection_origin, collection_destiny):
    query = {"partnerId": integration_id,
             "processId": process_id}

    count = 0
    bulk_operations = []
    for document in collection_origin.find(query):
        try:
            document.pop('_id', None)
            bulk_operations.append(
                InsertOne(document)
            )
            count += 1

            if len(bulk_operations) >= 5000:
                collection_destiny.bulk_write(bulk_operations)
                bulk_operations = []

        except Exception as e:
            print(f"Error on document {count}: {e}")

    if bulk_operations:
        collection_destiny.bulk_write(bulk_operations)

    print(f"Copied {count} documents to the new collection.")


if __name__ == "__main__":
    import os
    from src.mongo_integration import mongo_connection
    from dotenv import load_dotenv

    load_dotenv()

    # Database connection THemis
    uriThemis = os.getenv("UPLAN_URI_MONGO")
    db_incoming = mongo_connection.connect_to_mongodb(uriThemis, "Incoming")

    # Database connection Anubis
    uriAnubis = os.getenv("UPLAN_URI_MONGO_ANUBIS")
    db_live = mongo_connection.connect_to_mongodb(uriAnubis, "Items")

    # Collections to search
    collection_incoming_items = db_incoming["Items"]
    collection_live_items = db_live["Items"]

    # Query to filter items
    integration_id = "666788e0eb8f5b0ac6f826cc"
    query = {"partnerId": integration_id}

    copyDocumentsToNewCollection(
        query, collection_incoming_items, collection_live_items)
