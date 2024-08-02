from pymongo import MongoClient
from . import schemas


def connect_to_mongodb2(uri: str, database_name: str):
    try:
        client = MongoClient(uri)
        database = client[database_name]
        print("Conexão bem-sucedida ao MongoDB!")
        return database
    except Exception as e:
        print(f"Erro ao conectar ao MongoDB: {e}")
        return None


def connect_to_mongodb(uri: str):
    try:
        client = MongoClient(uri)
        print("Conexão bem-sucedida ao MongoDB!")
        return client
    except Exception as e:
        print(f"Erro ao conectar ao MongoDB: {e}")
        return None


def insert_document(database, collection_name: str, document: dict):
    collection = database[collection_name]
    result = collection.insert_one(document)
    return result.inserted_id


def update_documents(database, collection_name: str, query: dict, new_values: dict):
    collection = database[collection_name]
    result = collection.update_many(query, {'$set': new_values})
    return result.modified_count


def find_documents(database, collection_name: str, query: dict):
    collection = database[collection_name]
    documents = collection.find(query)
    return list(documents)


def remove_none_keys(d):
    """
    Using to not create field on a Documento with null value. 

    :param d: Dicionário original
    :return: Dicionário sem chaves com valor None
    """
    if not isinstance(d, dict):
        return d

    cleaned_dict = {}
    for k, v in d.items():
        if isinstance(v, dict):
            nested_dict = remove_none_keys(v)
            if nested_dict:
                cleaned_dict[k] = nested_dict
        elif v is not None:
            cleaned_dict[k] = v

    return cleaned_dict


if __name__ == "__main__":
    uri = "mongodb://localhost:27017"
    database_name = "meu_banco"
    collection_name = "minha_colecao"

    db = connect_to_mongodb(uri, database_name)

    item = schemas.ItemSchema(name="Example Item", description="This is an example.",
                              price=10.99, tags=["example", "item"])
    item_dict = item.model_dump()
    connect_to_mongodb.insert_document(db, "test", item_dict)
