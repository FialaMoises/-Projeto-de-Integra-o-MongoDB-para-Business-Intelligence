from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from environment_mongo import VARIABLES


def get_mongo_client():
    try:
        client = MongoClient(
            VARIABLES["MONGODB_ENVIRONMENT_CONNECTION_STRING"])
        return client
    except ConnectionFailure as e:
        print(f"Erro ao conectar ao MongoDB: {str(e)}")
        return None
