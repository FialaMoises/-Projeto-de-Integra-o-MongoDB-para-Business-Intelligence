from bson import ObjectId
import json
from datetime import datetime
from mongo_connection import get_mongo_client
from pymongo.errors import ConnectionFailure


def convert_object_ids(data):
    def convert_datetime(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, ObjectId):
            return str(obj)
        return obj

    return json.loads(json.dumps(data, default=convert_datetime))


def get_data_by_collection(collection_name, projection=None):
    client = get_mongo_client()
    if client:
        try:
            database_names = client.list_database_names()
            data = []

            for db_name in database_names:
                db = client[db_name]
                if collection_name in db.list_collection_names():
                    collection = db[collection_name]
                    if collection.count_documents({}) > 0:
                        # Use a projeção na consulta find se fornecida
                        cursor = collection.find({}, projection)
                        # Use o cursor para iterar sobre os documentos
                        for document in cursor:
                            # Converta ObjectId e datetime para string para cada item no documento
                            document = convert_object_ids(document)
                            # Converta ObjectId para string para toda a coleção
                            document['_id'] = str(document['_id'])
                            # Adicione o ID do banco de dados
                            document['ID'] = db_name
                            data.append(document)

            return data

        except ConnectionFailure as e:
            print(f"Erro ao consultar {collection_name}: {str(e)}")
            return None
        finally:
            client.close()

    return None


def get_data_by_collection_name(collection_name, projection=None):
    return get_data_by_collection(collection_name, projection)


def get_1_data():
    projection = {'ID': 1, '_id': 1, '3_id': 1,
                  'creation_time': 1, 'deleted': 1, 'propose_list': 1,
                  'responsible_user_id': 1, 'won_date': 1}
    return convert_object_ids(get_data_by_collection_name("1", projection))




def get_2_data():
    projection = {'ID': 1, '3_id': 1, '_id': 1,
                  'creation_time': 1, 'deleted': 1, 'total_price': 1}
    return convert_object_ids(get_data_by_collection_name("2", projection))



def get_3_data():
    projection = {'ID': 1, '_id': 1, '3_name': 1, 'activated': 1,
                  'deleted': 1, 'country_id': 1, '3_mail': 1}


