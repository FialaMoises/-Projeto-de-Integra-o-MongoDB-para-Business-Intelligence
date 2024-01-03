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


def get_propose_data():
    projection = {'ID': 1, '_id': 1, 'public_identification': 1,
                  'is_deleted': 1, 'is_active': 1, 'calc_id': 1}
    return convert_object_ids(get_data_by_collection_name("Propose", projection))


def get_2_data():
    projection = {'ID': 1, '3_id': 1, '_id': 1,
                  'creation_time': 1, 'deleted': 1, 'total_price': 1}
    return convert_object_ids(get_data_by_collection_name("2", projection))


def get_financial_transaction_data():
    projection = {'ID': 1, '_id': 1, 'amount': 1, 'date': 1}
    return convert_object_ids(get_data_by_collection_name("FinancialTransaction", projection))


def get_breakeven_point_data():
    projection = {'ID': 1, '_id': 1, 'month_number': 1,
                  'year_number': 1, 'breakeven_percentage_value': 1, 'fixed_cost_value': 1}
    return convert_object_ids(get_data_by_collection_name("BreakevenPoint", projection))


def get_financial_account_data():
    projection = {'ID': 1, '_id': 1, 'description': 1, 'role': 1,
                  'active': 1, 'cash_account': 1, 'creation_time': 1}
    return convert_object_ids(get_data_by_collection_name("FinancialAccount", projection))


def get_3_data():
    projection = {'ID': 1, '_id': 1, '3_name': 1, 'activated': 1,
                  'deleted': 1, 'country_id': 1, '3_mail': 1}
    return convert_object_ids(get_data_by_collection_name("3", projection))


def get_holdprint_account_data():
    projection = {'ID': 1, '_id': 1, '3_name': 1,
                  'country': 1, 'activated': 1}
    return convert_object_ids(get_data_by_collection_name("HoldprintAccount", projection))


def get_holdprint_user_data():
    projection = {'ID': 1, '_id': 1, 'activated': 1, 'user_profile_id': 1, 'role': 1,
                  'first_name': 1, 'last_name': 1,
                  'activated': 1, 'last_login_time': 1, 'email': 1}
    return convert_object_ids(get_data_by_collection_name("HoldprintUsers", projection))


def get_costmemento_data():
    projection = {'ID': 1, '_id': 1, 'name': 1, 'public_identification': 1, 'total_price': 1,
                  'creation_time': 1,
                  'lock': 1, 'total_profit_percentual': 1}
    return convert_object_ids(get_data_by_collection_name("CostMemento", projection))


def get_costcentergroup_data():
    projection = {'ID': 1, 'lock': 1, 'cost_centers': 1, 'creation_date': 1}
    return convert_object_ids(get_data_by_collection_name("CostCenterGroup", projection))
