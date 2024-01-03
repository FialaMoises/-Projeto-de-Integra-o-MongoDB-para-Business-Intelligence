from flask import Flask, jsonify, request, make_response
from pymongo.errors import ConnectionFailure
from environment_mongo import VARIABLES
from queries import (
    get_1_data,
    get_2_data,
    get_3_data,

)

app = Flask(__name__)


def apply_projection(data, projection):
    if projection:
        # Aplica a projeção aos dados
        return [{key: item[key] for key in projection if key in item} for item in data]

    # Retorna os dados sem alterações se não houver projeção
    return data


def get_data_safely(get_data_function, projection_key):
    try:
        authorization_param = request.headers.get('X-API-KEY')
        expected_authorization = VARIABLES['X_API_KEY']

        if authorization_param != expected_authorization:
            return make_response(jsonify({"error": "Parâmetro de autorização inválido"}), 401)

        data = get_data_function()
        print("Data retrieved successfully")

        # Obtém a projeção específica para esta coleção
        projection = request.args.get(projection_key)

        # Aplica a projeção aos dados
        data = apply_projection(data, projection)

        if data:
            return jsonify(data), 200
        else:
            return make_response(jsonify({"error": "Nenhum dado encontrado"}), 404)

    except ConnectionFailure as e:
        print("Connection error:", str(e))
        return jsonify({"error": f"Erro de conexão com o MongoDB: {str(e)}"}), 500

    except Exception as e:
        print("Unexpected error:", str(e))
        return jsonify({"error": f"Erro inesperado: {str(e)}"}), 500

# Funções de projeção específicas para cada coleção


def project_1(data):
    projection = ['ID', '_id', '3_id',
                  'creation_time', 'deleted', 'propose_list',
                  'responsible_user_id', 'won_date']
    return apply_projection(data, projection)


def project_2(data):
    projection = ['ID', '3_id', '_id',
                  'creation_time', 'deleted', 'total_price']
    return apply_projection(data, projection)


def project_3(data):
    projection = ['ID', '_id', '3_name', 'activated',
                  'deleted', 'country_id', '3_mail']
    return apply_projection(data, projection)


# Rotas


@app.route('/get-1-data')
def get_1():
    print('Received request from ' + request.remote_addr)
    return get_data_safely(lambda: project_1(get_1_data()), '1_projection')


@app.route('/get-2-data')
def get_2():
    print('Received request from ' + request.remote_addr)
    return get_data_safely(lambda: project_2(get_2_data()), '2_projection')


@app.route('/get-3-data')
def get_3():
    print('Received request from ' + request.remote_addr)
    return get_data_safely(lambda: project_3(get_3_data()), '3_projection')


if __name__ == '__main__':
    app.run(debug=True)
