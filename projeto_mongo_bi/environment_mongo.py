from os import environ


VARIABLES = {
    'MONGODB_ENVIRONMENT_CONNECTION_STRING':
    environ.get("123"),
    'X_API_KEY': environ.get('X_API_KEY', "123")
}
