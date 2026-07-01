# connection paramters
import os

connection_params_db = {
    "host": os.environ.get('DB_HOST'), 
    "port": os.environ.get('DB_PORT'),
    "dbname": os.environ.get('DB_DATABASE'),
    "user": os.environ.get('DB_USER'),
    "password": os.environ.get('DB_PASSWORD'),
}

connection_params_redis_cache = {
    "host": os.environ.get('RDC_HOST'),
    "port": os.environ.get('RDC_PORT'),
    "db": 0,
    "decode_responses": True,
}

connection_params_rabbitmq = {
    "host": os.environ.get('RBMQ_HOST'),
    "port": int(os.environ.get('RBMQ_PORT')),
}

# testing purposes
# connection_params_rabbitmq = {
#     "host": "localhost",
#     "port": 5672,
# }