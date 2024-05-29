#Parámetros
APP_NAME = "Data Pipeline"
SPARK_MASTER = "spark.master"
LOCAL = "local"
PRICE = "price"
ROW_PRICE = 'price'
ROW_TIMESTAMP = 'timestamp'
ROW_USER_ID = 'user_id'
USER_ID = "user_id"
TIMESTAMP = "timestamp"
HEADER = "header"
TRUE = "true"
DOUBLE = "double"
IMPUTS = "./data/"
MIN = "min"
MAX = "max"
FILES = [f"{IMPUTS}2012-{i}.csv" for i in range(1, 6)]
VALIDATION_FILE = f"{IMPUTS}validation.csv"
VALIDATION_DATABASE = "SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s"
INSERT_DATA = "INSERT INTO transactions (timestamp, price, user_id) VALUES (%s, %s, %s)"
POSTGRES = 'postgres'
USER = 'user'
PASSWORD = 'password'
HOST = 'host'
PORT = 'port'
DBNAME = 'dbname'
CREATE_DATABASE = "CREATE DATABASE {}"
PRINT_DATABASE_CREATE = "Base de datos creada "
PRINT_ERROR_DATABASE = "Error al verificar/crear la base de datos: "
CREATE_TABLE = '''
        CREATE TABLE IF NOT EXISTS transactions (
            timestamp TEXT,
            price REAL,
            user_id INTEGER
        )
        '''
PRINT_ERROR_CONECTION = "Error al conectar a la base de datos: "
SELECT_DATABASE = "SELECT * FROM transactions"
DATA_QUERY = """
        SELECT COUNT(*), SUM(price), MIN(price), MAX(price)
        FROM transactions
    """

