import psycopg2
import parameter
from psycopg2 import sql
from config import DB_CONFIG
from parameter import*


#Validación existe la base de datos
def create_database_if_not_exists():
    conn = None
    cursor = None

    try:
        # Conectarse a PostgreSQL si la base de datos no existe
        conn = psycopg2.connect(
            dbname=POSTGRES,  # Conectarse a la base de datos predeterminada de PostgreSQL
            user=DB_CONFIG[USER],
            password=DB_CONFIG[PASSWORD],
            host=DB_CONFIG[HOST],
            port=DB_CONFIG[PORT]
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Verificar si la base de datos existe
        cursor.execute(VALIDATION_DATABASE, (DB_CONFIG[DBNAME],))
        exists = cursor.fetchone()

        # Crear la base de datos si no existe
        if not exists:
            cursor.execute(sql.SQL(CREATE_DATABASE).format(sql.Identifier(DB_CONFIG[DBNAME])))
            print(PRINT_DATABASE_CREATE)

    except Exception as e:
        print(PRINT_ERROR_DATABASE + e)

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def connect_to_database():
    # Se valida si la base de datos no existe
    create_database_if_not_exists()
    try:
        conn = psycopg2.connect(
            dbname=DB_CONFIG[DBNAME],
            user=DB_CONFIG[USER],
            password=DB_CONFIG[PASSWORD],
            host=DB_CONFIG[HOST],
            port=DB_CONFIG[PORT]
        )
        cursor = conn.cursor()

        # Crear la tabla si no existe
        cursor.execute(CREATE_TABLE)
        conn.commit()

        return conn, cursor
    except Exception as e:
        print(PRINT_ERROR_CONECTION + e)
        return None, None
    conn.commit()

    return conn, cursor