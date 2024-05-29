from pyspark.sql.functions import col, min, max, sum
from builtins import min as python_min
from builtins import max as python_max
from parameter import *

# Variables globales para las estadísticas
TOTAL_COUNT = 0
TOTAL_SUM = 0.0
AVG_PRICE = 0.0
MIN_PRICE = float('inf')
MAX_PRICE = float('-inf')

# Procesar cada archivo batch
def batch_process(spark, file_path, cursor):
    global TOTAL_COUNT, TOTAL_SUM, MIN_PRICE, MAX_PRICE, AVG_PRICE
    df_batch = (spark.read.option(HEADER, TRUE).csv(file_path)
                .withColumn(PRICE, col(PRICE).cast(DOUBLE)))

    #Calculo de variables se seguimiento por cada batch
    count_batch = df_batch.count()
    min_batch = df_batch.select(min(PRICE)).collect()[0][0]
    max_batch = df_batch.select(max(PRICE)).collect()[0][0]
    sum_batch = df_batch.select(sum(PRICE)).collect()[0][0]

    #Inserciones a la base de datos de los datos batch
    rows = df_batch.collect()
    for row in rows:
        cursor.execute(INSERT_DATA,
                       (row[ROW_TIMESTAMP], row[ROW_PRICE], row[ROW_USER_ID]))

    #Actualización de las varibales
    TOTAL_COUNT += count_batch
    TOTAL_SUM += sum_batch
    MIN_PRICE = python_min(MIN_PRICE, min_batch)
    MAX_PRICE = python_max(MAX_PRICE, max_batch)
    AVG_PRICE = TOTAL_SUM / TOTAL_COUNT
    print(f"PROCESAMIENTO BATCH {file_path}: Count = {TOTAL_COUNT}, Avg = {AVG_PRICE}, Min = {MIN_PRICE}, Max = {MAX_PRICE}")


def get_statistics(cursor):
    # Ejecutar la consulta
    cursor.execute(DATA_QUERY)

    # Obtener los resultados
    result = cursor.fetchone()

    # Asignar los resultados a variables
    total_count = result[0]
    avg_price = result[1]/result[0]
    min_price = result[2]
    max_price = result[3]

    print(f"ESTADISTICAS DE PROCESAMIENTO EN BASE DE DATOS: Count = {total_count}, Avg = {avg_price}, Min = {min_price}, Max = {max_price}")