# Pipeline en Pyspark
from pyspark.sql import SparkSession
from data_process import batch_process, get_statistics
from database_connector import connect_to_database
from parameter import *

#Ininicalización de Apache Spark
spark = SparkSession.builder .appName(APP_NAME) \
    .config(SPARK_MASTER, LOCAL).getOrCreate()

#Establecer conexción con la base de datos
conn, cursor = connect_to_database()

#Recorre varios archivos batch de la lista
def pilepile_process(spark, files, cursor):
    for file_path in files:
        batch_process(spark, file_path, cursor)
        conn.commit()

#Agrega el archivo de validation.csv al proceso pipeline
def validation_process(spark, cursor):
    batch_process(spark, VALIDATION_FILE, cursor)
    conn.commit()

#Ejecución del proceso y validación del proceso
pilepile_process(spark, FILES, cursor)
get_statistics(cursor)
validation_process(spark, cursor)
get_statistics(cursor)

#Cierre de Spark y la conexión con la base de datos
cursor.close()
conn.close()
spark.stop()


