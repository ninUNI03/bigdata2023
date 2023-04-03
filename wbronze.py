#!/bin/bash
from pyspark.sql import SparkSession

# Configuración de SparkSession
spark = SparkSession.builder \
    .appName("mi_app") \
    .config("spark.executor.memory", "1g") \
    .config("spark.cores.max", "2") \
    .getOrCreate()
# Carga de datos desde archivo CSV
df1 = spark.read.options(header='False', inferSchema='True').csv("gs://bucket-csv1-003/TiendaResp3.csv")
df2 = spark.read.options(header='False', inferSchema='True').csv("gs://bucket-csv1-003/comportamiento.csv")
df3 = spark.read.options(header='False', inferSchema='True').csv("gs://bucket-csv1-003/productos.csv")
#df2 = spark.read \
#    .option("header", "true") \
#    .option("inferSchema", "true") \
#    .csv("gs://bucket-csv1-003/comportamiento.csv")

#df3 = spark.read \
#    .option("header", "true") \
#    .option("inferSchema", "true") \
#    .csv("gs://bucket-csv1-003/productos.csv")
# Escritura de datos procesados a un archivo CSV
df1.write.mode("overwrite").csv('gs://bucket-escritura003/bronze/resultado1')
df2.write.mode("overwrite").csv('gs://bucket-escritura003/bronze/resultado2')
df3.write.mode("overwrite").csv('gs://bucket-escritura003/bronze/resultado3')
# Cerrar SparkSession
spark.stop()

