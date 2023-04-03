#!/bin/bash
from pyspark.sql import SparkSession

# Configuración de SparkSession
spark = SparkSession.builder \
    .appName("mi_app") \
    .config("spark.executor.memory", "1g") \
    .config("spark.cores.max", "2") \
    .getOrCreate()

# Carga de datos desde archivo CSV
df1 = spark.read.options(header='True', inferSchema='True').csv("gs://bucket-escritura003/bronze/resultado1")
df2 = spark.read.options(header='True', inferSchema='True').csv("gs://bucket-escritura003/bronze/resultado2")
df3 = spark.read.options(header='True', inferSchema='True').csv("gs://bucket-escritura003/bronze/resultado3")
df4 = spark.read.options(header='True', inferSchema='True').csv("gs://bucket-csv1-003/productos.csv")

# Limpieza y carga
df1.write.mode("overwrite").csv('gs://bucket-escritura003/silver/resultado1')
df2.write.mode("overwrite").csv('gs://bucket-escritura003/silver/resultado2')
df3 = df3.drop('_c0')
df3.write.mode("overwrite").csv('gs://bucket-escritura003/silver/resultado3')

# Generar modelo estrella
df1.createOrReplaceTempView("TrResp")
df4.createOrReplaceTempView("Prod")

final = spark.sql("""
  SELECT TrResp.sub_ID, Prod.shopid, Prod.itemid
  FROM TrResp
  JOIN Prod ON TrResp.shopid = Prod.shopid
  GROUP BY TrResp.sub_ID, TrResp.shopid, Prod.itemid
""")

final.write.format("csv").option(header='True').mode("overwrite").save("gs://bucket-escritura003/gold/estrella")

# Cerrar SparkSession
spark.stop()
