# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import io
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

#Variaveis declaradas para o storage e a key
storage_account_name = "stgmannaz"
storage_account_access_key = "biHOdhE3NSG3nCdoaijoDhBDDT4uSA0l+dNKflCAHOKy6C9WlpFaCo/uK2rRIFHM0eg1qdDBZ3PA+AStl9Canw=="

# COMMAND ----------

#variaveis para ler o caminho do arquivo e o tipo
file_location = "wasbs://poc-databricks-bronze@stgmannaz.blob.core.windows.net/rawdata.csv"
file_type = "csv"
file_silver = "wasbs://poc-databricks-silver@stgmannaz.blob.core.windows.net/rawdata.parquet"

# COMMAND ----------

#configurando o spark com as credenciais do storage
spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

# COMMAND ----------

#carregando o arquivo dentro do blob, utilizando o método spark.read.format passando o formato declarado acima como parâmetro, e utilizando o método load par carregar o arquivo, utilizando de parâmetro o caminho dele
df = spark.read.format(file_type).option("inferSchema", "true").option("header","true").load(file_location)


# COMMAND ----------

#display do dataframe acima
display(df)

# COMMAND ----------

df.createOrReplaceTempView("WC")
df_WC_sql = spark.sql("""
SELECT
  BUKRS,
  SUBSTRING(BUKRS, 2, 3) BUKRS_sub,
  LEFT (BUKRS,2) BUKRS_left,
  RIGHT(BUKRS,2) BUKRS_right,
  *
FROM WC
""")
display (df_WC_sql)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   BUKRS,
# MAGIC   SUBSTRING(BUKRS, 2, 3),
# MAGIC   LEFT (BUKRS,2),
# MAGIC   RIGHT(BUKRS,2)
# MAGIC FROM WC

# COMMAND ----------

df_WC_pyspark = df

df_WC_pyspark = df_WC_pyspark.withColumn(
"BUKRS_sub", substring("BUKRS",1,2)
).withColumn(
    "BUKRS_right",expr("RIGHT(BUKRS,2)")
).withColumn(
    "BUKRS_left",expr("LEFT(BUKRS,2)"))
display(df_WC_pyspark)

# COMMAND ----------

df_WC_pyspark.write.parquet("wasbs://poc-databricks-silver@stgmannaz.blob.core.windows.net/df_WC_pyspark.parquet")

