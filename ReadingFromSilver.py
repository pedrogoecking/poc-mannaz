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
file_location = "wasbs://poc-databricks-silver@stgmannaz.blob.core.windows.net/df_WC_pyspark.parquet/part-00000-tid-4715792454154803576-9ffa4be0-4061-4f9d-8bb8-c035173a6bb9-7-1-c000.snappy.parquet"
file_type = "parquet"
#file_silver = "wasbs://poc-databricks-silver@stgmannaz.blob.core.windows.net/rawdata.parquet"

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

df.write.mode("overwrite").format("delta").saveAsTable("df_wc")


# COMMAND ----------

df.write.format("delta").mode("overwrite").option("mergeSchema","true").save("wasbs://poc-databricks-gold@stgmannaz.blob.core.windows.net/df_wc_gold.delta")
