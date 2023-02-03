# Databricks notebook source
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import pandas as pd
import io
from faker import Faker

fake = Faker()


# COMMAND ----------

output = io.StringIO()
df = pd.DataFrame(
    [
        {
            "ARBPL": fake.name(),
            "BUKRS": fake.name(),
            "DIRECT_PM": fake.name(),
            "DIRECTORIA": fake.name(),
            "EMPRESA_LD3": fake.name(),
            "FASE_WC": fake.name(),
            "GRCIA_AREA": fake.name(),
            "GRCIA_GRAL": fake.name(),
            "LVORM": fake.name(),
            "OBJID_DEX_TEXT": fake.name(),
            "OBJID_DEX": fake.name(),
            "OBJID_DIR_TEXT": fake.name(),
            "OBJID_DIR": fake.name(),
            "OBJID_GAR_TEXT": fake.name(),
            "OBJID_GAR": fake.name(),
            "OBJID_GEG_TEXT": fake.name(),
            "OBJID_GEG": fake.name(),
            "OBJID_GER_TEXT": fake.name(),
            "OBJID_GER": fake.name(),
            "OBJID_SUP_TEXT": fake.name(),
            "OBJID_SUP": fake.name(),
            "PRIMARY_KEY_JOIN_WC": fake.name(),
            "STEUS": fake.name(),
            "SUPERV": fake.name(),
            "UBERLAST": fake.name(),
            "UPDATE_ON": fake.date_time(),
            "VERAN": fake.name(),
            "VERWE": fake.name(),
            "WC_ENG": fake.name(),
            "WERKS_DESCR": fake.name(),
            "WERKS": fake.name(),
            "ZRESP_TEXT": fake.name(),
            "ZRESP": fake.name(),
            "CALLMONTH": fake.name(),
            "PLANTA_VALIDA": fake.boolean()
           
        }
        for _ in range(10)
        
    ]
)
output = df.to_csv(index_label="idx", encoding = "utf-8")

connect_str='DefaultEndpointsProtocol=https;AccountName=stgmannaz;AccountKey=biHOdhE3NSG3nCdoaijoDhBDDT4uSA0l+dNKflCAHOKy6C9WlpFaCo/uK2rRIFHM0eg1qdDBZ3PA+AStl9Canw==;EndpointSuffix=core.windows.net'
blob_service_client = BlobServiceClient.from_connection_string(connect_str)
container_name = "poc-databricks-bronze"
url_blob = "https://stgmannaz.blob.core.windows.net/teste-b"

blob_client = blob_service_client.get_blob_client(container=container_name, blob="rawdata.csv")

blob_client.upload_blob(output, overwrite=True)
