# Databricks notebook source
spark.conf.set(
 "fs.azure.account.auth.type.stgmannaz.blob.core.windows.net",
 "OAuth"
)
spark.conf.set(
 "fs.azure.account.oauth.provider.type.stgmannaz.blob.core.windows.net",
 "org.apache.hadoop.fs.azurebfs.oauth.ServiceOAuthProvider"
)
spark.conf.set(
 "fs.azure.account.oauth.client.id.stgmannaz.blob.core.windows.net",
 "0127-151332-ecak85z1"
)
spark.conf.set(
 "fs.azure.account.oauth.client.secret.stgmannaz.blob.core.windows.net",
 "biHOdhE3NSG3nCdoaijoDhBDDT4uSA0l+dNKflCAHOKy6C9WlpFaCo/uK2rRIFHM0eg1qdDBZ3PA+AStl9Canw=="
)
spark.conf.set(
 "fs.azure.account.oauth.token.provider.type.stgmannaz.blob.core.windows.net",
 "ClientCredential"
)