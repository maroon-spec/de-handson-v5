# Databricks notebook source
# DBTITLE 1,ハンズオン用の初期設定
#個人用の環境を分けるため固有の名前を指定してください。（この後ハンズオン用のデータベース名として利用します。）         

user = "databricks"

# COMMAND ----------

# DBTITLE 1,***.  ここより以下は修正不要です  ***
# Handsonで利用するCatalog
catalog = "handson_catalog"
schema = 'handson_db'

# 利用するDatabase（Schema)を作成
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{user}")
spark.sql(f"USE {catalog}.{user}")

print(f"サンプルデータのテーブル格納場所： {catalog}.{schema} ")
print(f"みなさんが作成するデータの格納場所: 　{catalog}.{user}")