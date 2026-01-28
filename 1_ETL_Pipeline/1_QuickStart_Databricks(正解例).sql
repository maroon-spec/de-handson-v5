-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Quick Start & データパイプライン作成(正解例)

-- COMMAND ----------

-- DBTITLE 1,ハンズオン環境セットアップ
-- MAGIC %run ../0_userenv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 1. silver_inquiry テーブルの作成
-- MAGIC
-- MAGIC handson_catalog.handson_db にある bronze_inquiry_historyと bronze_user をuser_idをキーとして結合し、current schema上にsilver_inquiry テーブルを作成。またuser_idカラムは重複しないように一つだけ残すようにする。また作成するテーブルはすでにある場合は上書きする。最後に作成したテーブルを表示
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE handson_catalog.databricks.silver_inquiry AS
SELECT
  bih.inquiry_id,
  bih.user_id,
  bih.inquiry_date,
  bih.inquiry_type,
  bih.inquiry_detail,
  bih.inquiry_channel,
  bih.resolution_date,
  bih.resolution_status,
  bih.cs_rep,
  bu.name,
  bu.birth_date,
  bu.gender,
  bu.residence_prefecture,
  bu.phone_number,
  bu.email,
  bu.registration_date
FROM
  handson_catalog.handson_db.bronze_inquiry_history AS bih
INNER JOIN
  handson_catalog.handson_db.bronze_user AS bu
ON
  bih.user_id = bu.user_id;

SELECT * FROM handson_catalog.databricks.silver_inquiry;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 2. CSVファイルから、bronze_sales テーブルを作成
-- MAGIC
-- MAGIC UnityCatalog Volumes上のhandson_catalog.handson_db.handson_volume にある sales_mapping.csv ファイルを読み込み、current schema上に bronze_salesテーブルとして保存。ファイルはCSVであり、ヘッダーを読み込む。最後に作成したテーブルを表示

-- COMMAND ----------

CREATE OR REPLACE TABLE handson_catalog.databricks.bronze_sales AS
SELECT *
FROM read_files(
  '/Volumes/handson_catalog/handson_db/handson_volume/sales_mapping.csv',
  format => 'csv',
  header => 'true',
  inferSchema => 'true'
);

SELECT * FROM handson_catalog.databricks.bronze_sales;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 3. silver_inquiry_sales テーブルを作成
-- MAGIC
-- MAGIC handson_catalog.databricks にある silver_inquiryと bronze_sales をcs_rep と sales_rep_id をキーとして結合し、current schema上にsilver_inquiry_sales テーブルを作成。またキーに関してはcs_repカラムのみ残して、sales_rep_idは削除する。また作成するテーブルはすでにある場合は上書きする。最後に作成したテーブルを表示

-- COMMAND ----------

CREATE OR REPLACE TABLE handson_catalog.databricks.silver_inquiry_sales AS
SELECT
  si.inquiry_id,
  si.user_id,
  si.inquiry_date,
  si.inquiry_type,
  si.inquiry_detail,
  si.inquiry_channel,
  si.resolution_date,
  si.resolution_status,
  si.cs_rep,
  si.name,
  si.birth_date,
  si.gender,
  si.residence_prefecture,
  si.phone_number,
  si.email,
  si.registration_date,
  bs.sales_rep_name
FROM
  handson_catalog.databricks.silver_inquiry AS si
INNER JOIN
  handson_catalog.databricks.bronze_sales AS bs
ON
  si.cs_rep = bs.sales_rep_id;

SELECT * FROM handson_catalog.databricks.silver_inquiry_sales;
