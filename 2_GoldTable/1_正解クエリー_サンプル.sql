-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## SQLクエリー　正解クエリー例

-- COMMAND ----------

-- DBTITLE 1,gold_inquiry_month_mv
-- MAGIC %skip
-- MAGIC -- ゴールドレイヤーの月別・問い合わせタイプ別集計をマテリアライズドビューとして作成
-- MAGIC CREATE OR REPLACE MATERIALIZED VIEW handson_catalog.databricks.gold_inquiry_month_mv AS
-- MAGIC SELECT
-- MAGIC   date_trunc('month', inquiry_date) AS month,
-- MAGIC   inquiry_type,
-- MAGIC   COUNT(*) AS inquiry_count
-- MAGIC FROM
-- MAGIC   handson_catalog.databricks.silver_inquiry_sales
-- MAGIC GROUP BY
-- MAGIC   date_trunc('month', inquiry_date),
-- MAGIC   inquiry_type;
-- MAGIC
-- MAGIC -- 結果を表示
-- MAGIC SELECT * FROM handson_catalog.databricks.gold_inquiry_month_mv
-- MAGIC ORDER BY month, inquiry_type;

-- COMMAND ----------

-- DBTITLE 1,gold_resolve_date_mv
-- MAGIC %skip
-- MAGIC CREATE OR REPLACE MATERIALIZED VIEW handson_catalog.databricks.gold_resolve_date_mv AS
-- MAGIC SELECT
-- MAGIC   inquiry_id,
-- MAGIC   inquiry_date,
-- MAGIC   resolution_date,
-- MAGIC   DATEDIFF(resolution_date, inquiry_date) AS `解決日数`
-- MAGIC FROM
-- MAGIC   handson_catalog.databricks.silver_inquiry_sales
-- MAGIC WHERE
-- MAGIC   resolution_date IS NOT NULL;
-- MAGIC
-- MAGIC select * from handson_catalog.databricks.gold_resolve_date_mv

-- COMMAND ----------

-- DBTITLE 1,gold_resolve_date_mv
-- MAGIC %skip
-- MAGIC CREATE OR REPLACE MATERIALIZED VIEW handson_catalog.databricks.gold_resolve_status_mv AS
-- MAGIC SELECT
-- MAGIC   date_trunc('month', resolution_date) AS month,
-- MAGIC   resolution_status,
-- MAGIC   COUNT(*) AS resolved_count
-- MAGIC FROM
-- MAGIC   handson_catalog.databricks.silver_inquiry_sales
-- MAGIC GROUP BY
-- MAGIC   month, resolution_status
-- MAGIC ORDER BY
-- MAGIC   month;
-- MAGIC
-- MAGIC select * from handson_catalog.databricks.gold_resolve_status_mv