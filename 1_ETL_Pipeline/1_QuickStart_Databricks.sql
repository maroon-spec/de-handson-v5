-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Quick Start & データパイプライン作成

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Databricks Notebookの特徴
-- MAGIC
-- MAGIC - 複数言語（Python, SQL, Scala, R）を同一ノートブック内で利用可能
-- MAGIC - リアルタイム共同編集が可能
-- MAGIC - バージョン管理機能を搭載
-- MAGIC - スケジュール実行
-- MAGIC - クラスター切り替えが簡単
-- MAGIC - Unity CatalogやMLflowなどDatabricksサービス連携
-- MAGIC - git連携
-- MAGIC - AI Assistant機能
-- MAGIC
-- MAGIC [ベストプラクティス](https://docs.databricks.com/aws/ja/notebooks/best-practices)

-- COMMAND ----------

-- DBTITLE 1,ハンズオン環境セットアップ
-- MAGIC %run ../0_userenv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 1. silver_inquiry テーブルの作成
-- MAGIC
-- MAGIC handson_catalog.handson_db にある bronze_inquiry_historyと bronze_user をuser_idをキーとして結合し、silver_inquiry テーブルを作成。 その際に、保存場所はhandson_catalog.databricks。またuser_idカラムは重複しないように一つだけ残すようにする。また作成するテーブルはすでにある場合は上書きする。最後に作成したテーブルを表示
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 2. CSVファイルから、bronze_sales テーブルを作成
-- MAGIC
-- MAGIC UnityCatalog Volumes上のhandson_catalog.handson_db.handson_volume にある sales_mapping.csv ファイルを読み込み、handson_catalog.databricksに bronze_salesテーブルとして保存。その際にread_filesを使い、headerあり、カラムのスキーマも推測する。また作成するテーブルはすでにある場合は上書きする。最後に作成したテーブルを表示。 (パスは/Volumesとする)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 3. silver_inquiry_sales テーブルを作成
-- MAGIC
-- MAGIC handson_catalog.databricks にある silver_inquiryと bronze_sales をcs_rep と sales_rep_id をキーとして結合し、silver_inquiry_sales テーブルを作成。 保存場所はhandson_catalog.databricks。またcs_repカラムのみ残して、sales_rep_idは削除する。_rescued_dataカラムは不要。また作成するテーブルはすでにある場合は上書きする。最後に作成したテーブルを表示
