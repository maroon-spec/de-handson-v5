-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold Tableの作成
-- MAGIC
-- MAGIC 一般的にGOlD Tableはユーザー側で作成することが多いです。その場合シンプルなSQLエディタを使った作成が可能です。
-- MAGIC ここでも、AI Assistantを使った作成が可能となります。
-- MAGIC
-- MAGIC #### Step 1. 新規クエリーの作成 
-- MAGIC 2_GoldTableフォルダーの右にあるケバブマークをクリックして、作成ー＞クエリーを選択
-- MAGIC <img src="../images/query_editor.png" alt="Query作成" width="500">
-- MAGIC
-- MAGIC #### Step 2. 作成されたクエリーをクリックして、名前をgold_inquiry_month_mvにする。
-- MAGIC #### Step 3. 新規のMateriarized Viewを作成。（内容は以下の指示にしたがってください）
-- MAGIC #### Step 4. 同様にgold_resolve_date_mv, gold_resolve_status_mv を作成する。 (Step2,3と同じステップ)
-- MAGIC
-- MAGIC 最終的に、３つの新規クエリーが作成されます。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## SQLクエリーのAI Assistant への支持例
-- MAGIC
-- MAGIC スキーマのパス (handson_catalog.databricks) は随時、ご自身のパスに変更してください。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### gold_inquiry_month_mv
-- MAGIC
-- MAGIC > ```handson_catalog.databricks.silver_inquiry_sales　から月別・問い合わせタイプ別の集計をマテリアライズドビュー(gold_inquiry_month_mv）として作成して、そのあと結果を表示する。ちなみに日付部分ははdate型とする。なお上書きが出来るようにする```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### gold_resolve_date_mv
-- MAGIC
-- MAGIC > `handson_catalog.databricks.silver_inquiry_salesテーブルから、解決済み（resolution_dateがNULLでない）の問い合わせについて、inquiry_id、inquiry_date、resolution_date、および問い合わせ日から解決日までの日数を計算したマテリアライズドビューgold_resolve_date_mvを作成してください。作成後、内容を確認する。なお上書きが出来るようにする`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### gold_resolve_status_mv
-- MAGIC
-- MAGIC > `handson_catalog.databricks.silver_inquiry_salesテーブルから、resolution_dateを月単位で集計し、resolution_statusごとの解決件数をカウントするマテリアライズドビューgold_resolve_status_mvを作成してください。月順に並べて、作成後に内容を確認したいです。なお上書きが出来るようにする`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### 最後にクエリー保存を忘れずに
-- MAGIC
-- MAGIC <img src="../images/query_save.png" alt="クエリー保存" width="500">
-- MAGIC
-- MAGIC クエリーの作成が完了したら、最後に右上の「保存」ボタンをクリックしてクエリーを保存してください。ジョブ実行時に保存されていないとエラーになります。
