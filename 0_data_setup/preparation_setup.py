# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC ##  環境条件
# MAGIC
# MAGIC Unity Catalogがセットアップされており、このノートブックを実行する管理者はカタログ作成権限を持っていること
# MAGIC
# MAGIC ##  セットアップ方法
# MAGIC 1. 以下のカタログ名を入力してください。
# MAGIC 1. 管理者はハンズオンやデモの前にこちらのノートブックを実行しておきます。

# COMMAND ----------

# デモで利用するカタログやスキーマ情報を定義します
catalog = 'handson_catalog'
schema = 'handson_db'

# COMMAND ----------

# DBTITLE 1,Catalog準備
# Catalog 作成
spark.sql(f'CREATE CATALOG IF NOT EXISTS {catalog}')
spark.sql(f'GRANT CREATE, USAGE ON CATALOG {catalog} TO `account users`')
spark.sql(f'USE CATALOG {catalog}')

# Schema 作成
spark.sql(f'Create schema if not exists {schema}')
spark.sql(f'GRANT ALL PRIVILEGES ON SCHEMA {schema} TO `account users`')
spark.sql(f'USE SCHEMA {schema}')

# Volume作成
spark.sql('CREATE VOLUME IF NOT EXISTS handson_volume;')
spark.sql('GRANT ALL PRIVILEGES ON VOLUME handson_volume TO `account users`')

# 本日の日付を取得
from datetime import datetime
CURRENT_DATE = datetime.now().strftime('%Y-%m-%d')

# Path名
schema_path = f'{catalog}.{schema}'

# COMMAND ----------

# MAGIC %md
# MAGIC ## 基礎データ作成

# COMMAND ----------

import random
from datetime import datetime, timedelta
from pyspark.sql.functions import col

# 男性と女性に適した名前を定義
first_names_male = ['次郎', '健太', '翔太', '直樹', '浩二', '拓也', '誠', '隆', '大介', '裕太', '和也', '勇太', '智也', '健一', '雄太', '剛', '亮', '洋平', '翔', '竜也']
first_names_female = ['凛', '陽葵', '紬', '芽依', '結愛', '莉子', '葵', '陽菜', '美桜', '心春', '凪', '翠', '咲良', '結菜', '澪', '彩葉', '柚葉', '茉白', '詩', '楓']
last_names = ['田中', '佐藤', '鈴木', '高橋', '渡辺', '伊藤', '山田', '中村', '小林', '加藤']

# 600人の男性と400人の女性のユーザーを適切な名前と性別の組み合わせで生成
names_gender = []
for _ in range(600):
    last_name = random.choice(last_names)
    first_name = random.choice(first_names_male)
    names_gender.append((last_name + ' ' + first_name, '男性'))

for _ in range(400):
    last_name = random.choice(last_names)
    first_name = random.choice(first_names_female)
    names_gender.append((last_name + ' ' + first_name, '女性'))

# 日本の都道府県の人口比率を定義（簡略化された例）
prefecture_data = {
    '東京都': 0.1,
    '大阪府': 0.09,
    '神奈川県': 0.08,
    '愛知県': 0.07,
    '埼玉県': 0.06,
    '北海道': 0.05,
    '福岡県': 0.05,
    '兵庫県': 0.04,
    '千葉県': 0.03
}

# 人口分布に基づいてリストを作成
prefectures = []
for prefecture, weight in prefecture_data.items():
    prefectures.extend([prefecture] * int(weight * 100))  # 例示目的でスケール調整

# 1000人のユーザーに対してランダムな居住都道府県を生成
random_prefectures = [random.choice(prefectures) for _ in range(1000)]

# ランダムな電話番号を生成
random_phone_numbers = [f'{random.randint(100, 999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}' for _ in range(1000)]

# ランダムなメールアドレスを生成
random_emails = [f'user{random.randint(1, 10000)}@example.com' for _ in range(1000)]

# 1000人のユーザーに対してランダムな年齢を生成
random_ages = [random.randint(18, 65) for _ in range(1000)]

# 担当営業を定義
sales_reps = ['CS担当_1', 'CS担当_2', 'CS担当_3', 'CS担当_4', 'CS担当_5']

# 特約の有無と有効期限を生成
special_contracts = [random.choice([True, False]) for _ in range(1000)]
special_contract_expiry_dates = [(datetime(2024, 6, 1) + timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d') for _ in range(1000)]

# 入会日を生成（2020年から2024年7月まで）
join_dates = [(datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1642))).strftime('%Y-%m-%d') for _ in range(1000)]

# 契約データのサンプルデータを生成
contract_data = [
  (1001+i, names_gender[i][0], random_ages[i], names_gender[i][1],
     random_phone_numbers[i], random_emails[i], random_prefectures[i], join_dates[i],
     random.choice(['生命保険商品A', '生命保険商品B', '生命保険商品C']),
     random.choice([1, 2, 3, 5]), random.randint(3000, 30000),
     special_contracts[i], special_contract_expiry_dates[i], random.choice(sales_reps))
    for i in range(1000)
]

# 追加のカラムを持つDataFrameを作成
contract_df = spark.createDataFrame(contract_data, \
    ['user_id', 'user_name', 'age', 'gender', 'phone_number', 'email', 'residence_prefecture', 'join_date', 'contract_product', 'contract_period', 'insurance_fee', 'is_special_contract', 'special_contract_expiry', 'sales_rep']) 

# データフレームの表示
display(contract_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze User Table作成

# COMMAND ----------

# DBTITLE 1,bronze_user Table作成
import random
from datetime import datetime, timedelta
from pyspark.sql.functions import to_date

# 1. ユーザーマスタテーブル (bronze_user)
user_master_data = [
    (row.user_id,
     row.user_name, 
     (datetime.now() - timedelta(days=365*row.age)).strftime('%Y-%m-%d'),
     row.gender, row.residence_prefecture,
     row.phone_number, row.email, row.join_date)
    for row in contract_df.collect()
]

user_master_df = spark.createDataFrame(user_master_data, 
    ["user_id", "name", "birth_date", "gender", "residence_prefecture", "phone_number", "email", "registration_date"])

# 日付型に変換
user_master_df = user_master_df.withColumn("birth_date", to_date("birth_date", "yyyy-MM-dd"))
user_master_df = user_master_df.withColumn("registration_date", to_date("registration_date", "yyyy-MM-dd"))

# テーブル生成
user_master_df.write.format("delta").mode("overwrite").saveAsTable(f'{schema_path}.bronze_user')
display(user_master_df.limit(10))

# Set the Primary Key
spark.sql(f"ALTER TABLE {schema_path}.bronze_user ALTER COLUMN user_id SET NOT NULL")
spark.sql(f"ALTER TABLE {schema_path}.bronze_user ADD CONSTRAINT user_id_pk PRIMARY KEY (user_id)")


# COMMAND ----------

# DBTITLE 1,コメント付与
# テーブル名
table_name = f'{schema_path}.bronze_user'

# テーブルコメント
comment = """
`bronze_user`テーブルには、保険契約者に関する情報が含まれています。このテーブルには、契約者の一意のユーザーID、氏名、生年月日、性別、住所、電話番号、メールアドレス、および登録日が含まれています。このテーブルは顧客データの管理や保険契約の追跡に不可欠です。契約者の人口統計や連絡先の詳細に関する貴重な洞察を提供し、ビジネスが効果的にコミュニケーションを取り、パーソナライズされたサービスを提供することを可能にします。このテーブルは顧客情報の中央リポジトリとして機能し、保険契約管理、顧客サービス、マーケティングキャンペーンなどのさまざまなビジネスプロセスをサポートします。
"""
spark.sql(f'COMMENT ON TABLE {table_name} IS "{comment}"')

# カラムコメント
column_comments = {
    "user_id": "文字列、ユニーク(主キー)、契約者ID",
    "name": "文字列、契約者名",
    "birth_date": "日付、YYYY-MM-DDフォーマット、",
    "gender": "文字列、性別",
    "residence_prefecture": "文字列、例: '東京都', '神奈川県', '北海道'",
    "phone_number": "文字列、000-0000-0000",
    "email": "文字列、メールフォーマット",
    "registration_date": "日付、YYYY-MM-DDフォーマット、入会日"
}

for column, comment in column_comments.items():
    # シングルクォートをエスケープ
    escaped_comment = comment.replace("'", "\\'")
    sql_query = f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_comment}'"
    spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### bronze_inquiry_history（問い合わせ履歴）

# COMMAND ----------

import random
from datetime import datetime, timedelta
from pyspark.sql.functions import to_date

# 問い合わせタイプと問い合わせ詳細のマッピング
inquiry_mapping = {
  "一般問い合わせ": [
    "現在契約している保険の詳細を確認したいのですが、契約内容をメールで送っていただけますか？",
    "保険金請求をしたのですが、現在の進捗状況を教えていただけますか？迅速な対応を期待しています。",
    "契約更新の手続き方法を教えてください。少し複雑に感じています。",
    "特約の変更を希望しています。どのような手続きが必要か教えていただけますか？",
    "受取人の変更をしたいのですが、手続き方法を教えてください。",
    "解約を考えています。手続き方法を教えてください。",
    "保険料の支払い方法を変更したいのですが、どのように手続きすればよいですか？",
    "病気に関する保険請求について、手続き方法を教えてください。",
    "保険の見直しを考えています。どのように進めればよいですか？",
    "顧客情報の変更をしたいのですが、手続き方法を教えてください。",
    "オンラインサービスの利用方法について教えてください。使いやすいと聞いています。",
    "相談窓口の場所と営業時間を教えてください。",
    "保険商品の詳細情報を教えてください。",
    "担当者を変更したいのですが、手続き方法を教えてください。",
    "今後の保険商品の情報を教えてください。",
    "保険請求後の手続きについて教えてください。",
    "個人情報保護についての詳細を教えてください。",
    "その他のサービスについて教えてください。"
  ],
  "苦情": [
    "契約内容がよく分からないので、再度説明をお願いしたいです。前回の説明が不十分だったと思います。",
    "保険金請求をしたのに、全然進捗がありません。対応が遅すぎると思います。",
    "契約更新の手続きが複雑すぎて困っています。もっと簡単にできる方法はないのでしょうか？",
    "特約の変更をお願いしたのに、全く進んでいないようです。対応が遅すぎます。",
    "受取人の変更を依頼したのに、手続きが進んでいません。どうなっているのですか？",
    "解約手続きをお願いしたのに、全然進んでいません。どうなっているのですか？",
    "支払い方法の変更をお願いしたのに、全然反映されていません。どうなっているんですか？",
    "事故の際に保険会社の対応が遅すぎて困りました。もっと迅速に対応してほしいです。",
    "保険の見直しをお願いしたのに、全然提案がありません。どうなっているのですか？",
    "顧客情報の変更を依頼したのに、全然反映されていません。どうなっているんですか？",
    "オンラインサービスが使いにくくて困っています。もっと改善してください。",
    "相談窓口の対応が不親切で、不満です。もっと丁寧に対応してほしいです。",
    "保険商品の説明が分かりにくくて困っています。もっと詳しく教えてください。",
    "担当者の対応が不親切で、不満です。担当者を変えてほしいです。",
    "新しい保険商品の情報が全然届かないのですが、どうなっているのですか？",
    "保険請求後の手続きが全然進んでいないのですが、どうなっているのですか？",
    "個人情報の取り扱いが不安です。もっとしっかりとした対応をお願いします。",
    "その他のサービスが全然役に立たないのですが、どうなっているのですか？"
  ],
  "追加保障相談": [
    "特約の変更をお願いしましたが、すぐに対応していただき、ありがとうございました。新しい特約内容に満足しています。",
    "保険の見直しをお願いしましたが、最適なプランを提案していただき、非常に満足しています。",
    "新しい支払い方法に変更しましたが、手続きがスムーズで助かりました。ありがとうございました。"
  ],
  "その他": [
    "先日契約した保険の内容を確認したいのですが、とても分かりやすい説明をいただきありがとうございました。再度確認したいので、契約書のコピーを送っていただけますか？",
    "先日、保険金請求をしましたが、迅速に対応していただきありがとうございます。手続きもスムーズで助かりました。",
    "契約更新手続きが完了しました。迅速な対応に感謝します。今後ともよろしくお願いします。",
    "受取人の変更手続きがスムーズに完了しました。迅速な対応に感謝しています。",
    "解約手続きがスムーズに完了しました。迅速な対応に感謝しています。",
    "事故の際に迅速に対応していただき、ありがとうございました。安心して手続きを進めることができました。",
    "住所変更の手続きが迅速に完了しました。ありがとうございました。",
    "オンラインサービスが非常に使いやすく、便利です。ありがとうございます。",
    "相談窓口の対応がとても親切で、安心して相談できました。ありがとうございました。",
    "新しい保険商品の説明がとても分かりやすく、興味を持ちました。ありがとうございます。",
    "担当者の方がとても親切で、安心して相談できました。今後ともよろしくお願いします。",
    "新しい保険商品の紹介をいただき、非常に興味を持ちました。ありがとうございます。",
    "保険請求後の手続きがスムーズで、非常に満足しています。ありがとうございました。",
    "個人情報保護について、しっかりとした対応をしていただき、安心しました。",
    "その他のサービスについて、非常に満足しています。ありがとうございます。"
  ]
}

# 5. 問い合わせ履歴テーブル (bronze_inquiry_history)
inquiry_history_data = [
    (i+1, 
     1000+random.randint(1, 100),
     (datetime.now() - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d'),
     inquiry_type := random.choice(list(inquiry_mapping.keys())),
     inquiry_detail := random.choice(inquiry_mapping[inquiry_type]),  # マッピングに基づく問い合わせ詳細
     random.choice(["電話", "メール", "対面"]),
     (datetime.now() - timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d'),
     random.choice(["新規", "解決", "対応中", "未解決"]),
     random.choice(['CS担当_1', 'CS担当_2', 'CS担当_3', 'CS担当_4', 'CS担当_5'])
    )
    for i in range(300)  # 300件のサンプルデータ
]

inquiry_history_df = spark.createDataFrame(inquiry_history_data,
    ["inquiry_id", "user_id", "inquiry_date", "inquiry_type", "inquiry_detail", "inquiry_channel", "resolution_date", "resolution_status", "cs_rep"])

# 日付型に変換
inquiry_history_df = inquiry_history_df.withColumn("inquiry_date", to_date("inquiry_date", "yyyy-MM-dd"))
inquiry_history_df = inquiry_history_df.withColumn("resolution_date", to_date("resolution_date", "yyyy-MM-dd"))

# テーブル作成
inquiry_history_df.write.format("delta").mode("overwrite").saveAsTable(f'{schema_path}.bronze_inquiry_history')

# Set the Primary Key
spark.sql(f"ALTER TABLE {schema_path}.bronze_inquiry_history ALTER COLUMN inquiry_id SET NOT NULL")
spark.sql(f"ALTER TABLE {schema_path}.bronze_inquiry_history ADD CONSTRAINT inquiry_id_pk PRIMARY KEY (inquiry_id)")

# 外部キーの作成
spark.sql(f"ALTER TABLE {schema_path}.bronze_inquiry_history ADD CONSTRAINT fk2_user_id FOREIGN KEY (user_id) REFERENCES {schema_path}.bronze_user(user_id)")

display(inquiry_history_df.limit(10))

# COMMAND ----------

# DBTITLE 1,コメント付与
# テーブル名
table_name = f'{schema_path}.bronze_inquiry_history'

# テーブルコメント
comment = """
`bronze_inquiry_history` テーブルには、ユーザーが行った問い合わせに関する情報が含まれています。このテーブルには、問い合わせID、ユーザーID、問い合わせ日、問い合わせチャネル、問い合わせの種類、問い合わせの詳細が含まれています。また、解決日と解決状況も含まれており、問い合わせがいつどのように解決されたかを示します。このテーブルは、ユーザーからの問い合わせを追跡・分析し、受け取った問い合わせのチャネルや種類を理解し、解決プロセスを監視するために重要です。顧客サービスの改善や、注意が必要なトレンドや問題を特定するための貴重なインサイトを提供します。
"""
spark.sql(f'COMMENT ON TABLE {table_name} IS "{comment}"')

# カラムコメント
column_comments = {
    "inquiry_id": "整数、ユニーク（主キー）、問い合わせID",
    "user_id": "整数、ユニーク(外部キー)、契約者ID",
    "inquiry_date": "日付、YYYY-MM-DDフォーマット、問い合わせ日",
    "inquiry_channel": "文字列、問い合わせチャネル、例: '電話', 'メール', '対面'",
    "inquiry_type": "文字列、問い合わせタイプ、例: '一般問い合わせ', '苦情', '追加保障相談'",
    "inquiry_detail": "文字列、問い合わせ詳細",
    "resolution_date": "日付、YYYY-MM-DDフォーマット、解決日",
    "resolution_status": "文字列、解決状況、例: '新規', '解決', '対応中', '未解決'",
    "cs_rep": "文字列、CS担当"
}

for column, comment in column_comments.items():
    # シングルクォートをエスケープ
    escaped_comment = comment.replace("'", "\\'")
    sql_query = f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_comment}'"
    spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Payment 履歴

# COMMAND ----------

import random
from datetime import datetime, timedelta
from pyspark.sql.functions import to_date

# bronze_table から user_id を取得
user_ids = [row.user_id for row in spark.table(f"{schema_path}.bronze_user").select("user_id").collect()]

# 保険料支払いデータを生成
premium_payment_data = []
for user_id in user_ids:
    # 各 user_id に対して複数の支払いデータを生成
    for _ in range(random.randint(1, 3)):  # 1から3件の支払いデータを生成
        premium_payment_data.append((
            len(premium_payment_data) + 1,
            user_id,
            (datetime.now() - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d'),
            random.randint(2500, 8333),
            random.choice(["正常", "正常", "正常", "正常", "正常", "正常", "正常", "正常", "遅延", "未払い"])
        ))

premium_payment_df = spark.createDataFrame(premium_payment_data,
    ["payment_id", "user_id", "payment_date", "payment_amount", "payment_status"])

# 日付型に変換
premium_payment_df = premium_payment_df.withColumn("payment_date", to_date("payment_date", "yyyy-MM-dd"))

# Save Data
premium_payment_df.write.format("delta").mode("overwrite").saveAsTable(f'{schema_path}.bronze_payment')

# Set the Primary Key
spark.sql(f"ALTER TABLE {schema_path}.bronze_payment ALTER COLUMN payment_id SET NOT NULL")

# 既存の主キー制約があれば削除（存在しない場合はエラーを無視）
try:
    spark.sql(f"ALTER TABLE {schema_path}.bronze_payment DROP CONSTRAINT payment_id_pk")
except Exception as e:
    if "CONSTRAINT_DOES_NOT_EXIST" in str(e) or "does not exist" in str(e):
        pass
    else:
        raise

spark.sql(f"ALTER TABLE {schema_path}.bronze_payment ADD CONSTRAINT payment_id_pk PRIMARY KEY (payment_id)")

# 外部キーの作成（既に存在する場合は無視）
try:
    spark.sql(f"ALTER TABLE {schema_path}.bronze_payment ADD CONSTRAINT fk2_user_id FOREIGN KEY (user_id) REFERENCES {schema_path}.bronze_user(user_id)")
except Exception as e:
    if "CONSTRAINT_ALREADY_EXISTS_IN_SCHEMA" in str(e):
        print("外部キー制約 'fk2_user_id' は既に存在します。スキップします。")
    else:
        raise

display(premium_payment_df)

# COMMAND ----------

# テーブル名
table_name = f'{schema_path}.bronze_payment'

# テーブルコメント
comment = """
`bronze_payment`テーブルには、FSI生命保険デモにおけるブロンズプレミアム契約の支払いに関連するデータが含まれています。このテーブルには、支払いID、契約ID、支払日、支払額、および支払いステータスが含まれています。支払いIDは主キーとして機能し、各支払いを一意に識別します。ユーザーIDは外部キーであり、各ユーザーに対応するユーザーにリンクします。支払日は支払いが行われた日付を示し、支払額は各支払いに対して支払われた金額を示します。最後に、支払いステータスは各支払いの現在の状態を表し、「正常」、「遅延」または「未払い」などのステータスが含まれます。
"""
spark.sql(f'COMMENT ON TABLE {table_name} IS "{comment}"')

# カラムコメント
column_comments = {
    "payment_id": "整数、ユニーク（主キー）、支払いID",
    "user_id": "整数、ユニーク（外部キー）、ユーザーID",
    "payment_date": "日付、YYYY-MM-DDフォーマット、支払い日",
    "payment_amount": "整数、支払い料金",
    "payment_status": "文字列、支払い状況、例: '正常', '遅延', '未払い'"
}

for column, comment in column_comments.items():
    # シングルクォートをエスケープ
    escaped_comment = comment.replace("'", "\\'")
    sql_query = f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_comment}'"
    spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## CSV生成

# COMMAND ----------

# 営業担当者IDと氏名のマッピングデータを作成
sales_rep_mapping = [
    ('CS担当_1', '田中 太郎'),
    ('CS担当_2', '佐藤 花子'),
    ('CS担当_3', '鈴木 一郎'),
    ('CS担当_4', '高橋 美咲'),
    ('CS担当_5', '渡辺 健')
]

sales_rep_df = spark.createDataFrame(sales_rep_mapping, ['sales_rep_id', 'sales_rep_name'])

# pandasとしてcsv保存
import pandas as pd, os

pandas_df = sales_rep_df.toPandas()
csv_path = f'/Volumes/{catalog}/{schema}/handson_volume/sales_mapping.csv'

pandas_df.to_csv(csv_path, index=False, encoding='utf-8-sig')

display(sales_rep_df)

# COMMAND ----------

# MAGIC %md
# MAGIC