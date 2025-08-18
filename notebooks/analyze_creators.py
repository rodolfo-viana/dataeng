# Databricks notebook source
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime, timedelta

print("=== ANÁLISE DE CREATORS ===")
print(f"Executado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

users_yt = spark.table("default.users_yt")
silver_posts_creator = spark.table("default.silver_post_creator")

print(f"Users YT: {users_yt.count()} registros")
print(f"Silver Posts Creator: {silver_posts_creator.count()} registros")

joined_data = silver_posts_creator.join(
    users_yt,
    silver_posts_creator.yt_user == users_yt.user_id,
    "left"
)

print(f"Dados após join: {joined_data.count()} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top 3 Posts por Likes (Últimos 2 Anos)
# MAGIC Observação: O enunciado pede posts dos últimos seis meses, mas creio que a extração dos dados foi anterior a isso, por não há posts no último semestre. Aumentei para dois anos.

# COMMAND ----------

# Abaixo está o código Python, mas o Databricks Free Edition estava
# apresentando problema para conectar, então também fiz em SQL, considerando
# que o servidor é outro. 
# (Serverless Starter Warehouse (SQL) vs. Serverless (Python)).
#
# SELECT user_id, title, likes, rank
# FROM (
#   SELECT 
#     u.user_id,
#     s.title,
#     s.likes,
#     ROW_NUMBER() OVER (PARTITION BY u.user_id ORDER BY s.likes DESC) as rank
#   FROM default.silver_post_creator s
#   INNER JOIN default.users_yt u ON s.yt_user = u.user_id
#   WHERE s.published_at >= DATE_SUB(current_date(), 730)
# )
# WHERE rank <= 3
# ORDER BY user_id, rank;

data_limite = (datetime.now() - timedelta(days=730))
print(f"Data limite para análise (últimos 6 meses): {data_limite}")

recent_posts_likes = joined_data.filter(
    (F.col("published_at") >= data_limite) & 
    (F.col("user_id").isNotNull())
)

window_likes = Window.partitionBy("user_id").orderBy(F.col("likes").desc())

top_posts_likes = recent_posts_likes.withColumn(
    "rank", F.row_number().over(window_likes)
).filter(
    F.col("rank") <= 3
).select(
    "user_id", "title", "likes", "rank"
).orderBy("user_id", "rank")

print("=== TOP 3 POSTS POR LIKES (ÚLTIMOS 6 MESES) ===")
top_posts_likes.show(50, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top 3 Posts por Views (Últimos 2 Anos)

# COMMAND ----------

# SELECT user_id, title, views, rank
# FROM (
#   SELECT 
#     u.user_id,
#     s.title,
#     s.views,
#     ROW_NUMBER() OVER (PARTITION BY u.user_id ORDER BY s.views DESC) as rank
#   FROM default.silver_post_creator s
#   INNER JOIN default.users_yt u ON s.yt_user = u.user_id
#   WHERE s.published_at >= DATE_SUB(current_date(), 730)
# )
# WHERE rank <= 3
# ORDER BY user_id, rank;

recent_posts_views = joined_data.filter(
    (F.col("published_at") >= data_limite) & 
    (F.col("user_id").isNotNull())
)

window_views = Window.partitionBy("user_id").orderBy(F.col("views").desc())

top_posts_views = recent_posts_views.withColumn(
    "rank", F.row_number().over(window_views)
).filter(
    F.col("rank") <= 3
).select(
    "user_id", "title", "views", "rank"
).orderBy("user_id", "rank")

print("=== TOP 3 POSTS POR VIEWS (ÚLTIMOS 6 MESES) ===")
top_posts_views.show(50, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creators Órfãos

# COMMAND ----------

# SELECT 
#   s.yt_user,
#   COUNT(*) as post_count
# FROM default.silver_post_creator s
# LEFT ANTI JOIN default.users_yt u ON s.yt_user = u.user_id
# WHERE s.yt_user IS NOT NULL
# GROUP BY s.yt_user
# ORDER BY s.yt_user;

orphan_users = silver_posts_creator.select("yt_user").distinct().join(
    users_yt.select("user_id"),
    silver_posts_creator.yt_user == users_yt.user_id,
    "left_anti"
).filter(
    F.col("yt_user").isNotNull()
)

orphan_with_counts = orphan_users.join(
    silver_posts_creator.groupBy("yt_user").count().withColumnRenamed("count", "post_count"),
    "yt_user"
).orderBy("yt_user")

print("=== CREATORS ÓRFÃOS ===")
print("'yt_users' que estão na tabela silver_posts_creator mas não estão na tabela users_yt:")
orphan_with_counts.show(50, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Publicações por Mês de Cada Creator

# COMMAND ----------

# SELECT 
#   user_id,
#   SUM(post_count) as total_posts,
#   AVG(post_count) as avg_monthly_posts,
#   MAX(post_count) as max_monthly_posts
# FROM (
#   SELECT 
#     u.user_id,
#     DATE_FORMAT(s.published_at, 'yyyy-MM') as year_month,
#     COUNT(*) as post_count
#   FROM default.silver_post_creator s
#   INNER JOIN default.users_yt u ON s.yt_user = u.user_id
#   GROUP BY u.user_id, DATE_FORMAT(s.published_at, 'yyyy-MM')
# )
# GROUP BY user_id
# ORDER BY total_posts DESC;

monthly_data = joined_data.filter(
    F.col("user_id").isNotNull()
).withColumn(
    "year_month", F.date_format(F.col("published_at"), "yyyy-MM")
)

monthly_counts = monthly_data.groupBy("user_id", "year_month").count()
all_months = monthly_data.select("year_month").distinct()
all_users = monthly_data.select("user_id").distinct()
all_combinations = all_users.crossJoin(all_months)
complete_monthly_data = all_combinations.join(
    monthly_counts,
    ["user_id", "year_month"],
    "left"
).fillna(0, subset=["count"])

print("=== PUBLICAÇÕES POR MÊS ===")
print("Resumo de publicações mensais por creator:")

monthly_summary = complete_monthly_data.groupBy("user_id").agg(
    F.sum("count").alias("total_posts"),
    F.avg("count").alias("media_mensal"),
    F.max("count").alias("max_mensal")
).orderBy("total_posts", ascending=False)

monthly_summary.show(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Publicações por Mês

# COMMAND ----------

complete_monthly_pandas = complete_monthly_data.toPandas()

pivot_table = complete_monthly_pandas.pivot(
    index='user_id', 
    columns='year_month', 
    values='count'
).fillna(0).astype(int)

print("=== PUBLICAÇÕES POR MÊS ===")
print(f"Tabela pivot: {len(pivot_table)} creators x {len(pivot_table.columns)} meses")
print("\nTabela pivot completa (user_id nas linhas, meses nas colunas):")

pivot_spark_df = spark.createDataFrame(
    pivot_table.reset_index().rename_axis(None, axis=1)
)

pivot_spark_df.show(20, truncate=False)
