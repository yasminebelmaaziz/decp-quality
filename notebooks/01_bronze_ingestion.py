# Databricks notebook source
# MAGIC %md
# MAGIC # 01 bronze : chargement brut des decp

# COMMAND ----------

import os
import sys
from pyspark.sql import SparkSession, functions as F

# git folder databricks : cwd = dossier du notebook, donc ../src
sys.path.append(os.path.abspath("../src"))
sys.path.append(os.path.abspath("src"))
from decp_quality import config

year = 2024
on_dbx = "DATABRICKS_RUNTIME_VERSION" in os.environ

if on_dbx:
    src = config.decp_file
    target = config.table("bronze_decp")
else:
    src = "data/decp.parquet"
    target = "data/bronze/decp"

spark = SparkSession.builder.appName("bronze").config("spark.ui.enabled", "false").getOrCreate()

# COMMAND ----------

raw = spark.read.parquet(src)
n_raw = raw.count()

# on garde aussi les dates de notif bizarres si publiees en 2024
y_notif = F.year("dateNotification")
bad_notif = y_notif.isNull() | (y_notif < 2015) | (y_notif > F.year(F.current_date()))
keep = (y_notif == year) | (bad_notif & (F.year("datePublicationDonnees") == year))

bronze = (raw.filter(keep)
    .withColumn("_scope", F.when(y_notif == year, "notification").otherwise("notification_aberrante_publiee"))
    .withColumn("_source", F.lit(os.path.basename(src)))
    .withColumn("_ingested_at", F.current_timestamp()))

# COMMAND ----------

if on_dbx:
    bronze.write.format("delta").mode("overwrite").saveAsTable(target)
    n_bronze = spark.table(target).count()
else:
    bronze.write.mode("overwrite").parquet(target)
    n_bronze = spark.read.parquet(target).count()

print("source:", n_raw, "bronze:", n_bronze)
