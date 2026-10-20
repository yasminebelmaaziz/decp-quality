# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze : ingestion brute des DECP consolidées
# MAGIC Données chargées telles quelles (aucun nettoyage), restreintes à l'année retenue,
# MAGIC avec deux colonnes techniques : `_source` et `_ingested_at`.

# COMMAND ----------

import os
from pyspark.sql import SparkSession, functions as F

YEAR = 2024  # année retenue (voir docs/DECISIONS.md)
IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ

if IS_DATABRICKS:
    # Volume Unity Catalog : adapter catalogue / schéma à votre espace
    SOURCE_PATH = "/Volumes/main/decp/raw/decp.parquet"
    BRONZE_TARGET = "main.decp.bronze_decp"
else:
    SOURCE_PATH = "data/decp.parquet"
    BRONZE_TARGET = "data/bronze/decp"

spark = SparkSession.builder.appName("bronze_decp").config("spark.ui.enabled", "false").getOrCreate()

# COMMAND ----------

raw = spark.read.parquet(SOURCE_PATH)
n_total = raw.count()

# Périmètre : marchés notifiés en YEAR, PLUS les lignes dont la date de notification est aberrante
# (hors 2015..année courante) mais publiées en YEAR. Sans cela, ces anomalies seraient exclues
# avant même la mesure de qualité. Le reste est conservé brut (modifications, doublons : traités en silver).
notif_year = F.year("dateNotification")
notif_aberrante = notif_year.isNull() | (notif_year < 2015) | (notif_year > F.year(F.current_date()))
in_scope = (notif_year == YEAR) | (notif_aberrante & (F.year("datePublicationDonnees") == YEAR))

bronze = (
    raw.filter(in_scope)
    .withColumn("_scope", F.when(notif_year == YEAR, F.lit("notification")).otherwise(F.lit("notification_aberrante_publiee")))
    .withColumn("_source", F.lit(os.path.basename(SOURCE_PATH)))
    .withColumn("_ingested_at", F.current_timestamp())
)

# COMMAND ----------

if IS_DATABRICKS:
    bronze.write.format("delta").mode("overwrite").saveAsTable(BRONZE_TARGET)
else:
    # En local : Parquet (Delta n'est utilisé que sur Databricks)
    bronze.write.mode("overwrite").parquet(BRONZE_TARGET)

# COMMAND ----------

n_bronze = spark.read.parquet(BRONZE_TARGET).count() if not IS_DATABRICKS else spark.table(BRONZE_TARGET).count()
print(f"Lignes source : {n_total} | lignes bronze (périmètre {YEAR}) : {n_bronze}")
