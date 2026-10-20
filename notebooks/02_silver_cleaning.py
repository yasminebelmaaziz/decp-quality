# Databricks notebook source
# MAGIC %md
# MAGIC # 02 silver : nettoyage, dedup, tables marche et marche_titulaire

# COMMAND ----------

import os
import sys
from pyspark.sql import SparkSession, Window, functions as F

# git folder databricks : cwd = dossier du notebook, donc ../src
sys.path.append(os.path.abspath("../src"))
sys.path.append(os.path.abspath("src"))
from decp_quality import config

on_dbx = "DATABRICKS_RUNTIME_VERSION" in os.environ
if on_dbx:
    src = config.table("bronze_decp")
    out_marche, out_tit = config.table("silver_marche"), config.table("silver_marche_titulaire")
else:
    src, out_marche, out_tit = "data/bronze/decp", "data/silver/marche", "data/silver/marche_titulaire"

spark = SparkSession.builder.appName("silver").config("spark.ui.enabled", "false").getOrCreate()
bronze = spark.table(src) if on_dbx else spark.read.parquet(src)

# COMMAND ----------

#attributions init seulement
df = bronze.filter(F.col("modification_id") == 0)
n_init = df.count()

#texte vide -> null
for c in ["id", "acheteur_id", "titulaire_id", "objet", "codeCPV"]:
    df = df.withColumn(c, F.nullif(F.trim(F.col(c)), F.lit("")))

#type id : Siret, siret -> SIRET / HORS-UE -> HORS_UE
typ = F.upper(F.regexp_replace(F.trim(F.col("titulaire_typeIdentifiant")), r"[\s-]+", "_"))
df = df.withColumn("titulaire_typeIdentifiant", F.nullif(typ, F.lit("")))

# nature en majuscules sans accents
nat = F.upper(F.translate(F.lower(F.trim(F.col("nature"))), "éèêëàâäîïôöùûüç", "eeeeaaaiioouuuc"))
df = df.withColumn("nature", F.regexp_replace(nat, r"^ACCORD CADRE$", "ACCORD-CADRE"))

# espaces et points dans les siret
df = df.withColumn("titulaire_id",
    F.when(F.col("titulaire_typeIdentifiant") == "SIRET", F.regexp_replace("titulaire_id", r"[\s.]", ""))
    .otherwise(F.col("titulaire_id")))

# COMMAND ----------

# doublon = meme acheteur, id, titulaire, cpv, montant, date notif
key = ["acheteur_id", "id", "titulaire_id", "codeCPV", "montant", "dateNotification"]
w = Window.partitionBy(*key).orderBy(F.col("datePublicationDonnees").desc_nulls_last(), F.col("sourceDataset").asc_nulls_last())
dedup = df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
n_dedup = dedup.count()

dedup = dedup.withColumn("marche_id", F.sha2(F.concat_ws("|", "acheteur_id", "id",
    F.col("dateNotification").cast("string"), F.coalesce("codeCPV", F.lit(""))), 256))

# COMMAND ----------

tit_cols = ["titulaire_id", "titulaire_typeIdentifiant", "titulaire_nom"]
marche_cols = [c for c in dedup.columns
    if not c.startswith("titulaire_") and c not in ("marche_id", "montant")]

# si les montants different pour un meme marche on met null et on garde min/max
agg = dedup.groupBy("marche_id").agg(
    F.min("montant").alias("montant_min"),
    F.max("montant").alias("montant_max"),
    F.countDistinct("montant").alias("n_montants"),
    F.count("*").alias("nb_lignes_titulaires"))
agg = (agg.withColumn("montant_conflit", F.col("n_montants") > 1)
    .withColumn("montant", F.when(F.col("n_montants") > 1, F.lit(None)).otherwise(F.col("montant_max")))
    .drop("n_montants"))

w2 = Window.partitionBy("marche_id").orderBy(F.col("datePublicationDonnees").desc_nulls_last(), F.col("sourceDataset").asc_nulls_last())
marche = (dedup.withColumn("rn", F.row_number().over(w2)).filter("rn = 1").drop("rn")
    .select("marche_id", *marche_cols)
    .join(agg, "marche_id"))

tit = dedup.select("marche_id", *tit_cols, F.col("montant").alias("montant_ligne")).dropDuplicates()

# COMMAND ----------

if on_dbx:
    marche.write.format("delta").mode("overwrite").saveAsTable(out_marche)
    tit.write.format("delta").mode("overwrite").saveAsTable(out_tit)
else:
    marche.write.mode("overwrite").parquet(out_marche)
    tit.write.mode("overwrite").parquet(out_tit)

print("modif=0:", n_init, "dedup:", n_dedup, "marches:", marche.count(), "marche_titulaire:", tit.count())
