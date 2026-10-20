# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver : typage, normalisation, aplatissement, déduplication
# MAGIC Entrée : bronze. Sorties : `silver_marche` (un marché) et `silver_marche_titulaire` (un titulaire d'un marché).
# MAGIC Les choix sont justifiés dans `docs/DECISIONS.md`. Aucun drapeau qualité n'est calculé ici (voir 03).

# COMMAND ----------

import os, unicodedata
from pyspark.sql import SparkSession, Window, functions as F

IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ
if IS_DATABRICKS:
    BRONZE, T_MARCHE, T_TIT = "main.decp.bronze_decp", "main.decp.silver_marche", "main.decp.silver_marche_titulaire"
else:
    BRONZE, T_MARCHE, T_TIT = "data/bronze/decp", "data/silver/marche", "data/silver/marche_titulaire"

spark = SparkSession.builder.appName("silver_decp").config("spark.ui.enabled", "false").getOrCreate()
bronze = spark.table(BRONZE) if IS_DATABRICKS else spark.read.parquet(BRONZE)

# COMMAND ----------
# MAGIC %md ## 1. Attributions initiales uniquement

# COMMAND ----------

df = bronze.filter(F.col("modification_id") == 0)
n_init = df.count()

# COMMAND ----------
# MAGIC %md ## 2. Normalisation (texte, types d'identifiant, nature)

# COMMAND ----------

def strip_accents(c):
    # translate suffit pour le français et évite un UDF
    return F.translate(c, "éèêëàâäîïôöùûüç", "eeeeaaaiioouuuc")

for c in ["id", "acheteur_id", "titulaire_id", "objet", "codeCPV"]:
    df = df.withColumn(c, F.nullif(F.trim(F.col(c)), F.lit("")))

# Types d'identifiant : SIRET / siret / Siret -> SIRET ; HORS-UE / HORS UE -> HORS_UE
t = F.upper(F.regexp_replace(F.trim(F.col("titulaire_typeIdentifiant")), r"[\s-]+", "_"))
df = df.withColumn("titulaire_typeIdentifiant", F.nullif(t, F.lit("")))

# Nature : majuscules sans accents, "ACCORD CADRE" -> "ACCORD-CADRE"
n = F.upper(strip_accents(F.lower(F.trim(F.col("nature")))))
n = F.regexp_replace(n, r"^ACCORD CADRE$", "ACCORD-CADRE")
df = df.withColumn("nature", n)

# Identifiant titulaire : on retire espaces/points des SIRET (le contrôle de validité vient en 03)
df = df.withColumn(
    "titulaire_id",
    F.when(F.col("titulaire_typeIdentifiant") == "SIRET", F.regexp_replace("titulaire_id", r"[\s.]", ""))
    .otherwise(F.col("titulaire_id")),
)

# COMMAND ----------
# MAGIC %md ## 3. Déduplication
# MAGIC Doublon = même acheteur + même id + même titulaire + même CPV + même montant + même date de notification.
# MAGIC (L'`id` seul n'est pas unique : un acheteur le réutilise pour des marchés différents.)
# MAGIC On garde la ligne la plus récemment publiée, puis la mieux renseignée.

# COMMAND ----------

KEY = ["acheteur_id", "id", "titulaire_id", "codeCPV", "montant", "dateNotification"]
w = Window.partitionBy(*KEY).orderBy(
    F.col("datePublicationDonnees").desc_nulls_last(), F.col("sourceDataset").asc_nulls_last()
)
dedup = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
n_dedup = dedup.count()

# Identifiant technique de marché (acheteur + id + date + CPV)
dedup = dedup.withColumn(
    "marche_id",
    F.sha2(F.concat_ws("|", "acheteur_id", "id", F.col("dateNotification").cast("string"), F.coalesce("codeCPV", F.lit(""))), 256),
)

# COMMAND ----------
# MAGIC %md ## 4. Aplatissement : marche / marche_titulaire

# COMMAND ----------

TIT_COLS = ["titulaire_id", "titulaire_typeIdentifiant", "titulaire_nom"]
market_cols = [c for c in dedup.columns
               if c not in TIT_COLS and not c.startswith("titulaire_") and c not in ("marche_id", "montant")]

# Montant : une ligne par titulaire, souvent le même montant répété. Quand les lignes d'un même marché
# ont des montants différents (conflit), on ne choisit PAS (ni max ni somme) : montant = NULL,
# conflit signalé, min/max conservés. Voir DECISIONS.md.
agg_m = dedup.groupBy("marche_id").agg(
    F.min("montant").alias("montant_min"),
    F.max("montant").alias("montant_max"),
    F.countDistinct("montant").alias("_n_montants"),
    F.count("*").alias("nb_lignes_titulaires"),
)
agg_m = (agg_m
         .withColumn("montant_conflit", F.col("_n_montants") > 1)
         .withColumn("montant", F.when(F.col("_n_montants") > 1, F.lit(None)).otherwise(F.col("montant_max")))
         .drop("_n_montants"))

wm = Window.partitionBy("marche_id").orderBy(F.col("datePublicationDonnees").desc_nulls_last(), F.col("sourceDataset").asc_nulls_last())
marche = (dedup.withColumn("_rn", F.row_number().over(wm)).filter("_rn = 1").drop("_rn")
          .select("marche_id", *market_cols)
          .join(agg_m, "marche_id"))

# Le montant publié pour chaque titulaire reste disponible dans marche_titulaire
tit = dedup.select("marche_id", *TIT_COLS, F.col("montant").alias("montant_ligne")).dropDuplicates()

# COMMAND ----------

if IS_DATABRICKS:
    marche.write.format("delta").mode("overwrite").saveAsTable(T_MARCHE)
    tit.write.format("delta").mode("overwrite").saveAsTable(T_TIT)
else:
    marche.write.mode("overwrite").parquet(T_MARCHE)
    tit.write.mode("overwrite").parquet(T_TIT)

print(f"bronze (modif=0): {n_init} | après dédup: {n_dedup} | marchés: {marche.count()} | marché-titulaires: {tit.count()}")
