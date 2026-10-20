"""Préfiltrage local de Sirene (DuckDB) sur les SIRET présents dans les DECP.

Sorties (dans data/, jamais commitées) :
- sirene_extract.parquet : établissements diffusibles correspondant aux SIRET des titulaires, colonnes utiles seulement
- sirene_non_diffusible.parquet : uniquement les SIRET (sans autre info) des établissements non diffusibles trouvés,
  pour pouvoir expliquer leur absence de l'extrait lors de la réconciliation.
"""
import argparse
import duckdb

USEFUL_COLS = [
    "siret", "siren", "statutDiffusionEtablissement", "etatAdministratifEtablissement",
    "dateCreationEtablissement", "dateDebut", "etablissementSiege",
    "activitePrincipaleEtablissement", "trancheEffectifsEtablissement",
    "caractereEmployeurEtablissement", "codePostalEtablissement", "codeCommuneEtablissement",
]
# Pas de nom, enseigne ni adresse de voie : minimise les données personnelles (entrepreneurs individuels).


def extract(sirene_path: str, titulaires_glob: str, out_dir: str) -> dict:
    con = duckdb.connect()
    con.sql(f"""
        create temp table wanted as
        select distinct titulaire_id as siret
        from '{titulaires_glob}'
        where titulaire_typeIdentifiant = 'SIRET' and regexp_matches(titulaire_id, '^[0-9]{{14}}$')
    """)
    cols = ", ".join(f"s.{c}" for c in USEFUL_COLS)
    con.sql(f"""
        create temp table matched as
        select {cols} from '{sirene_path}' s join wanted w using (siret)
    """)
    # 'O' = diffusible ; tout autre statut (N) = non diffusible, exclu de l'extrait
    con.sql(f"copy (select * from matched where statutDiffusionEtablissement = 'O') to '{out_dir}/sirene_extract.parquet' (format parquet)")
    con.sql(f"copy (select siret from matched where statutDiffusionEtablissement <> 'O') to '{out_dir}/sirene_non_diffusible.parquet' (format parquet)")
    return {
        "siret_demandes": con.sql("select count(*) from wanted").fetchone()[0],
        "trouves": con.sql("select count(*) from matched").fetchone()[0],
        "diffusibles_extraits": con.sql("select count(*) from matched where statutDiffusionEtablissement = 'O'").fetchone()[0],
        "non_diffusibles_exclus": con.sql("select count(*) from matched where statutDiffusionEtablissement <> 'O'").fetchone()[0],
    }


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--sirene", default="data/sirene_etablissement.parquet")
    p.add_argument("--titulaires", default="data/silver/marche_titulaire/*.parquet")
    p.add_argument("--out", default="data")
    a = p.parse_args()
    print(extract(a.sirene, a.titulaires, a.out))
