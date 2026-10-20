import argparse
import duckdb

# pas de nom ni adresse pour les donnees perso
cols = ["siret", "siren", "statutDiffusionEtablissement", "etatAdministratifEtablissement",
        "dateCreationEtablissement", "dateDebut", "etablissementSiege",
        "activitePrincipaleEtablissement", "trancheEffectifsEtablissement",
        "caractereEmployeurEtablissement", "codePostalEtablissement", "codeCommuneEtablissement"]


def extract(sirene, tit_path, out):
    con = duckdb.connect()
    con.sql(f"""create temp table wanted as
        select distinct titulaire_id as siret from '{tit_path}'
        where titulaire_typeIdentifiant = 'SIRET' and regexp_matches(titulaire_id, '^[0-9]{{14}}$')""")
    sel = ", ".join("s." + c for c in cols)
    con.sql(f"create temp table found as select {sel} from '{sirene}' s join wanted w using (siret)")
    # O = diffusible, le reste on exclut
    con.sql(f"copy (select * from found where statutDiffusionEtablissement = 'O') to '{out}/sirene_extract.parquet'")
    con.sql(f"copy (select siret from found where statutDiffusionEtablissement <> 'O') to '{out}/sirene_non_diffusible.parquet'")
    n = lambda q: con.sql(q).fetchone()[0]
    return {"demandes": n("select count(*) from wanted"),
            "trouves": n("select count(*) from found"),
            "diffusibles": n("select count(*) from found where statutDiffusionEtablissement = 'O'"),
            "non_diffusibles": n("select count(*) from found where statutDiffusionEtablissement <> 'O'")}


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--sirene", default="data/sirene_etablissement.parquet")
    p.add_argument("--tit", default="data/silver/marche_titulaire/*.parquet")
    p.add_argument("--out", default="data")
    a = p.parse_args()
    print(extract(a.sirene, a.tit, a.out))
