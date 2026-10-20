# noms databricks, a changer ici seulement
catalog = "workspace"
schema = "decp"
volume = "raw"

vol_path = f"/Volumes/{catalog}/{schema}/{volume}"
decp_file = f"{vol_path}/decp.parquet"
sirene_file = f"{vol_path}/sirene_extract.parquet"
sirene_nd_file = f"{vol_path}/sirene_non_diffusible.parquet"


def table(name):
    return f"{catalog}.{schema}.{name}"
