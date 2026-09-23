# ============================================================
# Ingestão de dados de royalties e PIB do RJ
# ============================================================

# conda install -c conda-forge fastparquet
# pip install fastparquet
# pip install git+https://github.com/dask/fastparquet
# pip install pyarrow
# pip install openpyxl
# pip install pandas
# python -m venv venv
# pip install pyspark
# pip install notebook findspark

import pandas as pd
from pyspark.sql import SparkSession
from pathlib import Path

# ============================================================
# Inicializar Spark Session
# ============================================================

spark = SparkSession.builder.appName("RoyaltiesPIB").getOrCreate()

royalties_pd = pd.read_excel(
    "/workspaces/MVP-royalteis/dados/royalties_rj.xlsx"
)

pib_pd = pd.read_excel(
    "/workspaces/MVP-royalteis/dados/pib_rj.xlsx"
)

#print(pib_pd.head())
#print(royalties_pd.head())


# Normalizar nomes das colunas
royalties_pd.columns = (
    royalties_pd.columns.astype(str)
    .str.strip()
    .str.replace(" ", "_", regex=False)
)

pib_pd.columns = (
    pib_pd.columns.astype(str)
    .str.strip()
    .str.replace(" ", "_", regex=False)
)

print(pib_pd.head())

# Converter Pandas DataFrame para Spark DataFrame
royalties_spark = spark.createDataFrame(royalties_pd)
pib_spark = spark.createDataFrame(pib_pd)


# ============================================================
#  Criar diretório de saída
# ============================================================

output_dir = Path("data/bronze")
output_dir.mkdir(parents=True, exist_ok=True)


# ============================================================
#  Salvar os DataFrames em Parquet
# ============================================================

royalties_pd.to_parquet(
    output_dir / "bronze_royalties.parquet",
    index=False
)

pib_pd.to_parquet(
    output_dir / "bronze_pib.parquet",
    index=False
)


# ============================================================
#   Conferir resultado
# ============================================================

print("Arquivos gravados com sucesso!")
print(f"Royalties: {output_dir / 'bronze_royalties.parquet'}")
print(f"PIB:       {output_dir / 'bronze_pib.parquet'}")
