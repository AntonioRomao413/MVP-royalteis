# ============================================================
# Ranking dos 10 municípios Mais Dependentes 2021
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ============================================================
#  riar SparkSession
# ============================================================


spark = (
    SparkSession.builder
    .appName("RankingDependenciaRoyalties")
    .getOrCreate()
)

# ============================================================
#  endo Tabela Gold
# ============================================================


gold = spark.read.parquet(
    "/workspaces/MVP-royalteis/data/gold/gold_dependencia_anual.parquet"
)

# ============================================================
#    Ordena os municípios pela dependência de royalties
#    do maior para o menor percentual
# ============================================================

window = Window.orderBy(
    F.col("dependencia_royalties_pct").desc()
)

# ============================================================
#   Filtragem do ano de 2021 e criação do ranking
# ============================================================

ranking_2021 = (
    gold
    .filter(F.col("ano") == 2021)
    .withColumn("ranking", F.row_number().over(window))
)

# ============================================================
#   Seleção das informações do ranking
#   Exibição dos 10 municípios mais dependente
# ============================================================

ranking_2021.select(
    "ranking",
    "municipio",
    "royalties_real",
    "pib_reais",
    "dependencia_royalties_pct",
).limit(10).show(truncate=False)
