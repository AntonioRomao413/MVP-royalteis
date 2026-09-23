# ============================================================
# Ranking dos 10 municípios Mais Dependentes 2021
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("RankingDependenciaRoyalties")
    .getOrCreate()
)

gold = spark.read.parquet(
    "/workspaces/MVP-royalteis/data/gold/gold_dependencia_anual.parquet"
)

window = Window.orderBy(
    F.col("dependencia_royalties_pct").desc()
)

ranking_2021 = (
    gold
    .filter(F.col("ano") == 2021)
    .withColumn("ranking", F.row_number().over(window))
)

ranking_2021.select(
    "ranking",
    "municipio",
    "royalties_real",
    "pib_reais",
    "dependencia_royalties_pct",
).limit(10).show(truncate=False)
