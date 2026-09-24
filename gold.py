# ============================================================
# TABELA GOLD 
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("Gold Royalties")
    .getOrCreate()
)

# ============================================================
# Lendo tabelas Silver
# ============================================================

roy = spark.read.parquet(
    "/workspaces/MVP-royalteis/data/silver/silver_royalties.parquet"
)

pib = spark.read.parquet(
    "/workspaces/MVP-royalteis/data/silver/silver_pib.parquet"
)

# ============================================================
# Realizando JOIN
# ============================================================

gold = roy.join(
    pib.drop("municipio"),
    on=["codigo_municipio", "ano"],
    how="inner"
)

# ============================================================
# Criando métrica de dependência — NUMÉRICA
# ============================================================

gold = gold.withColumn(
    "dependencia_royalties_pct",
    F.when(
        F.col("pib_reais") > 0,
        F.round(
            (
                F.col("royalties_real") /
                F.col("pib_reais")
            ) * 100,
            2
        ).cast("double")
    ).otherwise(
        F.lit(None).cast("double")
    )
)

# ============================================================
# Filtro para remover registros com royalties zerados
# ============================================================

gold_filtered = gold.filter(
    F.col("royalties_real") > 0
)

# ============================================================
# Exibir resultado
# ============================================================

print("=== GOLD FINAL ===")

gold_filtered.select(
    "municipio",
    "ano",
    "royalties_real",
    "pib_reais",
    "dependencia_royalties_pct"
).show(
    10,
    truncate=False
)

# ============================================================
# Salvar Gold como PARQUET
# ============================================================

gold_filtered.write \
    .mode("overwrite") \
    .parquet(
        "/workspaces/MVP-royalteis/data/gold/gold_dependencia_anual.parquet"
    )

# ============================================================
# Verificar schema
# ============================================================

print("=== SCHEMA GOLD ===")

gold_filtered.printSchema()

print("=== GOLD SALVA COM SUCESSO ===")
