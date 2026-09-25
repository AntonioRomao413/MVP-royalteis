# ============================================================
# TABELA GOLD — DEPENDÊNCIA DE ROYALTIES
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# CONFIGURAÇÃO
# ============================================================

CAMINHO_SILVER_ROYALTIES = (
    "/workspaces/MVP-royalteis/data/silver/"
    "silver_royalties.parquet"
)

CAMINHO_SILVER_PIB = (
    "/workspaces/MVP-royalteis/data/silver/"
    "silver_pib.parquet"
)

CAMINHO_GOLD = (
    "/workspaces/MVP-royalteis/data/gold/"
    "gold_dependencia_anual.parquet"
)


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("Gold Royalties")
    .getOrCreate()
)


# ============================================================
# LEITURA DAS TABELAS SILVER
# ============================================================

roy = (
    spark.read
    .parquet(CAMINHO_SILVER_ROYALTIES)
    .select(
        "codigo_municipio",
        "municipio",
        "ano",
        "royalties_real",
    )
)


pib = (
    spark.read
    .parquet(CAMINHO_SILVER_PIB)
    .select(
        "codigo_municipio",
        "ano",
        "pib_reais",
    )
)


# ============================================================
# PADRONIZAÇÃO DOS TIPOS
# ============================================================

roy = (
    roy
    .withColumn(
        "codigo_municipio",
        F.col("codigo_municipio").cast("string"),
    )
    .withColumn(
        "ano",
        F.col("ano").cast("integer"),
    )
    .withColumn(
        "royalties_real",
        F.col("royalties_real").cast("double"),
    )
)


pib = (
    pib
    .withColumn(
        "codigo_municipio",
        F.col("codigo_municipio").cast("string"),
    )
    .withColumn(
        "ano",
        F.col("ano").cast("integer"),
    )
    .withColumn(
        "pib_reais",
        F.col("pib_reais").cast("double"),
    )
)


# ============================================================
# REMOVER REGISTROS INVÁLIDOS
# ============================================================

roy = roy.filter(
    F.col("codigo_municipio").isNotNull()
    & F.col("ano").isNotNull()
    & F.col("municipio").isNotNull()
    & F.col("royalties_real").isNotNull()
)


pib = pib.filter(
    F.col("codigo_municipio").isNotNull()
    & F.col("ano").isNotNull()
    & F.col("pib_reais").isNotNull()
)


# ============================================================
# JOIN
# ============================================================

gold = (
    roy
    .join(
        pib,
        on=[
            "codigo_municipio",
            "ano",
        ],
        how="inner",
    )
)


# ============================================================
# DEPENDÊNCIA DE ROYALTIES
#
# royalties / PIB * 100
# ============================================================

gold = gold.withColumn(
    "dependencia_royalties_pct",
    F.when(
        F.col("pib_reais") > 0,
        F.round(
            (
                F.col("royalties_real")
                / F.col("pib_reais")
            ) * 100,
            2,
        ).cast("double"),
    ).otherwise(
        F.lit(None).cast("double")
    ),
)


# ============================================================
# FILTROS FINAIS
# ============================================================

gold_filtered = (
    gold
    .filter(
        F.col("royalties_real") > 0
    )
    .filter(
        F.col("pib_reais") > 0
    )
    .filter(
        F.col("dependencia_royalties_pct").isNotNull()
    )
)


# ============================================================
# SELECIONAR COLUNAS FINAIS
# ============================================================

gold_final = gold_filtered.select(
    "codigo_municipio",
    "municipio",
    "ano",
    "royalties_real",
    "pib_reais",
    "dependencia_royalties_pct",
)


# ============================================================
# ORDENAR PARA VISUALIZAÇÃO
# ============================================================

gold_final = gold_final.orderBy(
    "ano",
    "municipio",
)


# ============================================================
# EXIBIR RESULTADO
# ============================================================

print("=== GOLD FINAL ===")

gold_final.show(
    10,
    truncate=False,
)


# ============================================================
# SCHEMA
# ============================================================

print("=== SCHEMA GOLD ===")

gold_final.printSchema()


# ============================================================
# ESTATÍSTICAS
# ============================================================

print("=== ESTATÍSTICAS ===")

gold_final.select(
    F.count("*").alias("registros"),
    F.countDistinct(
        "codigo_municipio"
    ).alias("municipios"),
    F.min("ano").alias("ano_inicial"),
    F.max("ano").alias("ano_final"),
).show()


# ============================================================
# SALVAR GOLD
# ============================================================

(
    gold_final
    .write
    .mode("overwrite")
    .parquet(CAMINHO_GOLD)
)


# ============================================================
# CONFIRMAÇÃO
# ============================================================

print(
    "=== GOLD SALVA COM SUCESSO ==="
)

print(
    f"Caminho: {CAMINHO_GOLD}"
)


# ============================================================
# FINALIZAR SPARK
# ============================================================

spark.stop()