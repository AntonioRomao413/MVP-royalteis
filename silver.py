# ============================================================
# TABELA SILVER - ROYALTIES
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ============================================================
#   Criar SparkSession
# ============================================================

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("silver_royalties")
    .getOrCreate()
)


# ============================================================
#   Caminho da tabela Bronze
# ============================================================

bronze_path = (
    "/workspaces/MVP-royalteis/data/bronze/"
    "bronze_royalties.parquet"
)


# ============================================================
#    Ler arquivo Bronze
# ============================================================

roy = spark.read.parquet(bronze_path)


# ============================================================
#    Renomear colunas
# ============================================================

roy = (
    roy
    .withColumnRenamed(
        "Código_MU",
        "codigo_municipio",
    )
    .withColumnRenamed(
        "MUNICÍPIOS",
        "municipio",
    )
)


# ============================================================
#   Criar lista de anos
# ============================================================

anos = range(2011, 2022)


# ============================================================
#   Realizar o UNPIVOT
# ============================================================

roy_long = (
    roy
    .select(
        "codigo_municipio",
        "municipio",
        F.explode(
            F.array(
                *[
                    F.struct(
                        F.lit(ano).alias("ano"),
                        F.col(
                            f"`TOTAL_{ano}`"
                        )
                        .cast("double")
                        .alias("royalties"),
                        F.round(
                            F.col(
                                f"`TOTAL_{ano}`"
                            )
                            .cast("double")
                            / 1000,
                            2,
                        ).alias("royalties_real"),
                    )
                    for ano in anos
                ]
            )
        ).alias("dados"),
    )
    .select(
        "codigo_municipio",
        "municipio",
        "dados.*",
    )
)


# ============================================================
#    Remover registros zerados
# ============================================================

roy_long = roy_long.filter(
    F.col("royalties_real") > 0
)


# ============================================================
#   Ordenar resultado
# ============================================================

roy_long = roy_long.orderBy(
    "codigo_municipio",
    "ano",
)

# ============================================================
#   Exibir resultado
# ============================================================

roy_long.show(
    5,
    truncate=False,
)

# ============================================================
#    Exibir estrutura
# ============================================================

roy_long.printSchema()

# ============================================================
#    Criando Silver PIB - Salvar em Parquet
# ============================================================

pib_raw = spark.table("mvp.royalties_mvp.bronze_pib")

pib = pib_raw.filter(F.col("Nível") == "MU")

pib = pib.select(
    F.col("`Cód.`").cast("long").alias("codigo_municipio"),
    F.col("Município").alias("municipio"),
    *[
        F.col(f"`{ano}`").cast("double").alias(f"pib_{ano}")
        for ano in range(2011, 2022)
    ],
)
#============================================================
# Transformar PIB para formato longo
#============================================================

pib_long = pib.select(
    "codigo_municipio",
    "municipio",
    F.explode(
        F.array(
            *[
                F.struct(
                    F.lit(ano).alias("ano"),
                    F.col(f"pib_{ano}").cast("double").alias("pib_reais"),
                )
                for ano in range(2011, 2022)
            ]
        )
    ).alias("dados"),
).select("codigo_municipio", "municipio", "dados.*")

#============================================================
# Salvando tabela Silver
#============================================================

pib_long.write.mode("overwrite").option("overwriteSchema", "true").format(
    "delta"
).saveAsTable("mvp.royalties_mvp.silver_pib")