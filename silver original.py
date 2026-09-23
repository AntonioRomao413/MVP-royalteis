# ============================================================
# TABELAS SILVER - ROYALTIES E PIB
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# Criar SparkSession
# ============================================================

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("silver_royalties_pib")
    .getOrCreate()
)


# ============================================================
# ============================================================
# TABELA SILVER - ROYALTIES
# ============================================================
# ============================================================


# ============================================================
# Caminho da tabela Bronze
# ============================================================

bronze_royalties_path = (
    "/workspaces/MVP-royalteis/data/bronze/"
    "bronze_royalties.parquet"
)


# ============================================================
# Caminho da tabela Silver
# ============================================================

silver_royalties_path = (
    "/workspaces/MVP-royalteis/data/silver/"
    "silver_royalties.parquet"
)


# ============================================================
# Ler arquivo Bronze
# ============================================================

roy = spark.read.parquet(
    bronze_royalties_path
)


# ============================================================
# Renomear colunas
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
# Criar lista de anos
# ============================================================

anos = range(2011, 2022)


# ============================================================
# Realizar o UNPIVOT
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
# Remover registros zerados
# ============================================================

roy_long = roy_long.filter(
    F.col("royalties_real") > 0
)


# ============================================================
# Ordenar resultado
# ============================================================

roy_long = roy_long.orderBy(
    "codigo_municipio",
    "ano",
)


# ============================================================
# Exibir resultado
# ============================================================

print("=== SILVER ROYALTIES ===")

roy_long.show(
    5,
    truncate=False,
)

roy_long.printSchema()


# ============================================================
# Salvar tabela Silver - Royalties
# ============================================================

roy_long.write \
    .mode("overwrite") \
    .parquet(silver_royalties_path)

print(
    f"Silver Royalties salva em: "
    f"{silver_royalties_path}"
)


# ============================================================
# ============================================================
# TABELA SILVER - PIB
# ============================================================
# ============================================================


# ============================================================
# Caminho da tabela Bronze
# ============================================================

bronze_pib_path = (
    "/workspaces/MVP-royalteis/data/bronze/"
    "bronze_pib.parquet"
)


# ============================================================
# Caminho da tabela Silver
# ============================================================

silver_pib_path = (
    "/workspaces/MVP-royalteis/data/silver/"
    "silver_pib.parquet"
)


# ============================================================
# Ler arquivo Bronze
# ============================================================

pib_raw = spark.read.parquet(
    bronze_pib_path
)


# ============================================================
# Filtrar municípios
# ============================================================

pib = pib_raw.filter(
    F.col("Nível") == "MU"
)


# ============================================================
# Selecionar e renomear colunas
# ============================================================

pib = pib.select(
    F.col("`Cód.`")
    .cast("long")
    .alias("codigo_municipio"),

    F.col("Município")
    .alias("municipio"),

    *[
        F.col(f"`{ano}`")
        .cast("double")
        .alias(f"pib_{ano}")
        for ano in range(2011, 2022)
    ],
)


# ============================================================
# Transformar PIB para formato longo
# ============================================================

pib_long = (
    pib
    .select(
        "codigo_municipio",
        "municipio",

        F.explode(
            F.array(
                *[
                    F.struct(
                        F.lit(ano).alias("ano"),

                        F.col(f"pib_{ano}")
                        .cast("double")
                        .alias("pib_reais"),
                    )
                    for ano in range(2011, 2022)
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
# Remover PIB nulo
# ============================================================

pib_long = pib_long.filter(
    F.col("pib_reais").isNotNull()
)


# ============================================================
# Ordenar resultado
# ============================================================

pib_long = pib_long.orderBy(
    "codigo_municipio",
    "ano",
)


# ============================================================
# Exibir resultado
# ============================================================

print("=== SILVER PIB ===")

pib_long.show(
    5,
    truncate=False,
)

pib_long.printSchema()


# ============================================================
# Salvar tabela Silver - PIB
# ============================================================

pib_long.write \
    .mode("overwrite") \
    .parquet(silver_pib_path)

print(
    f"Silver PIB salva em: "
    f"{silver_pib_path}"
)


# ============================================================
# Finalização
# ============================================================

print("\n==========================================")
print("TABELAS SILVER SALVAS COM SUCESSO")
print("==========================================")
print(f"Royalties: {silver_royalties_path}")
print(f"PIB:       {silver_pib_path}")
