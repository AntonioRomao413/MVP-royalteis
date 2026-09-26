# ============================================================
# TABELAS SILVER - ROYALTIES E PIB
# ============================================================

import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# CONFIGURAÇÃO
# ============================================================

ANOS = range(2011, 2024)

BASE_PATH = (
    "/workspaces/MVP-royalteis/data"
)

BRONZE_PATH = f"{BASE_PATH}/bronze"
SILVER_PATH = f"{BASE_PATH}/silver"


BRONZE_ROYALTIES = (
    f"{BRONZE_PATH}/bronze_royalties.parquet"
)

BRONZE_PIB = (
    f"{BRONZE_PATH}/bronze_pib.parquet"
)

SILVER_ROYALTIES = (
    f"{SILVER_PATH}/silver_royalties.parquet"
)

SILVER_PIB = (
    f"{SILVER_PATH}/silver_pib.parquet"
)


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Silver Royalties PIB")
    .getOrCreate()
)


# ============================================================
# FUNÇÃO AUXILIAR
# ============================================================

def criar_array_anos(colunas):
    """
    Cria uma estrutura para transformar dados
    de formato wide para long.
    """

    return F.array(
        *[
            F.struct(
                F.lit(ano).cast("integer").alias("ano"),
                F.col(coluna)
                .cast("double")
                .alias("valor"),
            )
            for ano, coluna in colunas
        ]
    )


# ============================================================
# ============================================================
# SILVER - ROYALTIES
# ============================================================
# ============================================================

print("\n=== PROCESSANDO ROYALTIES ===")


# ============================================================
# LEITURA BRONZE
# ============================================================

roy = (
    spark.read
    .parquet(BRONZE_ROYALTIES)
)


# ============================================================
# SELEÇÃO E PADRONIZAÇÃO
# ============================================================

roy = (
    roy
    .select(
        F.col("Código_MU")
        .cast("string")
        .alias("codigo_municipio"),

        F.col("MUNICÍPIOS")
        .cast("string")
        .alias("municipio"),

        *[
            F.col(f"`TOTAL_{ano}`")
            .cast("double")
            .alias(f"TOTAL_{ano}")
            for ano in ANOS
        ],
    )
)


# ============================================================
# TRANSFORMAÇÃO WIDE → LONG
# ============================================================

roy_long = (
    roy
    .select(
        "codigo_municipio",
        "municipio",

        F.explode(
            criar_array_anos(
                [
                    (
                        ano,
                        f"TOTAL_{ano}",
                    )
                    for ano in ANOS
                ]
            )
        ).alias("dados"),
    )
    .select(
        "codigo_municipio",
        "municipio",
        "dados.ano",
        "dados.valor",
    )
    .withColumn(
        "royalties_real",
        F.col("valor"),
    )
    .drop("valor")
)


# ============================================================
# FILTROS
# ============================================================

roy_long = (
    roy_long
    .filter(
        F.col("codigo_municipio").isNotNull()
        & F.col("municipio").isNotNull()
        & F.col("royalties_real").isNotNull()
        & (F.col("royalties_real") > 0)
    )
)


# ============================================================
# SELEÇÃO FINAL
# ============================================================

roy_long = roy_long.select(
    "codigo_municipio",
    "municipio",
    "ano",
    "royalties_real",
)


# ============================================================
# EXIBIR
# ============================================================

print("=== SILVER ROYALTIES ===")

roy_long.show(
    5,
    truncate=False,
)

roy_long.printSchema()


# ============================================================
# SALVAR
# ============================================================

(
    roy_long
    .write
    .mode("overwrite")
    .parquet(SILVER_ROYALTIES)
)

print(
    f"Silver Royalties salva em:\n"
    f"{SILVER_ROYALTIES}"
)


# ============================================================
# ============================================================
# SILVER - PIB
# ============================================================
# ============================================================

print("\n=== PROCESSANDO PIB ===")


# ============================================================
# LEITURA BRONZE
# ============================================================

pib_raw = (
    spark.read
    .parquet(BRONZE_PIB)
)


# ============================================================
# FILTRAR MUNICÍPIOS
# ============================================================

pib = (
    pib_raw
    .filter(
        F.col("Nível") == "MU"
    )
)


# ============================================================
# SELEÇÃO E PADRONIZAÇÃO
# ============================================================

pib = (
    pib
    .select(
        F.col("`Cód.`")
        .cast("string")
        .alias("codigo_municipio"),

        F.col("Município")
        .cast("string")
        .alias("municipio"),

        *[
            F.col(f"`{ano}`")
            .cast("double")
            .alias(f"PIB_{ano}")
            for ano in ANOS
        ],
    )
)


# ============================================================
# TRANSFORMAÇÃO WIDE → LONG
# ============================================================

pib_long = (
    pib
    .select(
        "codigo_municipio",
        "municipio",

        F.explode(
            criar_array_anos(
                [
                    (
                        ano,
                        f"PIB_{ano}",
                    )
                    for ano in ANOS
                ]
            )
        ).alias("dados"),
    )
    .select(
        "codigo_municipio",
        "municipio",
        "dados.ano",
        "dados.valor",
    )
    .withColumn(
        "pib_reais",
        F.round(
            F.col("valor") * 1000,
            2,
        ),
    )
    .drop("valor")
)


# ============================================================
# FILTROS
# ============================================================

pib_long = (
    pib_long
    .filter(
        F.col("codigo_municipio").isNotNull()
        & F.col("municipio").isNotNull()
        & F.col("pib_reais").isNotNull()
        & (F.col("pib_reais") > 0)
    )
)


# ============================================================
# SELEÇÃO FINAL
# ============================================================

pib_long = pib_long.select(
    "codigo_municipio",
    "municipio",
    "ano",
    "pib_reais",
)


# ============================================================
# EXIBIR
# ============================================================

print("=== SILVER PIB ===")

pib_long.show(
    5,
    truncate=False,
)

pib_long.printSchema()


# ============================================================
# SALVAR
# ============================================================

(
    pib_long
    .write
    .mode("overwrite")
    .parquet(SILVER_PIB)
)

print(
    f"Silver PIB salva em:\n"
    f"{SILVER_PIB}"
)


# ============================================================
# VALIDAÇÃO
# ============================================================

print("\n==========================================")
print("VALIDAÇÃO DAS TABELAS SILVER")
print("==========================================")


# ============================================================
# ROYALTIES
# ============================================================

print("\n--- ROYALTIES ---")

roy_long.select(
    F.count("*").alias("registros"),
    F.countDistinct(
        "codigo_municipio"
    ).alias("municipios"),
    F.min("ano").alias("ano_inicial"),
    F.max("ano").alias("ano_final"),
).show()


# ============================================================
# PIB
# ============================================================

print("--- PIB ---")

pib_long.select(
    F.count("*").alias("registros"),
    F.countDistinct(
        "codigo_municipio"
    ).alias("municipios"),
    F.min("ano").alias("ano_inicial"),
    F.max("ano").alias("ano_final"),
).show()


# ============================================================
# FINALIZAÇÃO
# ============================================================

print("\n==========================================")
print("TABELAS SILVER SALVAS COM SUCESSO")
print("==========================================")

print(
    f"Royalties: {SILVER_ROYALTIES}"
)

print(
    f"PIB:       {SILVER_PIB}"
)


# ============================================================
# FINALIZAR SPARK
# ============================================================

spark.stop()
