# ============================================================
# DASHBOARD DE DEPENDÊNCIA DE ROYALTIES
# ============================================================
#
# EXECUTAR COM:
#
# streamlit run silver.py --server.address 0.0.0.0 --server.port 8501
#
# NÃO executar com:
#
# python silver.py
#
# ============================================================


# ============================================================
# 1. BIBLIOTECAS
# ============================================================

import streamlit as st
import pandas as pd
import plotly.express as px

from pyspark.sql import SparkSession


# ============================================================
# 2. CONFIGURAÇÃO DO STREAMLIT
# ============================================================

st.set_page_config(
    page_title="Dependência de Royalties",
    page_icon="📊",
    layout="wide"
)


# ============================================================
# 3. TÍTULO
# ============================================================

st.title("📊 Ranking de Dependência de Royalties")

st.markdown(
    """
    ### Municípios mais e menos dependentes de royalties

    Utilize o seletor de ano e os botões abaixo para analisar
    os municípios.
    """
)


# ============================================================
# 4. CAMINHO DA TABELA GOLD
# ============================================================

CAMINHO_GOLD = (
    "/workspaces/MVP-royalteis/data/gold/"
    "gold_dependencia_anual.parquet"
)


# ============================================================
# 5. CRIAR SPARK
# ============================================================

@st.cache_resource
def criar_spark():

    spark = (
        SparkSession.builder
        .appName("DashboardDependenciaRoyalties")
        .master("local[*]")
        .config(
            "spark.driver.bindAddress",
            "127.0.0.1"
        )
        .getOrCreate()
    )

    return spark


# ============================================================
# 6. CARREGAR DADOS
# ============================================================

@st.cache_data
def carregar_dados():

    spark = criar_spark()

    df_spark = spark.read.parquet(
        CAMINHO_GOLD
    )

    df = df_spark.toPandas()

    return df


# ============================================================
# 7. TENTAR CARREGAR OS DADOS
# ============================================================

try:

    gold = carregar_dados()

except Exception as erro:

    st.error(
        "Não foi possível carregar a tabela Gold."
    )

    st.code(
        str(erro)
    )

    st.stop()


# ============================================================
# 8. VERIFICAR SE A TABELA ESTÁ VAZIA
# ============================================================

if gold.empty:

    st.error(
        "A tabela Gold está vazia."
    )

    st.stop()


# ============================================================
# 9. VERIFICAR COLUNAS
# ============================================================

colunas_necessarias = [

    "municipio",

    "royalties_real",

    "pib_reais",

    "ano",

    "dependencia_royalties_pct"
]


colunas_faltantes = [

    coluna
    for coluna in colunas_necessarias
    if coluna not in gold.columns
]


if colunas_faltantes:

    st.error(
        "As seguintes colunas não foram encontradas:"
    )

    st.write(
        colunas_faltantes
    )

    st.write(
        "Colunas encontradas:"
    )

    st.write(
        gold.columns.tolist()
    )

    st.stop()


# ============================================================
# 10. PREPARAR A COLUNA ANO
# ============================================================

gold["ano"] = pd.to_numeric(
    gold["ano"],
    errors="coerce"
)


# ============================================================
# 11. PREPARAR A DEPENDÊNCIA
# ============================================================

gold["dependencia_royalties_pct"] = pd.to_numeric(
    gold["dependencia_royalties_pct"],
    errors="coerce"
)


# ============================================================
# 12. REMOVER DADOS INVÁLIDOS
# ============================================================

gold = gold.dropna(
    subset=[
        "ano",
        "municipio",
        "dependencia_royalties_pct"
    ]
)


# ============================================================
# 13. CONVERTER ANO PARA INTEIRO
# ============================================================

gold["ano"] = gold["ano"].astype(int)


# ============================================================
# 14. LISTA DE ANOS
# ============================================================

anos = sorted(
    gold["ano"]
    .unique()
    .tolist()
)


if not anos:

    st.error(
        "Nenhum ano foi encontrado na tabela."
    )

    st.stop()


# ============================================================
# 15. PAINEL DE CONTROLE
# ============================================================

st.divider()

st.subheader("🎛️ Controles")


col_ano, col_info = st.columns(
    [1, 3]
)


# ============================================================
# 16. SELETOR DE ANO
# ============================================================

with col_ano:

    if 2021 in anos:

        indice_inicial = anos.index(
            2021
        )

    else:

        indice_inicial = len(anos) - 1


    ano = st.selectbox(

        "📅 Selecione o ano",

        options=anos,

        index=indice_inicial
    )


# ============================================================
# 17. INFORMAÇÃO SOBRE O ANO
# ============================================================

with col_info:

    st.info(
        f"Você está analisando o ano **{ano}**."
    )


# ============================================================
# 18. FILTRAR PELO ANO
# ============================================================

dados_ano = gold[
    gold["ano"] == ano
].copy()


if dados_ano.empty:

    st.warning(
        f"Não existem dados para o ano {ano}."
    )

    st.stop()


# ============================================================
# 19. CRIAR OS DOIS RANKINGS
# ============================================================

mais_dependentes = (

    dados_ano

    .sort_values(
        by="dependencia_royalties_pct",
        ascending=False
    )

    .head(5)

    .copy()
)


menos_dependentes = (

    dados_ano

    .sort_values(
        by="dependencia_royalties_pct",
        ascending=True
    )

    .head(5)

    .copy()
)


# ============================================================
# 20. ESTADO DO BOTÃO
# ============================================================

if "tipo_ranking" not in st.session_state:

    st.session_state.tipo_ranking = "mais"


# ============================================================
# 21. BOTÕES
# ============================================================

col_mais, col_menos = st.columns(
    2
)


with col_mais:

    if st.button(
        "🔴 5 MAIS DEPENDENTES",
        width="stretch"
    ):

        st.session_state.tipo_ranking = "mais"


with col_menos:

    if st.button(
        "🟢 5 MENOS DEPENDENTES",
        width="stretch"
    ):

        st.session_state.tipo_ranking = "menos"


# ============================================================
# 22. DEFINIR RANKING
# ============================================================

if st.session_state.tipo_ranking == "mais":

    dados_resultado = (
        mais_dependentes.copy()
    )

    titulo = (
        f"🔴 5 Municípios Mais Dependentes "
        f"de Royalties — {ano}"
    )

    cor = "#d62728"

else:

    dados_resultado = (
        menos_dependentes.copy()
    )

    titulo = (
        f"🟢 5 Municípios Menos Dependentes "
        f"de Royalties — {ano}"
    )

    cor = "#2ca02c"


# ============================================================
# 23. TÍTULO
# ============================================================

st.divider()

st.subheader(
    titulo
)


# ============================================================
# 24. PREPARAR GRÁFICO
# ============================================================

grafico = (
    dados_resultado
    .sort_values(
        "dependencia_royalties_pct",
        ascending=True
    )
)


# ============================================================
# 25. CRIAR GRÁFICO
# ============================================================

fig = px.bar(

    grafico,

    x="dependencia_royalties_pct",

    y="municipio",

    orientation="h",

    text="dependencia_royalties_pct",

    color_discrete_sequence=[
        cor
    ],

    labels={

        "municipio":
            "Município",

        "dependencia_royalties_pct":
            "Dependência de Royalties (%)"
    }
)


# ============================================================
# 26. CONFIGURAR BARRAS
# ============================================================

fig.update_traces(

    texttemplate=(
        "%{text:.2f}%"
    ),

    textposition="outside",

    hovertemplate=(
        "<b>%{y}</b><br>"
        "Dependência: %{x:.2f}%"
        "<extra></extra>"
    )
)


# ============================================================
# 27. CONFIGURAR LAYOUT
# ============================================================

fig.update_layout(

    height=500,

    template="plotly_white",

    xaxis_title=(
        "Dependência de Royalties (%)"
    ),

    yaxis_title=(
        "Município"
    ),

    margin=dict(
        l=160,
        r=100,
        t=60,
        b=60
    )
)


# ============================================================
# 28. MOSTRAR GRÁFICO
# ============================================================

st.plotly_chart(

    fig,

    width="stretch"
)


# ============================================================
# 29. TABELA
# ============================================================

st.subheader(
    "📋 Dados dos municípios"
)


# ============================================================
# 30. PREPARAR TABELA
# ============================================================

tabela = dados_resultado[
    [
        "municipio",
        "royalties_real",
        "pib_reais",
        "ano",
        "dependencia_royalties_pct"
    ]
].copy()


# ============================================================
# 31. ORDENAR TABELA
# ============================================================

if st.session_state.tipo_ranking == "mais":

    tabela = tabela.sort_values(
        "dependencia_royalties_pct",
        ascending=False
    )

else:

    tabela = tabela.sort_values(
        "dependencia_royalties_pct",
        ascending=True
    )


# ============================================================
# 32. RENOMEAR COLUNAS
# ============================================================

tabela = tabela.rename(

    columns={

        "municipio":
            "Município",

        "royalties_real":
            "Royalties (R$)",

        "pib_reais":
            "PIB (R$)",

        "ano":
            "Ano",

        "dependencia_royalties_pct":
            "Dependência (%)"
    }
)


# ============================================================
# 33. FORMATAÇÃO DOS VALORES
# ============================================================

def formatar_reais(valor):

    if pd.isna(valor):

        return "-"

    return (
        f"R$ {valor:,.2f}"
        .replace(",", "X")
        .replace(".", ",")
        .replace("X", ".")
    )


def formatar_percentual(valor):

    if pd.isna(valor):

        return "-"

    return (
        f"{valor:.2f}%"
    )


# ============================================================
# 34. FORMATAR REAIS
# ============================================================

tabela["Royalties (R$)"] = (
    tabela["Royalties (R$)"]
    .apply(formatar_reais)
)


tabela["PIB (R$)"] = (
    tabela["PIB (R$)"]
    .apply(formatar_reais)
)


# ============================================================
# 35. FORMATAR PERCENTUAL
# ============================================================

tabela["Dependência (%)"] = (
    tabela["Dependência (%)"]
    .apply(formatar_percentual)
)


# ============================================================
# 36. MOSTRAR TABELA
# ============================================================

st.dataframe(

    tabela,

    width="stretch",

    hide_index=True
)


# ============================================================
# 37. RESUMO
# ============================================================

st.divider()

col_a, col_b, col_c = st.columns(
    3
)


with col_a:

    st.metric(
        "Ano",
        ano
    )


with col_b:

    st.metric(
        "Municípios analisados",
        len(dados_ano)
    )


with col_c:

    st.metric(
        "Ranking exibido",
        "5 Mais"
        if st.session_state.tipo_ranking == "mais"
        else "5 Menos"
    )


# ============================================================
# 38. RODAPÉ
# ============================================================

st.divider()

st.caption(
    "Fonte dos dados:SIDRA IBGE, ANP"
    )

st.caption(
    "Dashboard desenvolvido com "
    "Streamlit + PySpark + Plotly."
)

## utilizar Ctrl + C para encerrar o servidor do Streamlit no terminal
## /workspaces/MVP-royalteis/.venv/bin/streamlit run /workspaces/MVP-royalteis/silver.py --server.address 0.0.0.0 --server.port 8501
## link do dashboard: http://localhost:8501 - depois abrir no navegador. http://51.8.152.69:8501