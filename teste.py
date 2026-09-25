# ============================================================
# DASHBOARD DE DEPENDÊNCIA DE ROYALTIES
# ============================================================

import streamlit as st
import pandas as pd
import plotly.express as px

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# CONFIGURAÇÃO
# ============================================================

st.set_page_config(
    page_title="Dependência de Royalties",
    page_icon="📊",
    layout="wide",
)


# ============================================================
# CONFIGURAÇÕES
# ============================================================

CAMINHO_GOLD = (
    "/workspaces/MVP-royalteis/data/gold/"
    "gold_dependencia_anual.parquet"
)

COLUNAS_RANKING = [
    "municipio",
    "royalties_real",
    "pib_reais",
    "ano",
    "dependencia_royalties_pct",
]


# ============================================================
# SPARK
# ============================================================

@st.cache_resource
def criar_spark():
    """
    Cria uma única sessão Spark para o dashboard.
    """

    return (
        SparkSession.builder
        .appName("DashboardDependenciaRoyalties")
        .master("local[*]")
        .config(
            "spark.driver.bindAddress",
            "127.0.0.1",
        )
        .getOrCreate()
    )


# ============================================================
# ANOS DISPONÍVEIS
# ============================================================

@st.cache_data
def obter_anos(caminho):
    """
    Obtém os anos disponíveis na Gold.
    """

    spark = criar_spark()

    df = (
        spark.read
        .parquet(caminho)
        .select("ano")
        .withColumn(
            "ano",
            F.col("ano").cast("integer"),
        )
        .filter(
            F.col("ano").isNotNull()
        )
        .distinct()
        .orderBy("ano")
    )

    return [
        row["ano"]
        for row in df.collect()
    ]


# ============================================================
# DADOS DO ANO SELECIONADO
# ============================================================

@st.cache_data
def carregar_dados_ano(caminho, ano):
    """
    Carrega somente os dados do ano selecionado.

    O filtro é executado pelo Spark antes do toPandas().
    """

    spark = criar_spark()

    df = (
        spark.read
        .parquet(caminho)
        .select(*COLUNAS_RANKING)
        .withColumn(
            "ano",
            F.col("ano").cast("integer"),
        )
        .withColumn(
            "royalties_real",
            F.col("royalties_real").cast("double"),
        )
        .withColumn(
            "pib_reais",
            F.col("pib_reais").cast("double"),
        )
        .withColumn(
            "dependencia_royalties_pct",
            F.col(
                "dependencia_royalties_pct"
            ).cast("double"),
        )
        .filter(
            (F.col("ano") == ano)
            & F.col("municipio").isNotNull()
            & F.col(
                "dependencia_royalties_pct"
            ).isNotNull()
        )
    )

    return df.toPandas()


# ============================================================
# HISTÓRICO DO MUNICÍPIO
# ============================================================

@st.cache_data
def carregar_historico(caminho, municipio):
    """
    Carrega o histórico de Royalties e PIB
    do município selecionado.
    """

    spark = criar_spark()

    df = (
        spark.read
        .parquet(caminho)
        .select(
            "municipio",
            "ano",
            "royalties_real",
            "pib_reais",
            "dependencia_royalties_pct",
        )
        .withColumn(
            "ano",
            F.col("ano").cast("integer"),
        )
        .withColumn(
            "royalties_real",
            F.col("royalties_real").cast("double"),
        )
        .withColumn(
            "pib_reais",
            F.col("pib_reais").cast("double"),
        )
        .withColumn(
            "dependencia_royalties_pct",
            F.col(
                "dependencia_royalties_pct"
            ).cast("double"),
        )
        .filter(
            (F.col("municipio") == municipio)
            & F.col("ano").isNotNull()
            & F.col("royalties_real").isNotNull()
            & F.col("pib_reais").isNotNull()
        )
        .orderBy("ano")
    )

    return df.toPandas()


# ============================================================
# FORMATAÇÃO MONETÁRIA
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


# ============================================================
# FORMATAÇÃO DE PERCENTUAL
# ============================================================

def formatar_percentual(valor):

    if pd.isna(valor):
        return "-"

    return f"{valor:.2f}%"


# ============================================================
# RANKINGS
# ============================================================

def obter_rankings(dados):

    coluna = "dependencia_royalties_pct"

    mais = (
        dados
        .nlargest(10, coluna)
        .copy()
    )

    menos = (
        dados
        .nsmallest(10, coluna)
        .copy()
    )

    return mais, menos


# ============================================================
# PREPARAR TABELA
# ============================================================

def preparar_tabela(dados):

    tabela = (
        dados[
            [
                "municipio",
                "royalties_real",
                "pib_reais",
                "ano",
                "dependencia_royalties_pct",
            ]
        ]
        .copy()
        .rename(
            columns={
                "municipio": "Município",
                "royalties_real": "Royalties (R$)",
                "pib_reais": "PIB (R$)",
                "ano": "Ano",
                "dependencia_royalties_pct":
                    "Dependência (%)",
            }
        )
    )

    tabela["Royalties (R$)"] = (
        tabela["Royalties (R$)"]
        .map(formatar_reais)
    )

    tabela["PIB (R$)"] = (
        tabela["PIB (R$)"]
        .map(formatar_reais)
    )

    tabela["Dependência (%)"] = (
        tabela["Dependência (%)"]
        .map(formatar_percentual)
    )

    return tabela


# ============================================================
# GRÁFICO DE BARRAS
# ============================================================

def criar_grafico_barras(dados, cor):

    grafico = (
        dados
        .sort_values(
            "dependencia_royalties_pct",
            ascending=True,
        )
    )

    fig = px.bar(
        grafico,
        x="dependencia_royalties_pct",
        y="municipio",
        orientation="h",
        text="dependencia_royalties_pct",
        color_discrete_sequence=[cor],
        labels={
            "municipio": "Município",
            "dependencia_royalties_pct":
                "Dependência de Royalties (%)",
        },
    )

    fig.update_traces(
        texttemplate="%{text:.2f}%",
        textposition="outside",
        hovertemplate=(
            "<b>%{y}</b><br>"
            "Dependência: %{x:.2f}%"
            "<extra></extra>"
        ),
    )

    fig.update_layout(
        height=500,
        template="plotly_white",
        xaxis_title="Dependência de Royalties (%)",
        yaxis_title="Município",
        margin={
            "l": 160,
            "r": 100,
            "t": 60,
            "b": 60,
        },
    )

    return fig


# ============================================================
# GRÁFICO DE LINHA
# ROYALTIES + PIB
# ============================================================

def criar_grafico_linha(historico, municipio):
    """
    Cria gráfico de linha com a evolução de
    Royalties e PIB ao longo dos anos.

    Royalties -> eixo Y esquerdo
    PIB       -> eixo Y direito
    """

    fig = px.line(
        historico,
        x="ano",
        y=[
            "royalties_real",
            "pib_reais",
        ],
        markers=True,
        labels={
            "ano": "Ano",
            "royalties_real": "Royalties (R$)",
            "pib_reais": "PIB (R$)",
        },
        title=(
            f"Evolução de Royalties e PIB — "
            f"{municipio}"
        ),
    )

    # ========================================================
    # ROYALTIES
    # ========================================================

    fig.update_traces(
        selector={
            "name": "royalties_real"
        },
        line={
            "color": "#d62728",
            "width": 3,
        },
        marker={
            "size": 8,
        },
        hovertemplate=(
            "<b>Ano:</b> %{x}<br>"
            "<b>Royalties:</b> "
            "R$ %{y:,.2f}"
            "<extra></extra>"
        ),
    )

    # ========================================================
    # PIB
    # ========================================================

    fig.update_traces(
        selector={
            "name": "pib_reais"
        },
        line={
            "color": "#1f77b4",
            "width": 3,
        },
        marker={
            "size": 8,
        },
        hovertemplate=(
            "<b>Ano:</b> %{x}<br>"
            "<b>PIB:</b> "
            "R$ %{y:,.2f}"
            "<extra></extra>"
        ),
    )

    # ========================================================
    # LAYOUT
    # ========================================================

    fig.update_layout(
        height=500,
        template="plotly_white",
        hovermode="x unified",

        xaxis={
            "title": "Ano",
            "dtick": 1,
        },

        yaxis={
            "title": "Royalties (R$)",
        },

        yaxis2={
            "title": "PIB (R$)",
            "overlaying": "y",
            "side": "right",
        },

        margin={
            "l": 80,
            "r": 100,
            "t": 80,
            "b": 60,
        },

        legend={
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.02,
            "xanchor": "center",
            "x": 0.5,
        },
    )

    # ========================================================
    # EIXOS
    # ========================================================

    if len(fig.data) >= 2:

        fig.data[0].update(
            yaxis="y"
        )

        fig.data[1].update(
            yaxis="y2"
        )

    return fig


# ============================================================
# CABEÇALHO
# ============================================================

st.title(
    "📊 Ranking de Dependência de Royalties"
)

st.markdown(
    """
    ### Top 10 Municípios mais e menos dependentes de royalties

    Utilize o seletor de ano e os botões abaixo para analisar
    os municípios.
    """
)


# ============================================================
# ANOS DISPONÍVEIS
# ============================================================

try:

    anos = obter_anos(
        CAMINHO_GOLD
    )

except Exception as erro:

    st.error(
        "Não foi possível acessar a tabela Gold."
    )

    st.exception(erro)

    st.stop()


if not anos:

    st.error(
        "Nenhum ano foi encontrado na tabela Gold."
    )

    st.stop()


# ============================================================
# CONTROLES
# ============================================================

st.divider()

st.subheader("🎛️ Controles")

col_ano, col_info = st.columns(
    [1, 3]
)


with col_ano:

    indice_inicial = (
        anos.index(2021)
        if 2021 in anos
        else len(anos) - 1
    )

    ano = st.selectbox(
        "📅 Selecione o ano",
        options=anos,
        index=indice_inicial,
    )


with col_info:

    st.info(
        f"Você está analisando o ano **{ano}**."
    )


# ============================================================
# DADOS DO ANO
# ============================================================

try:

    dados_ano = carregar_dados_ano(
        CAMINHO_GOLD,
        ano,
    )

except Exception as erro:

    st.error(
        "Não foi possível carregar os dados."
    )

    st.exception(erro)

    st.stop()


if dados_ano.empty:

    st.warning(
        f"Não existem dados para o ano {ano}."
    )

    st.stop()


# ============================================================
# RANKINGS
# ============================================================

mais_dependentes, menos_dependentes = (
    obter_rankings(dados_ano)
)


# ============================================================
# ESTADO DO RANKING
# ============================================================

if "tipo_ranking" not in st.session_state:

    st.session_state.tipo_ranking = "mais"


# ============================================================
# BOTÕES
# ============================================================

col_mais, col_menos = st.columns(2)


with col_mais:

    if st.button(
        "🔴 TOP 10 MAIS DEPENDENTES",
        use_container_width=True,
    ):

        st.session_state.tipo_ranking = "mais"


with col_menos:

    if st.button(
        "🟢 TOP 10 MENOS DEPENDENTES",
        use_container_width=True,
    ):

        st.session_state.tipo_ranking = "menos"


# ============================================================
# DEFINIR RANKING
# ============================================================

if st.session_state.tipo_ranking == "mais":

    dados_resultado = mais_dependentes

    titulo = (
        f"🔴 TOP 10 Municípios Mais Dependentes "
        f"de Royalties — {ano}"
    )

    cor = "#d62728"

else:

    dados_resultado = menos_dependentes

    titulo = (
        f"🟢 TOP 10 Municípios Menos Dependentes "
        f"de Royalties — {ano}"
    )

    cor = "#2ca02c"


# ============================================================
# RANKING
# ============================================================

st.divider()

st.subheader(titulo)


# ============================================================
# GRÁFICO DE BARRAS
# ============================================================

fig_barras = criar_grafico_barras(
    dados_resultado,
    cor,
)

st.plotly_chart(
    fig_barras,
    use_container_width=True,
)


# ============================================================
# EVOLUÇÃO HISTÓRICA
# ============================================================

st.divider()

st.subheader(
    "📈 Evolução de Royalties e PIB"
)

st.markdown(
    """
    Selecione um município para visualizar a evolução
    dos royalties e do PIB ao longo dos anos.
    """
)


# ============================================================
# MUNICÍPIOS
# ============================================================

municipios = sorted(
    dados_ano["municipio"]
    .dropna()
    .unique()
    .tolist()
)


if municipios:

    municipio = st.selectbox(
        "🏙️ Selecione o município",
        options=municipios,
    )

    try:

        historico = carregar_historico(
            CAMINHO_GOLD,
            municipio,
        )

    except Exception as erro:

        st.error(
            "Não foi possível carregar o histórico."
        )

        st.exception(erro)

        historico = pd.DataFrame()


    if historico.empty:

        st.warning(
            f"Não existem dados históricos para "
            f"{municipio}."
        )

    else:

        fig_linha = criar_grafico_linha(
            historico,
            municipio,
        )

        st.plotly_chart(
            fig_linha,
            use_container_width=True,
        )


# ============================================================
# TABELA
# ============================================================

st.divider()

st.subheader(
    "📋 Dados dos municípios"
)


dados_tabela = (
    dados_resultado
    .sort_values(
        "dependencia_royalties_pct",
        ascending=(
            st.session_state.tipo_ranking == "menos"
        ),
    )
)


tabela = preparar_tabela(
    dados_tabela
)


st.dataframe(
    tabela,
    use_container_width=True,
    hide_index=True,
)


# ============================================================
# RESUMO
# ============================================================

st.divider()

col_a, col_b, col_c = st.columns(3)


with col_a:

    st.metric(
        "Ano",
        ano,
    )


with col_b:

    st.metric(
        "Municípios analisados",
        len(dados_ano),
    )


with col_c:

    st.metric(
        "Ranking exibido",
        (
            "10 Mais"
            if st.session_state.tipo_ranking == "mais"
            else "10 Menos"
        ),
    )


# ============================================================
# RODAPÉ
# ============================================================

st.divider()

st.caption(
    "Fonte dos dados: SIDRA IBGE, ANP"
)

st.caption(
    "Dashboard desenvolvido com "
    "Streamlit + PySpark + Plotly."
)

## utilizar Ctrl + C para encerrar o servidor do Streamlit no terminal
## /workspaces/MVP-royalteis/.venv/bin/streamlit run /workspaces/MVP-royalteis/silver.py --server.address 0.0.0.0 --server.port 8501
## link do dashboard: http://localhost:8501 - depois abrir no navegador. http://51.8.152.69:8501