# MVP-royalteis

## Introdução do Projeto:

O presente trabalho busca compreender o grau de dependência dos municípios do estado do Rio de Janeiro em relação às receitas provenientes dos royalties do petróleo, bem como analisar a participação desses recursos em relação ao Produto Interno Bruto (PIB) municipal.

Para isso, serão analisados dados referentes ao Produto Interno Bruto dos Municípios, disponibilizados pelo Instituto Brasileiro de Geografia e Estatística (IBGE), por meio do Sistema IBGE de Recuperação Automática (SIDRA), tabela 5938, e os valores arrecadados pelos municípios em royalties do petróleo, obtidos junto à Agência Nacional do Petróleo, Gás Natural e Biocombustíveis (ANP).

A partir dessas informações, pretende-se estabelecer uma relação entre o PIB municipal e os valores recebidos em royalties, permitindo identificar o grau de dependência econômica dos municípios fluminenses em relação a essa fonte de receita. A análise também possibilitará observar diferenças entre os municípios e a evolução dessa dependência ao longo do período estudado.O presente trabalho busca compreender o grau de relação dos municípios do estado do Rio de Janeiro com as receitas provenientes dos royalties do petróleo, analisando também a participação desses recursos em relação ao Produto Interno Bruto (PIB) municipal.

Para isso, são utilizados dados referentes ao Produto Interno Bruto dos Municípios, disponibilizados pelo Instituto Brasileiro de Geografia e Estatística (IBGE) por meio do Sistema IBGE de Recuperação Automática (SIDRA), e dados referentes aos valores arrecadados pelos municípios em royalties de petróleo, obtidos junto à Agência Nacional do Petróleo, Gás Natural e Biocombustíveis (ANP).

A partir dessas informações, o projeto estabelece uma relação entre o PIB municipal e os valores recebidos em royalties, permitindo construir um indicador que mostra a participação dos royalties em relação ao PIB.

A análise permite observar diferenças entre os municípios e a evolução dessa relação ao longo do período estudado.

Entretanto, este trabalho é uma pequena análise exploratória e superficial sobre o tema.

A utilização de apenas duas variáveis — royalties e PIB — não permite compreender a real dependência econômica ou fiscal de um município em relação aos royalties.

Para um trabalho mais aprofundado, seria necessário analisar outras variáveis e compreender as especificidades econômicas, fiscais e sociais de cada município. 

## Questionamentos do projeto
O projeto foi desenvolvido inicialmente para investigar as seguintes questões:

Quais são os municípios mais dependentes dos royalties?

Quais são os municípios menos dependentes dos royalties?

Qual é o percentual dos royalties em relação ao PIB de cada município?

Os municípios que mais recebem royalties são necessariamente os mais dependentes?

Municípios que recebem grandes volumes de royalties possuem necessariamente maior dependência econômica?

Como a relação entre royalties e PIB evoluiu ao longo do tempo?

Municípios com economias mais diversificadas apresentam menor relação entre royalties e PIB?

Essas perguntas orientam a construção do pipeline e do dashboard.

## Hipótese inicial

Uma das hipóteses iniciais era que os municípios que mais recebem royalties em valores absolutos também seriam aqueles que apresentariam maior dependência desses recursos.

Nesse contexto, havia a expectativa de que municípios como Niterói e Maricá pudessem aparecer entre os municípios com maior dependência, considerando os elevados valores arrecadados em royalties.

Entretanto, a análise evidencia uma distinção importante:

Maior arrecadação de royalties
              ≠
Maior dependência relativa

Isso acontece porque o indicador utilizado considera o tamanho da economia municipal.

Um município pode receber muitos royalties, mas possuir uma economia suficientemente grande e diversificada para que os royalties representem uma parcela relativamente menor do seu PIB.

Da mesma forma, um município pode receber um volume absoluto menor de royalties, mas apresentar uma relação royalties/PIB significativamente maior.




## 📊 MVP Royalties — Rio de Janeiro

Pipeline de dados desenvolvido para analisar a relação entre royalties de petróleo e gás e o PIB dos municípios do estado do Rio de Janeiro.

O projeto implementa um fluxo de dados em arquitetura Medallion (Bronze → Silver → Gold), utilizando Python, Pandas, PySpark, Parquet, Streamlit e Plotly.

A camada Gold gera um indicador anual baseado na relação entre os royalties recebidos e o PIB municipal, disponibilizado posteriormente em um dashboard interativo.

## 🎯 Objetivo

O objetivo do projeto é integrar dados municipais de:

Royalties;

PIB municipal;

Município;

Ano;

para gerar uma métrica que permita analisar a participação dos royalties em relação ao PIB.

O indicador utilizado é:

Royalties sobre PIB (%) = (Royalties / PIB) × 100


A análise considera o período de:

2011 a 2024


Nota: o indicador representa uma razão entre royalties e PIB. Ele não deve ser interpretado isoladamente como uma medida completa de dependência econômica.

🏗️ Arquitetura

O projeto segue uma arquitetura de dados em camadas:

                         FONTES
                           │
             ┌─────────────┴─────────────┐
             │                           │
             ▼                           ▼
      royalties_rj.xlsx             pib_rj.xlsx
             │                           │
             └─────────────┬─────────────┘
                           │
                           ▼
                    ┌────────────┐
                    │   BRONZE   │
                    │  Ingestão  │
                    └──────┬─────┘
                           │
                           ▼
                    ┌────────────┐
                    │   SILVER   │
                    │ Tratamento │
                    │ Wide→Long  │
                    └──────┬─────┘
                           │
                           ▼
                    ┌────────────┐
                    │    GOLD    │
                    │ Join + KPI │
                    └──────┬─────┘
                           │
                           ▼
                    ┌────────────┐
                    │ STREAMLIT  │
                    │ Dashboard  │
                    └────────────┘

🥉 Bronze

A camada Bronze é responsável pela ingestão dos dados brutos.

Os arquivos Excel são lidos utilizando Pandas e convertidos para Parquet.

Entradas
dados/
├── royalties_rj.xlsx
└── pib_rj.xlsx

Saídas
data/bronze/
├── bronze_royalties.parquet
└── bronze_pib.parquet

Tratamentos

A Bronze realiza apenas transformações mínimas:

leitura dos arquivos Excel;

validação de arquivo vazio;

normalização dos nomes das colunas;

persistência em formato Parquet.

A intenção é manter a camada Bronze próxima da estrutura original das fontes.

## 🥈 Silver

A camada Silver utiliza PySpark para padronizar os dados e prepará-los para integração.

Royalties

Entrada:

data/bronze/bronze_royalties.parquet


Saída:

data/silver/silver_royalties.parquet


Estrutura:

codigo_municipio
municipio
ano
royalties_real


Os dados anuais originalmente organizados em formato wide, como:

TOTAL_2011
TOTAL_2012
TOTAL_2013
...
TOTAL_2024


são transformados para o formato long:

codigo_municipio | municipio | ano | royalties_real

Tratamentos

seleção das colunas necessárias;

padronização dos nomes;

conversão de tipos;

transformação wide → long;

criação da coluna ano;

remoção de registros inválidos;

manutenção apenas de valores positivos de royalties.

PIB

Entrada:

data/bronze/bronze_pib.parquet


Saída:

data/silver/silver_pib.parquet


Estrutura:

codigo_municipio
municipio
ano
pib_reais


São considerados os registros classificados como município:

Nível = MU


Os valores anuais também são transformados de wide para long.

Tratamentos

seleção de registros municipais;

padronização das colunas;

conversão de tipos;

transformação wide → long;

remoção de registros inválidos;

manutenção de valores positivos de PIB.

## 🥇 Gold

A camada Gold combina as tabelas Silver para gerar o indicador analítico.

Entradas
data/silver/silver_royalties.parquet
data/silver/silver_pib.parquet

Saída
data/gold/gold_dependencia_anual.parquet

🔗 Integração

O relacionamento entre royalties e PIB é realizado utilizando:

codigo_municipio + ano


O JOIN utilizado é:

INNER JOIN


Isso significa que são mantidos apenas os municípios e anos presentes nas duas fontes.

## 📐 Indicador

A Gold calcula:

dependencia_royalties_pct =
    (royalties_real / pib_reais) × 100


A tabela final possui:

codigo_municipio
municipio
ano
royalties_real
pib_reais
dependencia_royalties_pct


Exemplo conceitual:

codigo_municipio | municipio | ano  | royalties_real | pib_reais | dependencia_royalties_pct
-----------------|-----------|------|----------------|-----------|---------------------------
3300100          | Município A | 2019 | ...            | ...       | ...
3300100          | Município A | 2020 | ...            | ...       | ...
3300100          | Município A | 2021 | ...            | ...       | ...

## 🔑 Granularidade

A granularidade esperada das tabelas Silver e Gold é:

1 registro = 1 município × 1 ano


A chave lógica é:

codigo_municipio + ano


Essa granularidade é importante para evitar duplicações durante o JOIN.

💰 Unidades monetárias

A comparação entre royalties e PIB exige que ambos estejam na mesma unidade monetária.

No processamento atual, os royalties passam pela transformação:

pib_reais = valor * 1000


Portanto, é necessário confirmar na fonte original a unidade do campo utilizado.

Antes de utilizar o indicador, deve ser garantido que:

pib_reais → mesma unidade do royalties_real


Por exemplo:

Royalties → R$
PIB       → R$


Caso as fontes estejam em milhares, milhões ou outra unidade, a conversão deve ser ajustada no pipeline.

📊 Dashboard

O dashboard foi desenvolvido utilizando:

Streamlit;

PySpark;

Pandas;

Plotly.

O dashboard utiliza diretamente a tabela Gold:

data/gold/gold_dependencia_anual.parquet

Funcionalidades
📅 Seleção de ano

Permite selecionar um dos anos disponíveis na Gold.

🔴 Ranking dos maiores valores

Apresenta os cinco municípios com os maiores valores de:

dependencia_royalties_pct

🟢 Ranking dos menores valores

Apresenta os cinco municípios com os menores valores positivos da métrica.

Tecnicamente, trata-se dos:

10 menores valores de royalties / PIB

📊 Gráfico de barras

Apresenta visualmente o ranking selecionado.

## 📈 Histórico municipal

Permite selecionar um município e visualizar a evolução histórica de:

royalties;

PIB.

O período disponível depende dos dados existentes na Gold.

## 📋 Tabela

O dashboard disponibiliza uma tabela contendo:

Município
Royalties (R$)
PIB (R$)
Ano
Dependência (%)

## 📁 Estrutura do projeto
MVP-royalteis/
│
├── dados/
│   ├── royalties_rj.xlsx
│   └── pib_rj.xlsx
│
├── data/
│   ├── bronze/
│   │   ├── bronze_royalties.parquet
│   │   └── bronze_pib.parquet
│   │
│   ├── silver/
│   │   ├── silver_royalties.parquet
│   │   └── silver_pib.parquet
│   │
│   └── gold/
│       └── gold_dependencia_anual.parquet
│
├── src/
│   ├── bronze.py
│   ├── silver.py
│   ├── gold.py
│   └── dashboard.py
│
├── requirements.txt
│
└── README.md

## 🛠️ Tecnologias
Tecnologia	Utilização
Python	Linguagem principal
Pandas	Ingestão dos arquivos Excel
PySpark	Processamento e transformação
Apache Spark	Engine de processamento
PyArrow	Persistência em Parquet
Parquet	Armazenamento intermediário
Streamlit	Dashboard
Plotly	Visualizações

## 👨‍💻 Projeto

MVP Royalties — Rio de Janeiro

Pipeline desenvolvido com:

Python · PySpark · Pandas · Parquet · Streamlit · Plotly

Período analisado: 2011–2024


## 💡 Principal insight do projeto
A expectativa inicial era que municípios como Niterói e Maricá, por apresentarem elevados valores de arrecadação provenientes dos royalties, também estivessem entre os municípios com maior dependência.

Entretanto, a análise mostra a importância de diferenciar receita absoluta de dependência relativa.

Municípios que recebem grandes volumes de royalties podem apresentar uma relação relativamente menor entre royalties e PIB porque possuem economias maiores e/ou mais diversificadas.

Esse resultado é importante porque demonstra que:

Receber mais royalties
não significa necessariamente
ser mais dependente dos royalties.

Por outro lado, municípios que apresentam uma relação elevada entre royalties e PIB merecem atenção em análises futuras, principalmente quando essa relação permanece elevada ao longo de vários anos.

## ⚠️ Interpretação dos resultados
Uma relação elevada entre royalties e PIB pode representar uma maior exposição relativa aos royalties, mas não permite concluir isoladamente que o município seja fiscalmente dependente dessa receita.

Da mesma forma, uma relação baixa não significa necessariamente ausência de dependência.

Para compreender a real situação de cada município seria necessário analisar, entre outros fatores:

transferências;

população;

renda;

emprego;