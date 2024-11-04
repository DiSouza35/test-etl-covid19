# Teste ETL COVID-19

## Projeto de ETL
Este projeto consiste em um pipeline de ETL que extrai dados de uma API, ingere-os inicialmente na camada transient e, em seguida, realiza a transferência para o Data Lake utilizando o Databricks.

## Introdução
Você foi contratado por uma agência de dados governamentais para projetar e implementar um pipeline de ETL (Extract, Transform, Load) que extrai dados de uma base pública, transforma e carrega os dados para análise em Databricks. Os dados escolhidos são os de COVID-19, disponíveis em [COVID-19 Open Data](https://storage.googleapis.com/covid19-open-data/v3/latest/aggregated.csv). Após o processo de ETL, você deve realizar uma análise sumária dos dados utilizando Python e Databricks. A quantidade de sumarização e os objetivos dessa análise ficam a seu critério.
## Arquitetura do Projero
### 1. Data Lake
A arquitetura de um Data Lake no DBFS (Databricks File System) do Databricks é projetada para armazenar grandes volumes de dados brutos de forma escalável e econômica. Aqui estão os principais componentes:

**Camadas de Dados:**

- **Camada Transient:** Armazena dados temporários durante o processo de ETL. É uma área de staging para ingestão inicial.

- **Camada Bronze:** Contém os dados brutos, tal como são recebidos, permitindo acesso rápido e recuperação.

- **Camada Silver:** Dados processados e limpos, onde ocorre a transformação e a agregação. É ideal para análises intermediárias.

- **Camada Gold:** Dados altamente refinados, otimizados para relatórios e análises finais. Aqui, os dados são organizados de maneira a facilitar insights e decisões.

**Formatos de Armazenamento:**

Os dados são armazenados em formato Delta proporcionando flexibilidade e eficiência no processamento.
Processamento de Dados:

**databricks enginee:**

Utiliza o Apache Spark para processamento em larga escala, permitindo transformações complexas e análises avançadas.

**Integração com APIs:**

Facilita a ingestão de dados a partir de diversas fontes, incluindo APIs públicas e bancos de dados.

**Segurança e Governança:**

O Databricks oferece ferramentas para controle de acesso, segurança e gerenciamento de dados, garantindo que apenas usuários autorizados tenham acesso a informações sensíveis.
## Detalhes Técnicos
Diretório dos notebooks: `Fairfax/Notebooks/`
O repositório é composto pelos seguintes notebooks:
- **Fairfax-ETL.py**: O notebook principal, que contém todo o processo ETL dentro do Data Lake, com todos os passos encapsulados e comentados.
- **Fairfax-importes_variáveis_enums.py**: Contém todos os imports, variáveis e enums necessários para o projeto.
- **Fairfax-metodos.py**: Abrange todos os métodos utilizados no processo de ETL, com explicações detalhadas sobre o código e as etapas.
- **Fairfax-teste.py**: Contém os testes dos métodos, incluindo comentários sobre cada teste.
- **desafio-data-Fairfax-V0.py**: Este é o notebook que contém todos os processos de forma aberta, encapsulada e comentada, servindo como base para o desenvolvimento.

## Executando o Processo ETL

Para a execução completa do pipeline, basta executar o notebook **Fairfax-ETL.py**, que irá importar todas as bibliotecas, variáveis, enums e métodos necessários.

**OBS.:** Em caso de erro ajusta o paths que estão no primeiro comando do notebook **Fairfax-ETL.py**,  **Fairfax-metodos.py** e **Fairfax-teste.py**, para o diretório do workspace de quem o executa.




