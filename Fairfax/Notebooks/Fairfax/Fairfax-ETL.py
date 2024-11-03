# Databricks notebook source
# MAGIC %run /Workspace/Fairfax/Notebooks/Fairfax/Fairfax-metodos

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1- Introdução
# MAGIC Você foi contratado por uma agência de dados governamentais para projetar e implementar um pipeline de ETL (Extract, Transform, Load) que extrai dados de uma base pública, transforma e carrega os dados para análise em Databricks. Os dados escolhidos são os de COVID-19 disponíveis em https://storage.googleapis.com/covid19-open-data/v3/latest/aggregated.csv.
# MAGIC Após o processo de ETL, você deve realizar uma análise sumária dos dados usando Python e Databricks. A quantidade de sumarização e os objetivos desta sumarização é de sua escolha.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2- Requisitos
# MAGIC
# MAGIC #### 2.1. Proponha uma arquitetura de dados para o pipeline de ETL:
# MAGIC
# MAGIC #####a. Explique como você planeja extrair os dados da fonte pública (URL).
# MAGIC
# MAGIC Resp.: Para extrair dados de um arquivo CSV disponível em uma URL pública usando a biblioteca requests e, em seguida, processá-lo com PySpark no Databricks, podemos seguir os seguintes passos para compor o pipeline de ETL. Nesse caso, requests fará o download do arquivo para o ambiente Databricks, e depois usaremos o PySpark para carregar os dados num dataframe. Conforme os CMD 6, 7 e 8.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b. Descreva as etapas de transformação necessárias para preparar os dados para análise.
# MAGIC Resp.: As etapas são as seguintes:
# MAGIC - **Pré-análise dos dados originais**: é um processo que envolve a revisão e avaliação dos dados antes de qualquer transformação ou manipulação. Envolve a análise de schema, atributos e volumetria;
# MAGIC - **Limpeza e Validação**: envolve a remoção de registros duplicados, o tratamento de valores ausentes, quando necessário, e a aplicação da conversão de tipos de dados.
# MAGIC - **Enriquecimento de Dados**: Esta etapa contempla junções (embora não sejam aplicáveis neste caso), além da padronização e normalização dos dados.
# MAGIC - **Análise final dos dados**: Processo de realização de insights a partir dos dados.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### c. Explique como você irá carregar os dados transformados na plataforma Databricks.
# MAGIC Resp.: Utilizamos PySpark no Databricks para realizar transformações escaláveis em grandes volumes de dados armazenados em tabelas Delta, aproveitando a capacidade de distribuição em cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.2. Implementação do Pipeline de ETL:
# MAGIC a. Implemente o processo de ETL utilizando Python/Databricks.<br>
# MAGIC b. Documente o código com comentários claros e concisos.<br>

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.3. Armazenamento na Plataforma Databricks:
# MAGIC ##### a. Escolha o formato de armazenamento adequado para os dados transformados
# MAGIC Resp.: Para uma estrutura de Data Lake o melhor formato é o Delta.
# MAGIC
# MAGIC #####  b. Justifique a escolha do formato de armazenamento e descreva como ele suporta consultas e análises eficientes.
# MAGIC Resp.: O formato Delta melhora o desempenho, confiabilidade e flexibilidade dos dados no Data Lake, facilitando análises rápidas e escaláveis ao permitir o processamento de dados complexos com garantia de consistência.

# COMMAND ----------

# dbutils.fs.rm('dbfs:/FileStore/tables/fairfaxDL/', True)

# COMMAND ----------

# DBTITLE 1,Etapa de Consulta à API e Ingestão do Arquivo na Camada Bronze
# Processo de consulta à API na URL especificada e ingestão do arquivo '.csv' na Camada Transient,
# que é a camada temporária do Data Lake dentro do Databricks (no DBFS).  
baixa_arquivos_url(url, transient_path)
# Ingestão dos dados na camada bronze no formato Delta, incluindo o atributo ingestion_timestamp (data e hora da ingestão), e remoção do arquivo da camada transient.
transient_to_bronze(transient_path, bronze_path)

# COMMAND ----------

# DBTITLE 1,Pré-análise dos Dados na Camada Bronze
# A pré-análise na camada bronze é um passo fundamental que garante a integridade e
# a utilidade dos dados ao longo de todo o ciclo de vida da análise. Ela contempla: 
# o detalhe do Schema do Dataframe;e
# a quantidade de valores null nos atributos. 
pre_analise(bronze_path)

# COMMAND ----------

# DBTITLE 1,Tratamento/Enriquecimento  dos Dados da Camada Bronze para Silver
# Tratamento de dados aplicados:
## 1. Seleciona os atributos;
## 2. Renomear os atributos que estão em ingles para portugues;
## 3. Transforma tipo date em timestamp;
## 3. Remover Acentos de uma Coluna;

lista_de_atributos = [origemEnum.LOCATION_KEY.value, 
                      origemEnum.DATE.value,
                      origemEnum.INFANT_MORTALITY_RATE.value,
                      origemEnum.ADULT_FEMALE_MORTALITY_RATE.value,
                      origemEnum.ADULT_MALE_MORTALITY_RATE.value,
                      origemEnum.POPULATION.value,
                      origemEnum.POPULATION_FEMALE.value,
                      origemEnum.POPULATION_MALE.value,
                      origemEnum.POPULATION_RURAL.value,
                      origemEnum.POPULATION_URBAN.value,
                      origemEnum.CUMULATIVE_CONFIRMED.value,
                      origemEnum.CUMULATIVE_DECEASED.value,
                      origemEnum.CUMULATIVE_RECOVERED.value,
                      origemEnum.CUMULATIVE_TESTED.value,
                      origemEnum.SUBREGION1_CODE.value,
                      origemEnum.SUBREGION1_NAME.value,
                      origemEnum.COUNTRY_CODE.value,
                      origemEnum.COUNTRY_NAME.value,
                      origemEnum.INGESTION_TIMESTAMP.value
                      ]
tratamento(lista_de_atributos, bronze_path, silver_path)

# COMMAND ----------

# DBTITLE 1,Processamento de Dados para a Camada Gold em Formato Otimizado para Relatórios e Análises finais
# Dados armazenados na Camada Gold em um formato otimizado para relatórios e análises finais.
# Esses dados foram agregados por país.
processamento_gold( silver_path, gold_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fim do Processo ETL

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.4. Análise Sumária dos Dados:
# MAGIC ##### a. Utilize a plataforma Databricks para realizar análises descritivas sobre os dados carregados.
# MAGIC ##### b. Gere visualizações e métricas chave (ex. número de casos, mortes, recuperações por região).
# MAGIC ##### c. Documente suas descobertas e insights derivados dos dados.

# COMMAND ----------

# DBTITLE 1,Leitura dos Dados da Camada Gold
analitico_df = spark.read.format(formato).load(gold_path)

# COMMAND ----------

# DBTITLE 1,Analise de Dados com a Funcao Relatorio Analitico
relatorio_analitico(analitico_df)

# COMMAND ----------

# DBTITLE 1,Explorando dados por meio de análise gráfica
analise_grafica(analitico_df)

# COMMAND ----------

# DBTITLE 1,Análise de Correlação em Python
analise_correlacao(analitico_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Resultados Esperados:
# MAGIC A matriz de correlação mostrará os coeficientes de correlação entre total_casos, total_mortes e total_recuperados. 
# MAGIC a. Os valores variam de -1 a 1, onde:
# MAGIC - 1: indica uma correlação positiva perfeita,
# MAGIC - -1: indica uma correlação negativa perfeita,
# MAGIC - 0: indica nenhuma correlação.
# MAGIC #### Interpretação:
# MAGIC Você pode analisar como as variáveis estão relacionadas. Por exemplo, uma alta correlação entre total_casos e total_mortes pode indicar que mais casos estão associados a um maior número de mortes.

# COMMAND ----------

# DBTITLE 1,Geração de relatório de análise abrangente
relatorio_geral(analitico_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.5. Implementação de Medidas de Segurança:
# MAGIC ##### a. Explique como você garante a segurança dos dados durante o processo de ETL.
# MAGIC Resp.: Uso de criptografia de dados, controle de acesso, Auditoria e Monitoramento,  Limpeza de Dados Sensíveis são boas praticas para garantir as medidas de segurança dos dados
# MAGIC ##### b. Descreva medidas de segurança implementadas para proteger os dados armazenados na plataforma Databricks.
# MAGIC Resp.: Seguem algumas medidas:
# MAGIC - Criptografia em Repouso: Databricks garante a criptografia em repouso para dados no Delta Lake e outros formatos, protegendo-os contra acessos físicos não autorizados;
# MAGIC - Controle de Acesso Granular:Utilize controle de acesso baseado em funções para restringir o acesso a notebooks, clusters e tabelas, definindo permissões específicas para leitura, escrita e execução em diferentes níveis de granularidade;
# MAGIC - Integração com IAM (Identity and Access Management): Integre o Databricks com serviços de gerenciamento de identidade, como AWS IAM ou Azure AD, para garantir uma autenticação e autorização seguras;
# MAGIC - Auditoria e Monitoramento de Atividades: Ative logs de auditoria para registrar acessos e operações em clusters e notebooks, permitindo a identificação de comportamentos suspeitos e fornecendo um registro para conformidade;
# MAGIC - Segurança da Rede: Implementar configurações de rede seguras, como VPCs e sub-redes, além de utilizar firewalls e grupos de segurança para restringir o acesso aos clusters Databricks.
# MAGIC - Proteção contra Perda de Dados: Configurar backups regulares e estratégias de recuperação de desastres para garantir que os dados possam ser restaurados em caso de perda ou corrupção.
# MAGIC
# MAGIC ##### Bibliografia
# MAGIC Databricks. (n.d.). Databricks Security Overview. Disponível em: [Databricks](https://docs.databricks.com/en/index.html).
# MAGIC
# MAGIC ##### Informação adicional
# MAGIC
# MAGIC A implementação dessas medidas de segurança durante o processo de ETL e no armazenamento de dados na plataforma Databricks não apenas protege dados sensíveis, mas também garante a conformidade com regulamentações importantes, como o GDPR e o HIPAA. É fundamental que a segurança seja uma prioridade em todas as etapas do ciclo de vida dos dados, desde a extração até a transformação e o armazenamento, assegurando a integridade e a confidencialidade das informações em todo o processo (NIST, 2020).
# MAGIC
# MAGIC Referência: NIST. (2020). Framework for Improving Critical Infrastructure Cybersecurity. National Institute of Standards and Technology. Disponível em: [NIST Cybersecurity Framework](https://www.nist.gov/cyberframework)
