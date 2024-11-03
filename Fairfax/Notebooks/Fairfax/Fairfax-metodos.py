# Databricks notebook source
# MAGIC %md
# MAGIC ### Métodos

# COMMAND ----------

# MAGIC %run /Workspace/Fairfax/Notebooks/Fairfax/Fairfax-importes_variáveis_enums

# COMMAND ----------

# Método de ingestão de dados: baixar o arquivo da URL e salvá-lo na camada transient no DBFS.

def baixa_arquivos_url(url, file_path):
    response = requests.get(url)
    file_name = 'aggregated'
    
    # Ensure the directory exists
    full_path = f'/dbfs{file_path}'
    if not os.path.exists(full_path):
        os.makedirs(full_path)
    
    # Now construct the correct path for the file
    with open(f'{full_path}/{file_name}', 'wb') as file:
        file.write(response.content)
    return print("Arquivo CSV baixado com sucesso!")
    
 # O método lê os dados do data lake e gera um dataframe.
    
def transient_to_bronze(transient_path, bronze_path):
    print("=========================================================================")
    df = spark.read.csv(f'dbfs:{transient_path}', header=True, inferSchema=True)
    # Adiciona a coluna ingestion_timestamp, que representa o carimbo de data e hora de ingestão, ao DataFrame.
    data_hora_sao_paulo = spark.sql("SELECT to_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') as data_hora").collect()[0]['data_hora']
    df = df.withColumn(origemEnum.INGESTION_TIMESTAMP.value, lit(data_hora_sao_paulo))
    # Os dados são escritos na camada bronze do Data Lake.
    df.write.format(formato).mode(outputMode).save(bronze_path)
    print(f"{df.count()} arquivo ingerido com sucesso na camada bronze em formato Delta!")
    print("=========================================================================")
    return print(f"Arquivo removido com sucesso da camada transient: {dbutils.fs.rm(f'dbfs:{transient_path}', True)}")

# COMMAND ----------

# DBTITLE 1,Relatório de Pré-análise dos Dados
def pre_analise(bronze_path):
    df = spark.read.format(formato).load(bronze_path)
    print("Pré-análise do dataset sobre a COVID-19")
    print("===========================================================================================")
    print("Detalhe do Schema de entrada do Dataset:")
    df.printSchema()
    print("===========================================================================================")
    print(f"A quantidade de atributos do DataFrame: {len(df.columns)}")
    print("===========================================================================================")
    print(f"A quantidade de linhas do DataFrame: {df.count()}")
    print("===========================================================================================")
    # Verificar se existem valores nulos
    num_nulls = df.select([col(column).isNull().cast("int").alias(column) for column in df.columns]).agg(*[sum(col(column)).alias(column) for column in df.columns]).collect()[0]
    # Exibir a quantidade de nulos por coluna
    print("Quantidade de valores nulos por coluna:")
    for column, count in zip(df.columns, num_nulls):
        print(f"{column}: {count}")
    print("===========================================================================================")

# COMMAND ----------

# Devido à falta de documentação dos dados, interpretarei as taxas de mortalidade como taxas de mortalidade associadas à COVID-19.
# Tratamento de dados aplicados:
## 1. Seleciona os atributos;
## 2. Renomear os atributos que estão em ingles para portugues;
## 3. Transforma tipo date em timestamp;
## 4. Remover Acentos de uma Coluna;
## 4. Salva os dados na camada silver.
def tratamento(lt_atriburos: list, bronze_path, silver_path):
    df = spark.read.format(formato).load(bronze_path)
    df = df.select(lt_atriburos)\
        .withColumnRenamed(origemEnum.LOCATION_KEY.value, colunasRenomeadasEnum.LOCATION_KEY.value)\
        .withColumnRenamed(origemEnum.DATE.value, colunasRenomeadasEnum.DATE.value)\
        .withColumnRenamed(origemEnum.INFANT_MORTALITY_RATE.value, colunasRenomeadasEnum.INFANT_MORTALITY_RATE.value)\
        .withColumnRenamed(origemEnum.ADULT_FEMALE_MORTALITY_RATE.value, colunasRenomeadasEnum.ADULT_FEMALE_MORTALITY_RATE.value)\
        .withColumnRenamed(origemEnum.ADULT_MALE_MORTALITY_RATE.value, colunasRenomeadasEnum.ADULT_MALE_MORTALITY_RATE.value)\
        .withColumnRenamed(origemEnum.POPULATION.value, colunasRenomeadasEnum.POPULATION.value)\
        .withColumnRenamed(origemEnum.SUBREGION1_CODE.value, colunasRenomeadasEnum.SUBREGION1_CODE.value)\
        .withColumnRenamed(origemEnum.SUBREGION1_NAME.value, colunasRenomeadasEnum.SUBREGION1_NAME.value)\
        .withColumnRenamed(origemEnum.POPULATION_AGE_80_AND_OLDER.value, colunasRenomeadasEnum.POPULATION_AGE_80_AND_OLDER.value)\
        .withColumnRenamed(origemEnum.POPULATION_FEMALE.value, colunasRenomeadasEnum.POPULATION_FEMALE.value)\
        .withColumnRenamed(origemEnum.POPULATION_MALE.value, colunasRenomeadasEnum.POPULATION_MALE.value)\
        .withColumnRenamed(origemEnum.POPULATION_RURAL.value, colunasRenomeadasEnum.POPULATION_RURAL.value)\
        .withColumnRenamed(origemEnum.POPULATION_URBAN.value, colunasRenomeadasEnum.POPULATION_URBAN.value)\
        .withColumnRenamed(origemEnum.COUNTRY_CODE.value, colunasRenomeadasEnum.COUNTRY_CODE.value)\
        .withColumnRenamed(origemEnum.COUNTRY_NAME.value, colunasRenomeadasEnum.COUNTRY_NAME.value)\
        .withColumnRenamed(origemEnum.CUMULATIVE_CONFIRMED.value, colunasRenomeadasEnum.CUMULATIVE_CONFIRMED.value)\
        .withColumnRenamed(origemEnum.CUMULATIVE_DECEASED.value, colunasRenomeadasEnum.CUMULATIVE_DECEASED.value)\
        .withColumnRenamed(origemEnum.CUMULATIVE_RECOVERED.value, colunasRenomeadasEnum.CUMULATIVE_RECOVERED.value)\
        .withColumnRenamed(origemEnum.CUMULATIVE_TESTED.value, colunasRenomeadasEnum.CUMULATIVE_TESTED.value)\
        .withColumnRenamed(origemEnum.INGESTION_TIMESTAMP.value, colunasRenomeadasEnum.INGESTION_TIMESTAMP.value)
    def remover_acentos(texto):
        return unidecode(texto) if texto else None
    remover_acentos_udf = udf(remover_acentos, StringType())
    df = df.withColumn(colunasRenomeadasEnum.SUBREGION1_NAME.value, remover_acentos_udf(df[colunasRenomeadasEnum.SUBREGION1_NAME.value]))
    df.write.format(formato).mode(outputMode).save(silver_path)
    return print(f"{df.count()} linhas ingeridas com sucesso na camada silver!")

# COMMAND ----------

def processamento_gold(silver_path,gold_path):
    df = spark.read.format(formato).load(silver_path)
    df = df.groupBy(col(colunasRenomeadasEnum.COUNTRY_NAME.value))\
    .agg(sum(col(colunasRenomeadasEnum.CUMULATIVE_CONFIRMED.value)).alias(colunasRenomeadasEnum.TOTAL_CASES.value),
    sum(col(colunasRenomeadasEnum.CUMULATIVE_DECEASED.value)).alias(colunasRenomeadasEnum.TOTAL_DEATHS.value),
    sum(col(colunasRenomeadasEnum.CUMULATIVE_RECOVERED.value)).alias(colunasRenomeadasEnum.TOTAL_RECOVERED.value),
    round(avg(col(colunasRenomeadasEnum.CUMULATIVE_CONFIRMED.value)),2).alias(colunasRenomeadasEnum.AVEREGE_CASES.value),
    round(avg(col(colunasRenomeadasEnum.CUMULATIVE_DECEASED.value)),2).alias(colunasRenomeadasEnum.AVEREGE_DEATHS.value),
    round(avg(col(colunasRenomeadasEnum.CUMULATIVE_RECOVERED.value)),2).alias(colunasRenomeadasEnum.AVEREGE_RECOVERED.value))\
    .na.drop()\
    .orderBy(col(colunasRenomeadasEnum.COUNTRY_NAME.value))
    df.write.format(formato).mode(outputMode).save(gold_path)
    return print(f"{df.count()} linhas ingeridas com sucesso na camada gold!")

# COMMAND ----------

def relatorio_analitico(df: DataFrame):
  maior_total_casos = df.orderBy(df[colunasRenomeadasEnum.TOTAL_CASES.value].desc()).limit(1)
  menor_total_casos = df.orderBy(df[colunasRenomeadasEnum.TOTAL_CASES.value].asc()).limit(1)
  maior_total_mortes = df.orderBy(df[colunasRenomeadasEnum.TOTAL_DEATHS.value].desc()).limit(1)
  menor_total_mortes = df.orderBy(df[colunasRenomeadasEnum.TOTAL_DEATHS.value].asc()).limit(1)
  maior_total_recuperados = df.orderBy(df[colunasRenomeadasEnum.TOTAL_RECOVERED.value].desc()).limit(1)
  menor_total_recuperados = df.orderBy(df[colunasRenomeadasEnum.TOTAL_RECOVERED.value].asc()).limit(1)

  # Coletar o resultado e exibir com print
  print("Relatório Analítico sobre os Dados do COVID-19")
  print("=================================================================================================")
  resultado_01 = maior_total_casos.collect()[0]  # Pega a primeira linha
  resultado_02 = menor_total_casos.collect()[0]  # Pega a primeira linha
  resultado_03 = maior_total_mortes.collect()[0]  # Pega a primeira linha
  resultado_04 = menor_total_mortes.collect()[0]  # Pega a primeira linha
  resultado_05 = maior_total_recuperados.collect()[0]  # Pega a primeira linha
  resultado_06 = menor_total_recuperados.collect()[0]  # Pega a primeira linha
  print(f"País com maior quantidade de casos é: {resultado_01[colunasRenomeadasEnum.COUNTRY_NAME.value]} com {resultado_01[colunasRenomeadasEnum.TOTAL_CASES.value]} casos de COVID-19.")
  print(f"País com menor quantidade de casos é: {resultado_02[colunasRenomeadasEnum.COUNTRY_NAME.value]} com {resultado_02[colunasRenomeadasEnum.TOTAL_CASES.value]} casos de COVID-19.")
  print("=================================================================================================")
  print(f"País com maior quantidade de mostes é: {resultado_03[colunasRenomeadasEnum.COUNTRY_NAME.value]} com {resultado_03[colunasRenomeadasEnum.TOTAL_DEATHS.value]} casos de COVID-19.")
  print(f"País com menor quantidade de mostes é: {resultado_04[colunasRenomeadasEnum.COUNTRY_NAME.value]} com {resultado_04[colunasRenomeadasEnum.TOTAL_DEATHS.value]} casos de COVID-19.")
  print("=================================================================================================")
  print(f"País com maior quantidade de recuperados é: {resultado_05[colunasRenomeadasEnum.COUNTRY_NAME.value]} com {resultado_05[colunasRenomeadasEnum.TOTAL_RECOVERED.value]} casos de COVID-19.")
  print(f"País com menor quantidade de recuperados é: {resultado_06[colunasRenomeadasEnum.COUNTRY_NAME.value]} com {resultado_06[colunasRenomeadasEnum.TOTAL_RECOVERED.value]} casos de COVID-19.")
  print("=================================================================================================")
  media_casos = df.agg(avg(colunasRenomeadasEnum.TOTAL_CASES.value).alias("media_casos_geral")).collect()[0]["media_casos_geral"]
  print(f"Média Geral de casos de COVID-19: {media_casos:.0f}.")
  media_mortes = df.agg(avg(colunasRenomeadasEnum.TOTAL_DEATHS.value).alias("media_mortes_geral")).collect()[0]["media_mortes_geral"]
  print(f"Média Geral de mortes por COVID-19: {media_mortes:.0f}.")
  media_recuperados = df.agg(avg(colunasRenomeadasEnum.TOTAL_RECOVERED.value).alias("media_recuperados_geral")).collect()[0]["media_recuperados_geral"]
  print(f"Média Geral de recuperados do COVID-19: {media_recuperados:.0f}.")


# COMMAND ----------

def analise_grafica(df:DataFrame):
    # Agrega os totais
    df_totais = df.agg(
        sum(colunasRenomeadasEnum.TOTAL_CASES.value).alias(colunasRenomeadasEnum.TOTAL_CASES.value),
        sum(colunasRenomeadasEnum.TOTAL_DEATHS.value).alias(colunasRenomeadasEnum.TOTAL_DEATHS.value),
        sum(colunasRenomeadasEnum.TOTAL_RECOVERED.value).alias(colunasRenomeadasEnum.TOTAL_RECOVERED.value)
    )
    
    # Coleta os dados em um formato pandas DataFrame
    pandas_df = df.toPandas()
    totais = df_totais.collect()[0]

    # Configuração do subgráfico
    fig, axs = plt.subplots(2, 2, figsize=(16, 12), gridspec_kw={'height_ratios': [1, 1]})
    fig.suptitle('Análise de COVID-19 por País', fontsize=18)

    # Gráfico 1: Total de Casos
    pandas_df_sorted_cases = pandas_df.sort_values(by='total_casos', ascending=False)
    media_casos = pandas_df_sorted_cases['total_casos'].mean()
    bars_cases = axs[0, 0].barh(pandas_df_sorted_cases['nome_pais'], pandas_df_sorted_cases['total_casos'], color='skyblue', edgecolor='black')
    axs[0, 0].axvline(media_casos, color='red', linestyle='--', linewidth=2, label=f'Média: {media_casos:,.0f}')
    for bar in bars_cases:
        axs[0, 0].text(bar.get_width() + 3500000, bar.get_y() + bar.get_height() / 1.5,
                    f'{int(bar.get_width()):,}', va='center', ha='left', fontsize=10)
    axs[0, 0].set_title('Total de Casos de COVID-19 por País', fontsize=14)
    axs[0, 0].set_xlabel('Total de Casos', fontsize=12)
    axs[0, 0].invert_yaxis()
    axs[0, 0].grid(axis='x', linestyle='--', alpha=0.7)
    axs[0, 0].set_xticks([])  # Remove os valores do eixo x
    axs[0, 0].set_xlim(right=pandas_df_sorted_cases['total_casos'].max() * 1.2)  # Ajusta limite do eixo x
    axs[0, 0].legend(loc='lower right', fontsize=10)  # Adiciona legenda da média

    # Gráfico 2: Total de Mortes
    pandas_df_sorted_deaths = pandas_df.sort_values(by='total_mortes', ascending=False)
    media_deaths = pandas_df_sorted_deaths['total_mortes'].mean()
    bars_deaths = axs[0, 1].barh(pandas_df_sorted_deaths['nome_pais'], pandas_df_sorted_deaths['total_mortes'], color='salmon', edgecolor='black')
    axs[0, 1].axvline(media_deaths, color='red', linestyle='--', linewidth=2, label=f'Média: {media_deaths:,.0f}')
    for bar in bars_deaths:
        axs[0, 1].text(bar.get_width() + 20000, bar.get_y() + bar.get_height() / 2,
                    f'{int(bar.get_width()):,}', va='center', ha='left', fontsize=10)
    axs[0, 1].set_title('Total de Mortes de COVID-19 por País', fontsize=14)
    axs[0, 1].set_xlabel('Total de Mortes', fontsize=12)
    axs[0, 1].invert_yaxis()
    axs[0, 1].grid(axis='x', linestyle='--', alpha=0.7)
    axs[0, 1].set_xticks([])  # Remove os valores do eixo x
    axs[0, 1].set_xlim(right=pandas_df_sorted_deaths['total_mortes'].max() * 1.2)  # Ajusta limite do eixo x
    axs[0, 1].legend(loc='lower right', fontsize=10)  # Adiciona legenda da média

    # Gráfico 3: Total de Recuperados
    pandas_df_sorted_recovered = pandas_df.sort_values(by='total_recuperados', ascending=False)
    media_recovered = pandas_df_sorted_recovered['total_recuperados'].mean()
    bars_recovered = axs[1, 0].barh(pandas_df_sorted_recovered['nome_pais'], pandas_df_sorted_recovered['total_recuperados'], color='lightgreen', edgecolor='black')
    axs[1, 0].axvline(media_recovered, color='red', linestyle='--', linewidth=2, label=f'Média: {media_recovered:,.0f}')
    for bar in bars_recovered:
        axs[1, 0].text(bar.get_width() + 20000, bar.get_y() + bar.get_height() / 1.5,
                    f'{int(bar.get_width()):,}', va='center', ha='left', fontsize=10)
    axs[1, 0].set_title('Total de Recuperados de COVID-19 por País', fontsize=14)
    axs[1, 0].set_xlabel('Total de Recuperados', fontsize=12)
    axs[1, 0].invert_yaxis()
    axs[1, 0].grid(axis='x', linestyle='--', alpha=0.7)
    axs[1, 0].set_xticks([])  # Remove os valores do eixo x
    axs[1, 0].set_xlim(right=pandas_df_sorted_recovered['total_recuperados'].max() * 1.2)  # Ajusta limite do eixo x
    axs[1, 0].legend(loc='lower right', fontsize=10)  # Adiciona legenda da média

    # Gráfico 4: Gráfico de Pizza - Distribuição Mundial de Casos, Mortes e Recuperados
    labels = ['Total Casos', 'Total Mortes', 'Total Recuperados']
    sizes = [totais['total_casos'], totais['total_mortes'], totais['total_recuperados']]
    axs[1, 1].pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140, colors=['lightblue', 'salmon', 'lightgreen'])
    axs[1, 1].set_title('Distribuição Mundial de Casos, Mortes e Recuperados', fontsize=14)
    axs[1, 1].axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle

    # Ajusta layout
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])  # Ajusta o layout para que o título não sobreponha os subgráficos
    plt.show()

# COMMAND ----------

def analise_correlacao(df:DataFrame):
    # Agrega os totais por país
    df_agregado = df.groupBy(colunasRenomeadasEnum.COUNTRY_NAME.value).agg(
        sum(colunasRenomeadasEnum.TOTAL_CASES.value).alias(colunasRenomeadasEnum.TOTAL_CASES.value),
        sum(colunasRenomeadasEnum.TOTAL_DEATHS.value).alias(colunasRenomeadasEnum.TOTAL_DEATHS.value),
        sum(colunasRenomeadasEnum.TOTAL_RECOVERED.value).alias(colunasRenomeadasEnum.TOTAL_RECOVERED.value))

    # Coleta os dados em um formato pandas DataFrame
    pandas_df = df_agregado.toPandas()

    # Calcula a correlação
    correlacao = pandas_df[[colunasRenomeadasEnum.TOTAL_CASES.value,
                            colunasRenomeadasEnum.TOTAL_DEATHS.value,
                            colunasRenomeadasEnum.TOTAL_RECOVERED.value]].corr()

    # Exibe a matriz de correlação
    print("Matriz de Correlação:")
    print(correlacao)

# COMMAND ----------

def relatorio_geral(df: DataFrame):
    df = df.groupBy(colunasRenomeadasEnum.COUNTRY_NAME.value).agg(
        sum(colunasRenomeadasEnum.TOTAL_CASES.value).alias(colunasRenomeadasEnum.TOTAL_CASES.value),
        sum(colunasRenomeadasEnum.TOTAL_DEATHS.value).alias(colunasRenomeadasEnum.TOTAL_DEATHS.value),
        sum(colunasRenomeadasEnum.TOTAL_RECOVERED.value).alias(colunasRenomeadasEnum.TOTAL_RECOVERED.value),
        avg(colunasRenomeadasEnum.AVEREGE_CASES.value).alias(colunasRenomeadasEnum.AVEREGE_CASES.value),
        avg(colunasRenomeadasEnum.AVEREGE_DEATHS.value).alias(colunasRenomeadasEnum.AVEREGE_DEATHS.value),
        avg(colunasRenomeadasEnum.AVEREGE_RECOVERED.value).alias(colunasRenomeadasEnum.AVEREGE_RECOVERED.value)
    )
    # Exibe as informações por país
    return df.show(truncate=False)
