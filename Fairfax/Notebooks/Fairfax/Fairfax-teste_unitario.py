# Databricks notebook source
# MAGIC %run /Workspace/Fairfax/Notebooks/Fairfax/Fairfax-metodos

# COMMAND ----------

# MAGIC %md
# MAGIC ###Testes

# COMMAND ----------

# DBTITLE 1,teste transient_to_bronze
# 1. Importar as bibliotecas necessárias
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# 2. Criar uma sessão Spark
spark = SparkSession.builder \
    .appName("TestTransientToBronze") \
    .getOrCreate()

# 3. Criar um DataFrame de exemplo
data = [("exemplo1", 1), ("exemplo2", 2)]
columns = ["nome", "valor"]
test_df = spark.createDataFrame(data, columns)

# 4. Definir os caminhos para os dados de teste
transient_path = "/mnt/transient/test_data.csv"
bronze_path = "/mnt/bronze/test_data"

# 5. Salvar o DataFrame de exemplo na camada transitória
test_df.write.csv(f'dbfs:{transient_path}', header=True, mode='overwrite')

# 6. Função que você deseja testar
def transient_to_bronze(transient_path, bronze_path):
    df = spark.read.csv(f'dbfs:{transient_path}', header=True, inferSchema=True)
    data_hora_sao_paulo = spark.sql("SELECT to_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') as data_hora").collect()[0]['data_hora']
    df = df.withColumn("ingestion_timestamp", lit(data_hora_sao_paulo))
    df.write.format("delta").mode("overwrite").save(bronze_path)
    print(f"{df.count()} arquivo ingerido com sucesso na camada bronze em formato Delta!")
    return dbutils.fs.rm(f'dbfs:{transient_path}', True)

# 7. Executar o método e verificar os resultados
transient_to_bronze(transient_path, bronze_path)

# 8. Verificar se os dados foram escritos na camada bronze
bronze_df = spark.read.format("delta").load(bronze_path)
print(f"Número de registros na camada bronze: {bronze_df.count()}")

# 9. Limpar os dados de teste (opcional)
dbutils.fs.rm(f'dbfs:{transient_path}', True)
dbutils.fs.rm(bronze_path, True)


# COMMAND ----------

# DBTITLE 1,teste-pre_analise
# 1. Criar um DataFrame de exemplo
data = [("exemplo1", 1, None), ("exemplo2", 2, 3), ("exemplo3", None, 5)]
columns = ["nome", "valor", "descricao"]
test_df = spark.createDataFrame(data, columns)

# 2. Caminho da camada bronze para testes
bronze_path = "/mnt/bronze/test_data"

# 3. Escrever o DataFrame na camada bronze
test_df.write.format("delta").mode("overwrite").save(bronze_path)

# 4. Chamar a função de pré-análise
pre_analise(bronze_path)

# 5. Limpar os dados de teste (opcional)
dbutils.fs.rm(bronze_path, True)


# COMMAND ----------

# DBTITLE 1,teste- tratamento
# Importações e inicialização
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from pyspark.sql.functions import udf
from unidecode import unidecode
from datetime import datetime

# Inicializa a sessão Spark
spark = SparkSession.builder \
    .appName("Test Tratamento") \
    .getOrCreate()

# Definição dos enums fictícios
class origemEnum:
    LOCATION_KEY = "LOCATION_KEY"
    DATE = "DATE"
    INFANT_MORTALITY_RATE = "INFANT_MORTALITY_RATE"
    ADULT_FEMALE_MORTALITY_RATE = "ADULT_FEMALE_MORTALITY_RATE"
    POPULATION = "POPULATION"
    SUBREGION1_NAME = "SUBREGION1_NAME"
    INGESTION_TIMESTAMP = "INGESTION_TIMESTAMP"

class colunasRenomeadasEnum:
    LOCATION_KEY = "location_key"
    DATE = "date"
    INFANT_MORTALITY_RATE = "infant_mortality_rate"
    ADULT_FEMALE_MORTALITY_RATE = "adult_female_mortality_rate"
    POPULATION = "population"
    SUBREGION1_NAME = "subregion1_name"
    INGESTION_TIMESTAMP = "ingestion_timestamp"

# Função de tratamento
def tratamento(df, lt_atriburos):
    df = df.select(lt_atriburos)\
        .withColumnRenamed(origemEnum.LOCATION_KEY, colunasRenomeadasEnum.LOCATION_KEY)\
        .withColumnRenamed(origemEnum.DATE, colunasRenomeadasEnum.DATE)\
        .withColumnRenamed(origemEnum.INFANT_MORTALITY_RATE, colunasRenomeadasEnum.INFANT_MORTALITY_RATE)\
        .withColumnRenamed(origemEnum.ADULT_FEMALE_MORTALITY_RATE, colunasRenomeadasEnum.ADULT_FEMALE_MORTALITY_RATE)\
        .withColumnRenamed(origemEnum.POPULATION, colunasRenomeadasEnum.POPULATION)\
        .withColumnRenamed(origemEnum.SUBREGION1_NAME, colunasRenomeadasEnum.SUBREGION1_NAME)\
        .withColumnRenamed(origemEnum.INGESTION_TIMESTAMP, colunasRenomeadasEnum.INGESTION_TIMESTAMP)

    def remover_acentos(texto):
        return unidecode(texto) if texto else None

    remover_acentos_udf = udf(remover_acentos, StringType())
    df = df.withColumn(colunasRenomeadasEnum.SUBREGION1_NAME, remover_acentos_udf(df[colunasRenomeadasEnum.SUBREGION1_NAME]))

    return df

# Célula 3: Teste da função de tratamento
# Criação de um DataFrame de exemplo
schema = StructType([
    StructField(origemEnum.LOCATION_KEY, StringType(), True),
    StructField(origemEnum.DATE, StringType(), True),
    StructField(origemEnum.INFANT_MORTALITY_RATE, FloatType(), True),
    StructField(origemEnum.ADULT_FEMALE_MORTALITY_RATE, FloatType(), True),
    StructField(origemEnum.POPULATION, FloatType(), True),
    StructField(origemEnum.SUBREGION1_NAME, StringType(), True),
    StructField(origemEnum.INGESTION_TIMESTAMP, TimestampType(), True)
])

data = [
    Row("LOC1", "2021-01-01", 5.0, 2.0, 1000.0, "São Paulo", datetime.now()),
    Row("LOC2", "2021-01-02", 4.0, 1.0, 1500.0, "Rio de Janeiro", datetime.now())
]

df_bronze = spark.createDataFrame(data, schema)

# Atributos a serem selecionados
lista_de_atributos = [
    origemEnum.LOCATION_KEY,
    origemEnum.DATE,
    origemEnum.INFANT_MORTALITY_RATE,
    origemEnum.ADULT_FEMALE_MORTALITY_RATE,
    origemEnum.POPULATION,
    origemEnum.SUBREGION1_NAME,
    origemEnum.INGESTION_TIMESTAMP
]

# Executando a função de tratamento
df_resultado = tratamento(df_bronze, lista_de_atributos)

# Verificações
assert df_resultado.count() == 2, "O número de linhas não está correto."
assert colunasRenomeadasEnum.LOCATION_KEY in df_resultado.columns, "Coluna LOCATION_KEY não renomeada corretamente."
assert df_resultado.filter(df_resultado[colunasRenomeadasEnum.SUBREGION1_NAME] == "Sao Paulo").count() == 1, "Acentos não foram removidos corretamente."

print("Todos os testes passaram com sucesso!")

# COMMAND ----------

# DBTITLE 1,teste-processamento_gold
# Célula 2: Importações e inicialização
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import col, sum, avg, round

# Inicializa a sessão Spark
spark = SparkSession.builder \
    .appName("Test Processamento Gold") \
    .getOrCreate()

# Definição dos enums fictícios
class colunasRenomeadasEnum:
    COUNTRY_NAME = "country_name"
    CUMULATIVE_CONFIRMED = "cumulative_confirmed"
    CUMULATIVE_DECEASED = "cumulative_deceased"
    CUMULATIVE_RECOVERED = "cumulative_recovered"
    TOTAL_CASES = "total_cases"
    TOTAL_DEATHS = "total_deaths"
    TOTAL_RECOVERED = "total_recovered"
    AVEREGE_CASES = "average_cases"
    AVEREGE_DEATHS = "average_deaths"
    AVEREGE_RECOVERED = "average_recovered"

formato = "delta"  # Defina seu formato, pode ser "delta", "parquet", etc.
outputMode = "overwrite"  # Ou "append", conforme necessário

# Função de processamento gold
def processamento_gold(silver_path, gold_path):
    df = spark.read.format(formato).load(silver_path)
    df = df.groupBy(col(colunasRenomeadasEnum.COUNTRY_NAME))\
        .agg(
            sum(col(colunasRenomeadasEnum.CUMULATIVE_CONFIRMED)).alias(colunasRenomeadasEnum.TOTAL_CASES),
            sum(col(colunasRenomeadasEnum.CUMULATIVE_DECEASED)).alias(colunasRenomeadasEnum.TOTAL_DEATHS),
            sum(col(colunasRenomeadasEnum.CUMULATIVE_RECOVERED)).alias(colunasRenomeadasEnum.TOTAL_RECOVERED),
            round(avg(col(colunasRenomeadasEnum.CUMULATIVE_CONFIRMED)), 2).alias(colunasRenomeadasEnum.AVEREGE_CASES),
            round(avg(col(colunasRenomeadasEnum.CUMULATIVE_DECEASED)), 2).alias(colunasRenomeadasEnum.AVEREGE_DEATHS),
            round(avg(col(colunasRenomeadasEnum.CUMULATIVE_RECOVERED)), 2).alias(colunasRenomeadasEnum.AVEREGE_RECOVERED)
        )\
        .na.drop()\
        .orderBy(col(colunasRenomeadasEnum.COUNTRY_NAME))
    
    df.write.format(formato).mode(outputMode).save(gold_path)
    return print(f"{df.count()} linhas ingeridas com sucesso na camada gold!")

# Célula 3: Teste da função de processamento gold
# Criação de um DataFrame de exemplo
schema = StructType([
    StructField(colunasRenomeadasEnum.COUNTRY_NAME, StringType(), True),
    StructField(colunasRenomeadasEnum.CUMULATIVE_CONFIRMED, FloatType(), True),
    StructField(colunasRenomeadasEnum.CUMULATIVE_DECEASED, FloatType(), True),
    StructField(colunasRenomeadasEnum.CUMULATIVE_RECOVERED, FloatType(), True)
])

data = [
    Row("Country A", 100.0, 10.0, 50.0),
    Row("Country A", 200.0, 20.0, 100.0),
    Row("Country B", 150.0, 15.0, 75.0)
]

# Criando DataFrame de exemplo no formato silver
silver_df = spark.createDataFrame(data, schema)
silver_path = "/tmp/silver_data"  # Ajuste o caminho conforme necessário
silver_df.write.format(formato).mode("overwrite").save(silver_path)

# Executando a função de processamento gold
gold_path = "/tmp/gold_data"  # Ajuste o caminho conforme necessário
processamento_gold(silver_path, gold_path)

# Verificando o resultado
gold_df = spark.read.format(formato).load(gold_path)
gold_df.show()

# Verificando as contagens
assert gold_df.count() == 2, "O número de linhas no DataFrame gold não está correto."
assert colunasRenomeadasEnum.TOTAL_CASES in gold_df.columns, "Coluna TOTAL_CASES não encontrada no DataFrame gold."
dbutils.fs.rm(silver_path, True)
dbutils.fs.rm(gold_path, True)
