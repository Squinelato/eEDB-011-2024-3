import json
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

with open('credentials.json', 'r') as f:
    credentials = json.load(f)

url = credentials['url']
username = credentials['user']
password = credentials['pass']
kafka_server = credentials['kafka_server']
topic = credentials['topic']

spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .config("spark.sql.warehouse.dir", "file:///C:/temp") \
    .config("spark.jars", "C:/Users/mdeso/Documents/mysql-connector-j-9.0.0/mysql-connector-j-9.0.0.jar") \
    .getOrCreate()

schema = StructType([
    StructField("Ano", StringType(), True),
    StructField("Trimestre", StringType(), True),
    StructField("Categoria", StringType(), True),
    StructField("Tipo", StringType(), True),
    StructField("CNPJ", StringType(), True),  # "CNPJ IF" 
    StructField("Instituicao", StringType(), True),
    StructField("Indice", StringType(), True),
    StructField("Reclamacoes_Procedentes", IntegerType(), True),
    StructField("Reclamacoes_Outras", IntegerType(), True),
    StructField("Reclamacoes_Nao_Reguladas", IntegerType(), True),
    StructField("Total_Reclamacoes", IntegerType(), True),
    StructField("Clientes_CCS_SCR", IntegerType(), True),
    StructField("Clientes_CCS", IntegerType(), True),
    StructField("Clientes_SCR", IntegerType(), True),
    StructField("_14", StringType(), True)
])

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_server) \
    .option("subscribe", topic) \
    .load()

df_string = df.selectExpr("CAST(value AS STRING) as csv_value")
df_split = df_string.withColumn("csv_cols", split(col("csv_value"), ";"))

for i, column_name in enumerate(["Ano", "Trimestre", "Categoria", "Tipo", "CNPJ", "Instituicao", "Indice", 
                                 "Reclamacoes_Procedentes", "Reclamacoes_Outras", "Reclamacoes_Nao_Reguladas",
                                 "Total_Reclamacoes", "Clientes_CCS_SCR", "Clientes_CCS", "Clientes_SCR", "_14"]):
    df_split = df_split.withColumn(column_name, col("csv_cols")[i])

source_table_name = "trusted_banks"

df_enriched = df_split.join(
    spark.read.format("jdbc")
    .option("url", url)
    .option("dbtable", source_table_name)
    .option("user", username)
    .option("password", password)
    .load(),
    df_split.CNPJ == col(f'{source_table_name}.cnpj'),
    how="left"
)

query = df_enriched \
    .writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "file:///C:/temp/enriched_messages/") \
    .option("checkpointLocation", "file:///C:/temp/checkpoints/") \
    .trigger(processingTime='10 seconds') \
    .start()

try:
    query.awaitTermination()
except Exception as e:
    print(f"Error occurred: {e}")
