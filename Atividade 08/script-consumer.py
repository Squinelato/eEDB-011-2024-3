import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, lpad
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os

os.environ['HADOOP_HOME'] = 'C:/hadoop'
os.environ['PATH'] += os.pathsep + 'C:/hadoop/bin'

with open('credentials.json', 'r') as f:
    credentials = json.load(f)

url = credentials['url']
username = credentials['user']
password = credentials['pass']
kafka_server = credentials['kafka_server']
topic = credentials['topic']

spark = SparkSession.builder \
    .appName('KafkaConsumer') \
    .config('spark.sql.warehouse.dir', 'file:///C:/tmp') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,mysql:mysql-connector-java:8.0.30') \
    .config('spark.driver.extraClassPath', 'C:/Users/mdeso/.ivy2/jars/mysql_mysql-connector-java-8.0.30.jar') \
    .config('spark.hadoop.io.native.lib.available', 'false') \
    .config("spark.hadoop.io.nativeio", "false") \
    .config("spark.cleaner.referenceTracking.cleanCheckpoints", "false") \
    .getOrCreate()

schema = StructType([
    StructField('Ano', StringType(), True),
    StructField('Trimestre', StringType(), True),
    StructField('Categoria', StringType(), True),
    StructField('Tipo', StringType(), True),
    StructField('CNPJ', StringType(), True),  # 'CNPJ IF' 
    StructField('Instituicao', StringType(), True),
    StructField('Indice', StringType(), True),
    StructField('Reclamacoes_Procedentes', IntegerType(), True),
    StructField('Reclamacoes_Outras', IntegerType(), True),
    StructField('Reclamacoes_Nao_Reguladas', IntegerType(), True),
    StructField('Total_Reclamacoes', IntegerType(), True),
    StructField('Clientes_CCS_SCR', IntegerType(), True),
    StructField('Clientes_CCS', IntegerType(), True),
    StructField('Clientes_SCR', IntegerType(), True)
])

df = spark \
    .readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', kafka_server) \
    .option('subscribe', topic) \
    .load()

df_string = df.selectExpr('CAST(value AS STRING) as csv_value')
df_split = df_string.withColumn('csv_cols', split(col('csv_value'), ';'))

columns = ['Ano', 'Trimestre', 'Categoria', 'Tipo', 'CNPJ', 'Instituicao', 'Indice', 
           'Reclamacoes_Procedentes', 'Reclamacoes_Outras', 'Reclamacoes_Nao_Reguladas',
           'Total_Reclamacoes', 'Clientes_CCS_SCR', 'Clientes_CCS', 'Clientes_SCR']

for i, column_name in enumerate(columns):
    df_split = df_split.withColumn(column_name, col('csv_cols')[i])

df_split = df_split.withColumn('CNPJ', lpad(col('CNPJ'), 8, '0'))

df_banks = spark.read.format('jdbc') \
    .option('url', url) \
    .option('dbtable', '(SELECT cnpj, segment, financial_institution_name FROM eedb011.trusted_banks) as trusted_banks') \
    .option('user', username) \
    .option('password', password) \
    .load()

print(df_banks.show())

df_joined = df_split.join(df_banks, df_split['CNPJ'] == df_banks['cnpj'], 'left')

query = df_joined.writeStream \
    .outputMode('append') \
    .format('csv') \
    .option('path', './enriched_messages/') \
    .option('checkpointLocation', './checkpoints/') \
    .start()

query.awaitTermination()

spark.stop()
