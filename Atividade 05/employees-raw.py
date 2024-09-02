import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import json
from unidecode import unidecode
from pyspark.sql.functions import lpad, col, lpad, concat, sha1, regexp_replace, udf, lower, lit, when
from pyspark.sql.types import StringType, FloatType, IntegerType

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()

def save_parquet(df, path, target_database, target_table):
    df.write \
    .mode('append') \
    .partitionBy('filename') \
    .format('parquet') \
    .option('compression', 'snappy') \
    .option('path', path) \
    .saveAsTable(f'{target_database}.{target_table}')

def solve_args(args_list):
    return getResolvedOptions(sys.argv, args_list)

print('Raw Employees')

args_list = ['file','bucket','raw_bucket','database','tablename']
args = solve_args(args_list)

file = args['file']
ingestion_bucket = args['bucket']
raw_bucket = args['raw_bucket']
database = args['database']
table_name = args['tablename']
print(args)

filename = file.split('/')[-1]
raw_bucket = f's3://{raw_bucket}'
ingestion_bucket_path = f's3://{ingestion_bucket}/{file}'

print(raw_bucket)
print(ingestion_bucket_path)

df = spark.read.csv(ingestion_bucket_path, sep='|', encoding='utf8', header=True)
df = df.dropDuplicates(['Nome', 'Segmento'])
df = df.withColumn('filename', lit(filename))

df.printSchema()

filenames_list = list()

if spark.catalog.tableExists(f"{database}.{table_name}"):
    df_filenames = spark.sql(f"SELECT DISTINCT filename FROM {database}.{table_name}")
    filenames_list = [row['filename'] for row in df_filenames.collect()]

print('filenames_list:', filenames_list)

if filename not in filenames_list:
    count = df.count()
    print(f'count: {count}')
    s3_path = f'{raw_bucket}/{database}/{table_name}/'
    save_parquet(df, s3_path, database, table_name)
else:
    print(f"Filename '{filename}' já existe na tabela. Salvamento ignorado.")