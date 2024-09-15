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

def save_parquet(df, path, target_database, target_table):
    print('Writing table...')
    df.write \
    .mode('append') \
    .partitionBy('filename') \
    .format('parquet') \
    .option('compression', 'snappy') \
    .option('path', path) \
    .saveAsTable(f'{target_database}.{target_table}')

def solve_args(args_list):
    return getResolvedOptions(sys.argv, args_list)
    
print('Raw Claims')

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

print(f'raw_bucket: {raw_bucket}')
print(f'ingestion_bucket_path: {ingestion_bucket_path}')

filenames_list = []
table_exists = spark.sql(f"SHOW TABLES IN {database} LIKE '{table_name}'").count() > 0
print(f'Does table {database}.{table_name} exists? {table_exists}')

if table_exists:
    partitions_df = spark.sql(f"SHOW PARTITIONS {database}.{table_name}")
    filenames_list = [row[0].split('=')[1] for row in partitions_df.collect()]

print('filenames_list:', filenames_list)

if filename not in filenames_list:
    df = spark.read.csv(ingestion_bucket_path, sep=';', encoding='latin1', header=True)
    df = df.drop('_c14')
    df = df.withColumn('filename', lit(filename))
    
    df.printSchema()
    
    count = df.count()
    print(f'count: {count}')
    s3_path = f'{raw_bucket}/{database}/{table_name}/'
    if count > 0:
        save_parquet(df, s3_path, database, table_name)
    print('End.')
else:
    print(f"Filename '{filename}' já existe na tabela. Salvamento ignorado.")

job.commit()