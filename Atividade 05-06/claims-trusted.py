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

def unicode_normalizer(word: str) -> str:
    return unidecode(word)
    
unaccent = udf(unicode_normalizer, StringType())

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

print('Trusted Claims')

args_list = ['file','bucket','trusted_bucket','database','tablename']
args = solve_args(args_list)

file = args['file']
ingestion_bucket = args['bucket']
trusted_bucket = args['trusted_bucket']
database = args['database']
table_name = args['tablename']
print(args)

filename = file.split('/')[-1]
trusted_bucket = f's3://{trusted_bucket}'
print(f'trusted_bucket: {trusted_bucket}')

filenames_list = []
table_exists = spark.sql(f"SHOW TABLES IN {database} LIKE '{table_name}'").count() > 0
print(f'Does table {database}.{table_name} exists? {table_exists}')

if table_exists:
    partitions_df = spark.sql(f"SHOW PARTITIONS {database}.{table_name}")
    filenames_list = [row[0].split('=')[1] for row in partitions_df.collect()]

print('filenames_list:', filenames_list)

if filename not in filenames_list:
    df = spark.sql(f"""SELECT * FROM rwzd.{table_name} WHERE filename = '{filename}'""")
    
    df = df \
        .withColumnRenamed('Ano', 'year_claim') \
        .withColumnRenamed('Trimestre', 'quarter_claim') \
        .withColumnRenamed('Categoria', 'category') \
        .withColumnRenamed('Tipo', 'bank_type') \
        .withColumnRenamed('CNPJ IF', 'cnpj') \
        .withColumnRenamed('Instituição financeira', 'financial_institution_name') \
        .withColumnRenamed('Índice', 'bank_index') \
        .withColumnRenamed('Quantidade de reclamações reguladas procedentes', 'number_of_regulated_proceeding_complaints') \
        .withColumnRenamed('Quantidade de reclamações reguladas - outras', 'number_of_regulated_other_complaints') \
        .withColumnRenamed('Quantidade de reclamações não reguladas', 'number_of_unregulated_complaints') \
        .withColumnRenamed('Quantidade total de reclamações', 'total_number_of_complaints') \
        .withColumnRenamed('Quantidade total de clientes  CCS e SCR', 'total_number_of_ccs_and_scr_customers') \
        .withColumnRenamed('Quantidade de clientes  CCS', 'number_of_ccs_customers') \
        .withColumnRenamed('Quantidade de clientes  SCR', 'number_of_scr_customers')
    
    df = df \
        .withColumn('bank_index', regexp_replace(col('bank_index'), r'\.', '')) \
        .withColumn('bank_index', regexp_replace(col('bank_index'), r',', '.')) \
        .withColumn('bank_index', regexp_replace(col('bank_index'), r' ', '')) \
        .withColumn("bank_index", col("bank_index").cast(FloatType()))
        
    df = df.withColumn('quarter_claim', regexp_replace(col('quarter_claim'), 'º', ''))
    
    df = df \
        .withColumn('cnpj', when(col('cnpj') == ' ', lit(None).cast(StringType())) \
        .otherwise(lpad(col('cnpj'), 8, '0')))
        
    df = df \
        .withColumn('total_number_of_ccs_and_scr_customers', regexp_replace(col('total_number_of_ccs_and_scr_customers'), r' ', '')) \
        .withColumn("total_number_of_ccs_and_scr_customers", col("total_number_of_ccs_and_scr_customers").cast(IntegerType()))

    df = df \
        .withColumn('number_of_ccs_customers', regexp_replace(col('number_of_ccs_customers'), r' ', '')) \
        .withColumn("number_of_ccs_customers", col("number_of_ccs_customers").cast(IntegerType()))

    df = df \
        .withColumn('number_of_scr_customers', regexp_replace(col('number_of_scr_customers'), r' ', '')) \
        .withColumn("number_of_scr_customers", col("number_of_scr_customers").cast(IntegerType()))

    df = df.withColumn('financial_institution_name', regexp_replace(col('financial_institution_name'), r' \(conglomerado\)', ''))
    
    bank_names = {
        'BB': 'BANCO DO BRASIL',
        'DAYCOVAL': 'BANCO DAYCOVAL S.A',
        'DEUTSCHE BANK S.A. - BANCO ALEMAO': 'DEUTSCHE',
        'BANCO SUMITOMO MITSUI BRASILEIRO S.A.': 'BANCO SUMITOMO MITSUI BRASIL S.A.'
    }
    
    df = df.na.replace(to_replace=bank_names, subset='financial_institution_name')
    
    df = df \
        .withColumn('sk_financial_institution_name', unaccent(col('financial_institution_name'))) \
        .withColumn('sk_financial_institution_name', lower(col('sk_financial_institution_name'))) \
        .withColumn('sk_financial_institution_name', regexp_replace(col('sk_financial_institution_name'), r' ', '')) \
        .withColumn('sk_financial_institution_name', sha1(col('sk_financial_institution_name')))
        
    df = df \
        .withColumn('quarter_claim', col('quarter_claim').cast(IntegerType())) \
        .withColumn('number_of_regulated_proceeding_complaints', col('number_of_regulated_proceeding_complaints').cast(IntegerType())) \
        .withColumn('number_of_regulated_other_complaints', col('number_of_regulated_other_complaints').cast(IntegerType())) \
        .withColumn('number_of_unregulated_complaints', col('number_of_unregulated_complaints').cast(IntegerType())) \
        .withColumn('total_number_of_complaints', col('total_number_of_complaints').cast(IntegerType()))
    
    df.printSchema()
    
    count = df.count()
    print(f'count: {count}')
    s3_path = f'{trusted_bucket}/{database}/{table_name}/'
    if count > 0:
        save_parquet(df, s3_path, database, table_name)
    print('End.')
else:
    print(f"Filename '{filename}' já existe na tabela. Salvamento ignorado.")

job.commit()