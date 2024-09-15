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

print('Trusted Employees')

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
        .withColumnRenamed('employer-website', 'employer_website') \
        .withColumnRenamed('employer-headquarters', 'employer_headquarters') \
        .withColumnRenamed('employer-founded', 'employer_founded') \
        .withColumnRenamed('employer-industry', 'employer_industry') \
        .withColumnRenamed('employer-revenue', 'employer_revenue') \
        .withColumnRenamed('Geral', 'general_score') \
        .withColumnRenamed('Cultura e valores', 'culture_values_score') \
        .withColumnRenamed('Diversidade e inclusão', 'diversity_inclusion_score') \
        .withColumnRenamed('Qualidade de vida', 'life_quality_score') \
        .withColumnRenamed('Alta liderança', 'senior_leadership_score') \
        .withColumnRenamed('Remuneração e benefícios', 'compensation_benefits_score') \
        .withColumnRenamed('Oportunidades de carreira', 'career_opportunities_score') \
        .withColumnRenamed('Recomendam para outras pessoas(%)', 'recommendation_score') \
        .withColumnRenamed('Perspectiva positiva da empresa(%)', 'company_positive_score') \
        .withColumnRenamed('Segmento', 'segment') \
        .withColumnRenamed('Nome', 'financial_institution_name')
        
    df = df.withColumn('employer_founded', regexp_replace(col('employer_founded'), r'\..*', ''))
    
    df = df \
        .withColumn('sk_financial_institution_name', unaccent(col('financial_institution_name'))) \
        .withColumn('sk_financial_institution_name', lower(col('sk_financial_institution_name'))) \
        .withColumn('sk_financial_institution_name', regexp_replace(col('sk_financial_institution_name'), r' ', '')) \
        .withColumn('sk_financial_institution_name', sha1(col('sk_financial_institution_name')))

    df = df \
        .withColumn('reviews_count', col('reviews_count').cast(IntegerType())) \
        .withColumn('culture_count', col('culture_count').cast(IntegerType())) \
        .withColumn('salaries_count', col('salaries_count').cast(IntegerType())) \
        .withColumn('benefits_count', col('benefits_count').cast(IntegerType())) \
        .withColumn('general_score', col('general_score').cast(FloatType())) \
        .withColumn('culture_values_score', col('culture_values_score').cast(FloatType())) \
        .withColumn('diversity_inclusion_score', col('diversity_inclusion_score').cast(FloatType())) \
        .withColumn('life_quality_score', col('life_quality_score').cast(FloatType())) \
        .withColumn('senior_leadership_score', col('senior_leadership_score').cast(FloatType())) \
        .withColumn('compensation_benefits_score', col('compensation_benefits_score').cast(FloatType())) \
        .withColumn('career_opportunities_score', col('career_opportunities_score').cast(FloatType())) \
        .withColumn('recommendation_score', col('recommendation_score').cast(FloatType())) \
        .withColumn('company_positive_score', col('company_positive_score').cast(FloatType())) \
        .withColumn('match_percent', col('match_percent').cast(IntegerType()))
    
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