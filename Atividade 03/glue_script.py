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

client = boto3.session.Session().client(service_name='secretsmanager')
secret_string = client.get_secret_value(SecretId='mydb')['SecretString']
secret_dict = json.loads(secret_string)

host = secret_dict['host']
port = secret_dict['port']
username = secret_dict['username']
password = secret_dict['password']
database = 'mydb'

raw_bucket = 's3://eedb-g2-raw'
trusted_bucket = 's3://eedb-g2-trusted'
ingestion_bucket_path = 's3://eedb-g2/eedb-011/Dados'

jdbc_url = f'jdbc:{host}:{port}/{database}'
jdbc_properties = {
    'user': username,
    'password': password,
    'driver': 'com.mysql.cj.jdbc.Driver'
}

def save_parquet(df, path):
    df.write.parquet(path, mode="overwrite")

###############################################################################################
############################                 RAW                   ############################
###############################################################################################


###########################                BANKS                 ##############################

print('raw banks')

table_name = 'rwzd_bank'
banks_path = f'{ingestion_bucket_path}/Bancos/EnquadramentoInicia_v2.tsv'
rwzd_bank = spark.read.csv(banks_path, sep='\t', encoding='utf8', header=True)

rwzd_bank.printSchema()

count_rwzd_bank = rwzd_bank.count()
print(f'count_rwzd_bank: {count_rwzd_bank}')

s3_path = f'{raw_bucket}/{table_name}/'
save_parquet(rwzd_bank, s3_path)

###########################              Employees               ##############################

print('raw employee')

table_name = 'rwzd_employee'
employee_path = f'{ingestion_bucket_path}/Empregados/glassdoor_consolidado_join_match_v2.csv'
rwzd_employee = spark.read.csv(employee_path, sep='|', encoding='utf8', header=True)

rwzd_employee = rwzd_employee.dropDuplicates(['Nome', 'Segmento'])

rwzd_employee.printSchema()

count_rwzd_employee = rwzd_employee.count()
print(f'count_rwzd_employee: {count_rwzd_employee}')

s3_path = f'{raw_bucket}/{table_name}/'
save_parquet(rwzd_employee, s3_path)

###########################               Claims                  ##############################

print('raw claim')

table_name = 'rwzd_claim'
claim_path = f'{ingestion_bucket_path}/Reclamacoes/'
rwzd_claim = spark.read.csv(claim_path, sep=';', encoding='latin1', header=True)

rwzd_employee.printSchema()

rwzd_claim = rwzd_claim.drop('_c14')

count_rwzd_claim = rwzd_claim.count()
print(f'count_rwzd_claim: {count_rwzd_claim}')

s3_path = f'{raw_bucket}/{table_name}/'
save_parquet(rwzd_claim, s3_path)

###############################################################################################
############################               TRUSTED                 ############################
###############################################################################################

def unicode_normalizer(word: str) -> str:
    return unidecode(word)
    
unaccent = udf(unicode_normalizer, StringType())


###########################                BANKS                 ##############################

print('trusted banks')

table_name = 'trzd_bank'
trzd_bank = rwzd_bank

trzd_bank = trzd_bank.withColumnRenamed('Segmento', 'segment') \
       .withColumnRenamed('CNPJ', 'cnpj') \
       .withColumnRenamed('Nome', 'financial_institution_name')

trzd_bank = trzd_bank \
    .withColumn('cnpj', when(col('cnpj') == '', lit(None).cast(StringType())) \
    .otherwise(lpad(col('cnpj'), 8, '0')))
    
trzd_bank = trzd_bank.withColumn('sk_cnpj_segment', sha1(concat(col('cnpj'), col('segment'))))

s3_path = f'{trusted_bucket}/{table_name}/'
save_parquet(trzd_bank, s3_path)

trzd_bank.show(truncate=False)

###########################              Employees               ##############################

print('trusted employee')

table_name = 'trzd_employee'
trzd_employee = rwzd_employee

trzd_employee = trzd_employee.withColumnRenamed('employer-website', 'employer_website') \
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

trzd_employee = trzd_employee.withColumn('employer_founded', regexp_replace(col('employer_founded'), r'\..*', ''))

trzd_employee = trzd_employee \
    .withColumn('sk_financial_institution_name', unaccent(col('financial_institution_name'))) \
    .withColumn('sk_financial_institution_name', lower(col('sk_financial_institution_name'))) \
    .withColumn('sk_financial_institution_name', regexp_replace(col('sk_financial_institution_name'), r' ', '')) \
    .withColumn('sk_financial_institution_name', sha1(col('sk_financial_institution_name')))
    
trzd_employee = trzd_employee \
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

s3_path = f'{trusted_bucket}/{table_name}/'
save_parquet(trzd_employee, s3_path)

trzd_employee.show(truncate=False)

###########################               Claims                  ##############################

print('trusted employee')

table_name = 'trzd_claim'
trzd_claim = rwzd_claim

trzd_claim = trzd_claim.withColumnRenamed('Ano', 'year_claim') \
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
       
trzd_claim = trzd_claim \
    .withColumn('bank_index', regexp_replace(col('bank_index'), r'\.', '')) \
    .withColumn('bank_index', regexp_replace(col('bank_index'), r',', '.')) \
    .withColumn('bank_index', regexp_replace(col('bank_index'), r' ', '')) \
    .withColumn("bank_index", col("bank_index").cast(FloatType()))
    
trzd_claim = trzd_claim.withColumn('quarter_claim', regexp_replace(col('quarter_claim'), 'º', ''))

trzd_claim = trzd_claim \
    .withColumn('cnpj', when(col('cnpj') == ' ', lit(None).cast(StringType())) \
    .otherwise(lpad(col('cnpj'), 8, '0')))
    
trzd_claim = trzd_claim \
    .withColumn('total_number_of_ccs_and_scr_customers', regexp_replace(col('total_number_of_ccs_and_scr_customers'), r' ', '')) \
    .withColumn("total_number_of_ccs_and_scr_customers", col("total_number_of_ccs_and_scr_customers").cast(IntegerType()))

trzd_claim = trzd_claim \
    .withColumn('number_of_ccs_customers', regexp_replace(col('number_of_ccs_customers'), r' ', '')) \
    .withColumn("number_of_ccs_customers", col("number_of_ccs_customers").cast(IntegerType()))

trzd_claim = trzd_claim \
    .withColumn('number_of_scr_customers', regexp_replace(col('number_of_scr_customers'), r' ', '')) \
    .withColumn("number_of_scr_customers", col("number_of_scr_customers").cast(IntegerType()))
    
trzd_claim = trzd_claim \
    .withColumn('financial_institution_name', regexp_replace(col('financial_institution_name'), r' \(conglomerado\)', ''))
    
bank_names = {
    'BB': 'BANCO DO BRASIL',
    'DAYCOVAL': 'BANCO DAYCOVAL S.A',
    'DEUTSCHE BANK S.A. - BANCO ALEMAO': 'DEUTSCHE',
    'BANCO SUMITOMO MITSUI BRASILEIRO S.A.': 'BANCO SUMITOMO MITSUI BRASIL S.A.'
}

trzd_claim = trzd_claim.na.replace(to_replace=bank_names, subset='financial_institution_name')

trzd_claim = trzd_claim \
    .withColumn('sk_financial_institution_name', unaccent(col('financial_institution_name'))) \
    .withColumn('sk_financial_institution_name', lower(col('sk_financial_institution_name'))) \
    .withColumn('sk_financial_institution_name', regexp_replace(col('sk_financial_institution_name'), r' ', '')) \
    .withColumn('sk_financial_institution_name', sha1(col('sk_financial_institution_name')))
    
trzd_claim = trzd_claim \
    .withColumn('quarter_claim', col('quarter_claim').cast(IntegerType())) \
    .withColumn('number_of_regulated_proceeding_complaints', col('number_of_regulated_proceeding_complaints').cast(IntegerType())) \
    .withColumn('number_of_regulated_other_complaints', col('number_of_regulated_other_complaints').cast(IntegerType())) \
    .withColumn('number_of_unregulated_complaints', col('number_of_unregulated_complaints').cast(IntegerType())) \
    .withColumn('total_number_of_complaints', col('total_number_of_complaints').cast(IntegerType()))
    
s3_path = f'{trusted_bucket}/{table_name}/'
save_parquet(trzd_claim, s3_path)
    
trzd_claim.show(truncate=False)

###############################################################################################
############################              DELIVERY                 ############################
###############################################################################################

table_name = 'dlzd_bank_employment_satisfaction'

banks = trzd_bank
employee = trzd_employee
claims = trzd_claim

banks = banks.drop('financial_institution_name')

banks_claims = claims.join(banks, on='cnpj', how='inner')

employee = employee.drop('financial_institution_name')

employee_claims = claims.join(employee, on='sk_financial_institution_name', how='inner')

employee_claims = employee_claims.withColumn('sk_cnpj_segment', sha1(concat(col('cnpj'), col('segment'))))

columns_to_add_employee_claims = list(set(employee_claims.columns) - set(banks_claims.columns))
columns_to_add_employee_claims.append('sk_cnpj_segment')
columns_to_drop = list(set(employee_claims.columns) - set(columns_to_add_employee_claims))

employee_claims = employee_claims.drop(*columns_to_drop)

dlzd_bank_employment_satisfaction = banks_claims.join(
    employee_claims, 
    on='sk_cnpj_segment', 
    how='inner'
)

dlzd_bank_employment_satisfaction = dlzd_bank_employment_satisfaction.drop_duplicates()

count_dlzd_bank_employment_satisfaction = dlzd_bank_employment_satisfaction.count()
print(f'count_dlzd_bank_employment_satisfaction: {count_dlzd_bank_employment_satisfaction}')

dlzd_bank_employment_satisfaction.show(truncate=False)


dlzd_bank_employment_satisfaction

# deixar comentado para não ficar sobrescrevendo em toda execução
# df.write.jdbc(url=url, 
#               table=table_name, 
#               mode="overwrite", 
#               properties=connection_properties)

df = spark.read.jdbc(
    url=jdbc_url,
    table=table_name,
    properties=jdbc_properties
)

df.show()