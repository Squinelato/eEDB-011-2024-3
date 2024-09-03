import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import json
from pyspark.sql.functions import lpad, col, lpad, concat, sha1, regexp_replace, udf, lower, lit, when
from pyspark.sql.types import StringType, FloatType, IntegerType

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def solve_args(args_list):
    return getResolvedOptions(sys.argv, args_list)

def get_secret_values(secretid):
    client = boto3.session.Session().client(service_name='secretsmanager')
    secret_string = client.get_secret_value(SecretId=secretid)['SecretString']
    return json.loads(secret_string)
    
def read_source_table(table_name):
    return spark.sql(f"""SELECT * FROM trzd.{table_name}""")
    
print('Delivery')

args_list = ['secretname','database','tablename']
args = solve_args(args_list)

secretname = args['secretname']
database = args['database']
table_name = args['tablename']

secret_dict = get_secret_values(secretname)

host = secret_dict['host']
port = secret_dict['port']
username = secret_dict['username']
password = secret_dict['password']
database = secret_dict['dbname']

jdbc_url = f'jdbc:{host}:{port}/{database}'
jdbc_properties = {
    'user': username,
    'password': password,
    'driver': 'com.mysql.cj.jdbc.Driver'
}

df_banks = read_source_table('banks')
df_employee = read_source_table('employees')
df_claims = read_source_table('claims')

df_banks = df_banks.drop('financial_institution_name')

df_banks_claims = df_claims.join(df_banks, on='cnpj', how='inner')

df_employee = df_employee.drop('financial_institution_name')

df_employee_claims = df_claims.join(df_employee, on='sk_financial_institution_name', how='inner')

df_employee_claims = df_employee_claims.withColumn('sk_cnpj_segment', sha1(concat(col('cnpj'), col('segment'))))

columns_to_add_employee_claims = list(set(df_employee_claims.columns) - set(df_banks_claims.columns))
columns_to_add_employee_claims.append('sk_cnpj_segment')
columns_to_drop = list(set(df_employee_claims.columns) - set(columns_to_add_employee_claims))

df_employee_claims = df_employee_claims.drop(*columns_to_drop)

df_bank_employment_satisfaction = df_banks_claims.join(df_employee_claims, on='sk_cnpj_segment', how='inner')

df_bank_employment_satisfaction = df_bank_employment_satisfaction.drop_duplicates()

count_df_bank_employment_satisfaction = df_bank_employment_satisfaction.count()
print(f'count_df_bank_employment_satisfaction: {count_df_bank_employment_satisfaction}')

df_bank_employment_satisfaction.show(truncate=False) # remover depois

df_bank_employment_satisfaction.write.jdbc(url=jdbc_url, 
                                          table=table_name, 
                                          mode="overwrite", 
                                          properties=jdbc_properties)
                                          
df = spark.read.jdbc(
    url=jdbc_url,
    table=table_name,
    properties=jdbc_properties
) # remover depois

df.show(truncate=False, n=20) # remover depois

job.commit()