from raw.load_raw_data import loading_banks_raw, loading_employee_raw, loading_claims_raw
from trusted.load_trusted_data import loading_banks_trusted, loading_employee_trusted, loading_claims_trusted
from delivery.loading_delivery import data_delivery
from pyspark.sql import SparkSession
import os

# RAW Files
# Input
DIR_PATH_ORIGIN = '/home/anapriss/trabalho_spark/Atividade 4/origin_data'
FILE_BANKS_ORIGIN = os.path.join(DIR_PATH_ORIGIN, 'Bancos')
FILE_EMPLOYEES_ORIGIN = os.path.join(DIR_PATH_ORIGIN, 'Empregados')
FILE_CLAIMS_ORIGIN = os.path.join(DIR_PATH_ORIGIN, 'Reclamacoes')
### Output
PATH_OUTPUT_RAW = '/home/anapriss/trabalho_spark/Atividade 4/etl_pipeline/raw/data_out'


#TRUSTED Files
## Input
FILE_BANKS_RAW = '/home/anapriss/trabalho_spark/Atividade 4/etl_pipeline/raw/data_out/banks.parquet'
FILE_EMPLOYEES_RAW = '/home/anapriss/trabalho_spark/Atividade 4/etl_pipeline/raw/data_out/employees.parquet'
FILE_CLAIMS_RAW = '/home/anapriss/trabalho_spark/Atividade 4/etl_pipeline/raw/data_out/claims.parquet'
##Output
PATH_OUTPUT_TRUSTED = '/home/anapriss/trabalho_spark/Atividade 4/etl_pipeline/trusted/data_out'


# DELIVERY



def run_raw():
    spark = SparkSession.builder.appName("RAW Ingestion").getOrCreate()
    loading_banks_raw(spark, FILE_BANKS_ORIGIN,PATH_OUTPUT_RAW)  
    loading_employee_raw(spark, FILE_EMPLOYEES_ORIGIN,PATH_OUTPUT_RAW)  
    loading_claims_raw(spark, FILE_CLAIMS_ORIGIN, PATH_OUTPUT_RAW)

def run_trusted():
    loading_banks_trusted(FILE_BANKS_RAW,PATH_OUTPUT_TRUSTED)  
    loading_employee_trusted(FILE_EMPLOYEES_RAW,PATH_OUTPUT_TRUSTED)  
    loading_claims_trusted(FILE_CLAIMS_RAW, PATH_OUTPUT_TRUSTED)
#
def run_delivery():
    data_delivery(PATH_OUTPUT_TRUSTED)

if __name__ == "__main__":
    run_raw()
    run_trusted()
    run_delivery()
