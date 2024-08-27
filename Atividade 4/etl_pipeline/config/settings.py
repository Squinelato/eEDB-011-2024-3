import os
from pyspark.sql import SparkSession
from raw.load_raw_data import carregar_banco_raw  # Corrigi o nome da função importada

def run_etl():
    spark = SparkSession.builder.appName("RAW Ingestion").getOrCreate()
    #spark = get_spark_session()
    
    # Processar cada dataset
    carregar_banco_raw(spark)  # Corrigi o nome da função chamada e removi o parâmetro desnecessário

if __name__ == "__main__":
    run_etl()


# Caminhos para os arquivos .parquet e .csv
DIR_PATH_ORIGIN = 'C:/Users/Ana Priss/Documents/POS/3 - Ingestão de dados/Atividade 4/origin_data'
PATH_ORIGIN_BANKS= os.path.join(DIR_PATH_ORIGIN, 'Bancos')
PATH_ORIGIN_EMPLOYEES = os.path.join(DIR_PATH_ORIGIN, 'Empregados')
PATH_ORIGIN_CLAIMS = os.path.join(DIR_PATH_ORIGIN, 'Reclamacoes')


print('Path Banks:',PATH_ORIGIN_BANKS)
print('Path Employess:',PATH_ORIGIN_EMPLOYEES)
print('Path Claims:',PATH_ORIGIN_CLAIMS)




## Configurações adicionais como nomes de colunas padrão, etc.
#COLUMN_MAPPINGS = {
#    'banks': {'Segmento': 'segment', 'Nome': 'financial_institution_name', 'CNPJ': 'cnpj'},
#    'employees': {...},  # Adicionar mapeamentos das colunas para cada tabela
#    'claims': {...}
#}
