import os
from pyspark.sql.functions import udf, lower, col, regexp_replace, sha1
from unidecode import unidecode
from pyspark.sql.types import StringType

def unicode_normalizer(word: str) -> str:
    return unidecode(word)

unaccent = udf(unicode_normalizer, StringType())

def loading_banks_raw(spark, FILE_BANKS_ORIGIN, PATH_OUTPUT_RAW):
    file_path = os.path.join(FILE_BANKS_ORIGIN, 'EnquadramentoInicia_v2.tsv')
    
    df_csv_banks = spark.read.csv(file_path, sep='\t', encoding='utf8', header=True)
    df_csv_banks.createOrReplaceTempView("banks_view")
    banks_view = spark.sql("SELECT * FROM banks_view")
    banks_view.show(truncate=False)
 
    spark.sql("DESCRIBE banks_view").show(truncate=False)
   
    total_records = spark.sql("SELECT COUNT(*) AS total FROM banks_view").collect()[0]['total']
    print(f"Total de registros: {total_records}")
    
    banks_view.write.parquet(f"{PATH_OUTPUT_RAW}/banks.parquet")

def loading_employee_raw(spark, FILE_EMPLOYEES_ORIGIN, PATH_OUTPUT_RAW):
    # Lendo os dois CSVs
    df1 = spark.read.csv(os.path.join(FILE_EMPLOYEES_ORIGIN, "glassdoor_consolidado_join_match_less_v2.csv"), sep='|', header=True, inferSchema=True)
    df2 = spark.read.csv(os.path.join(FILE_EMPLOYEES_ORIGIN, "glassdoor_consolidado_join_match_v2.csv"), sep='|', header=True, inferSchema=True)

    df_employees = df1.unionByName(df2, allowMissingColumns=True)

    df_employees.createOrReplaceTempView("employees_view")
    spark.sql("SELECT * FROM employees_view LIMIT 100").show()

    total_records_employee = spark.sql("SELECT COUNT(*) AS total FROM employees_view").collect()[0]['total']
    print(f"Total de registros: {total_records_employee}")

    # Remover duplicatas com base nas colunas 'Nome' e 'Segmento'
    spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW employees_view_dedup AS
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY Nome, Segmento ORDER BY Nome) AS row_num
        FROM employees_view
    ) tmp
    WHERE row_num = 1
    """)

    employees_view_dedup = spark.sql("SELECT * FROM employees_view_dedup") \
                                .withColumn('sk_financial_institution_name', 
                                    sha1(
                                        regexp_replace(
                                            lower(unaccent(col('Nome'))),  # Converte a coluna 'Nome' para minúsculas e remove acentos
                                            r'\s+', ''  # Remove todos os espaços em branco
                                        )
                                    )
                                )

    employees_view_dedup.show(truncate=False)

    employees_view_dedup.write.parquet(f"{PATH_OUTPUT_RAW}/employees.parquet")

def loading_claims_raw(spark, FILE_CLAIMS_ORIGIN, PATH_OUTPUT_RAW):
    claim_files = os.listdir(FILE_CLAIMS_ORIGIN)
    claim_paths = list(map(lambda file: os.path.join(FILE_CLAIMS_ORIGIN, file), claim_files))

    claim_df = spark.read.csv(claim_paths, sep=';', encoding='latin1', header=True)
    claim_df.createOrReplaceTempView("claims_view")
    claims_view = spark.sql("SELECT * FROM claims_view")
    claims_view.show(truncate=False)

    # Removendo coluna desnecessária
    spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW claims_view_filtered AS
        SELECT 
            Ano, Trimestre, Categoria, Tipo, `CNPJ IF`, `Instituição financeira`,
            `Índice`, `Quantidade de reclamações reguladas procedentes`, 
            `Quantidade de reclamações reguladas - outras`,
            `Quantidade de reclamações não reguladas`, 
            `Quantidade total de reclamações`, 
            "`Quantidade total de clientes – CCS e SCR`",
            "`Quantidade de clientes – CCS`", 
            "`Quantidade de clientes – SCR`"
        FROM claims_view
    """)

    claims_view_filtered = spark.sql("SELECT * FROM claims_view_filtered")

    spark.sql("DESCRIBE claims_view_filtered").show(truncate=False)

    # Contando a quantidade de linhas
    total_records_claim = spark.sql("SELECT COUNT(*) AS total FROM claims_view_filtered").collect()[0]['total']
    print(f"Registros claim: {total_records_claim}")

    # Converte a coluna 'Instituição financeira' para minúsculas, remove acentos e todos os espaços em branco
    claims_view_filtered = claims_view_filtered \
                                .withColumn('sk_financial_institution_name', 
                                    sha1(
                                        regexp_replace(
                                            lower(unaccent(col('Instituição financeira'))),  
                                            r'\s+', '' 
                                        )
                                    )
                                )

    claims_view_filtered.show(truncate=False)

    # Salvando como .parquet
    claims_view_filtered.write.parquet(f"{PATH_OUTPUT_RAW}/claims.parquet")

