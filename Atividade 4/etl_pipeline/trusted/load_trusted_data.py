import os
import duckdb

def loading_banks_trusted(FILE_BANKS_RAW, PATH_OUTPUT_TRUSTED):
    # Conectar ao banco de dados DuckDB
    con = duckdb.connect()

    # Ler o arquivo Parquet e registrar como uma tabela temporária
    con.execute(f"CREATE TABLE banks_raw AS SELECT * FROM read_parquet('{FILE_BANKS_RAW}/*.parquet')")

    # Executar a consulta SQL na tabela temporária
    banks_temp = con.execute("""
        SELECT 
            Segmento AS segment, 
            CASE
                WHEN regexp_replace(Nome, ' \\(- PRUDENCIAL\\)', '') = '' THEN NULL
                ELSE regexp_replace(Nome, ' \\(- PRUDENCIAL\\)', '')
            END AS financial_institution_name,                 
            CASE
                WHEN cnpj = '' THEN NULL
                ELSE LPAD(cnpj, 8, '0')
            END AS cnpj,
            md5(concat(LPAD(cnpj, 8, '0'), Segmento)) AS sk_cnpj_segment
        FROM banks_raw
    """).df()

    # Exibir os dados resultantes
    print(banks_temp)

    # Mostrar a estrutura da tabela
    print(banks_temp.dtypes)

    # Salvar o resultado como arquivo Parquet
    con.execute(f"COPY banks_temp TO '{PATH_OUTPUT_TRUSTED}/banks.parquet' (FORMAT PARQUET)")

    # Fechar a conexão com o banco de dados
    con.close()


def loading_employee_trusted(FILE_EMPLOYEES_RAW, PATH_OUTPUT_TRUSTED):
    # Conectar ao banco de dados DuckDB
    con = duckdb.connect()

    # Ler o arquivo Parquet e registrar como uma tabela temporária
    con.execute(f"CREATE TABLE employees_raw AS SELECT * FROM read_parquet('{FILE_EMPLOYEES_RAW}/*.parquet')")

    # Realizar as transformações necessárias
    employees_temp = con.execute("""
        SELECT 
            "employer-website" AS employer_website,
            split_part("employer-headquarters", ',', 2) AS state_employer_headquarters,
            split_part("employer-headquarters", ',', 3) AS country_employer_headquarters,
            CAST("employer-founded" AS INT) AS employer_founded,
            "employer-industry" AS employer_industry,
            "employer-revenue" AS employer_revenue,
            CAST(Geral AS FLOAT) AS general_score,
            CAST("Cultura e valores" AS FLOAT) AS culture_values_score,
            CAST("Diversidade e inclusão" AS FLOAT) AS diversity_inclusion_score,
            CAST("Qualidade de vida" AS FLOAT) AS life_quality_score,
            CAST("Alta liderança" AS FLOAT) AS senior_leadership_score,
            CAST("Remuneração e benefícios" AS FLOAT) AS compensation_benefits_score,
            CAST("Oportunidades de carreira" AS FLOAT) AS career_opportunities_score,
            CAST("Recomendam para outras pessoas(%)" AS FLOAT) AS recommendation_score,
            CAST("Perspectiva positiva da empresa(%)" AS FLOAT) AS company_positive_score,
            Segmento AS segment,
            Nome AS financial_institution_name,
            CAST(reviews_count AS INT) AS reviews_count,
            CAST(culture_count AS INT) AS culture_count,
            CAST(salaries_count AS INT) AS salaries_count,
            CAST(benefits_count AS INT) AS benefits_count,
            CAST(match_percent AS FLOAT) AS match_percent,
            sk_financial_institution_name
        FROM employees_raw
    """).df()
    
    print(employees_temp)

    # Salvar os dados filtrados como arquivo Parquet
    con.execute(f"COPY employees_temp TO '{PATH_OUTPUT_TRUSTED}/employees.parquet' (FORMAT PARQUET)")

    # Fechar a conexão com o banco de dados
    con.close()


def loading_claims_trusted(FILE_CLAIMS_RAW, PATH_OUTPUT_TRUSTED):
    # Conectar ao banco de dados DuckDB
    con = duckdb.connect()

    # Ler o arquivo Parquet e registrar como uma tabela temporária
    con.execute(f"CREATE TABLE claims_raw AS SELECT * FROM read_parquet('{FILE_CLAIMS_RAW}/*.parquet')")

    # Padronizar as colunas e realizar as transformações
    claims_temp = con.execute("""
        SELECT
            "Ano" AS year_claim,
            regexp_replace("Trimestre", 'º', '')::INTEGER AS quarter_claim,
            "Categoria" AS category,
            "Tipo" AS bank_type,
            CASE
                WHEN "CNPJ IF" = '' THEN NULL
                ELSE LPAD("CNPJ IF", 8, '0')
            END AS cnpj,
            CASE
                WHEN regexp_replace("Instituição financeira", ' \\(conglomerado\\)', '') = '' THEN NULL
                ELSE regexp_replace("Instituição financeira", ' \\(conglomerado\\)', '')
            END AS financial_institution_name,
            regexp_replace(regexp_replace(regexp_replace("Índice", '\\.', ''), ',', '.'), ' ', '') AS bank_index,
            --"Quantidade de reclamações reguladas procedentes"  AS number_of_regulated_proceeding_complaints,
            --"Quantidade de reclamações reguladas - outras" AS number_of_regulated_other_complaints,
            --"Quantidade de reclamações não reguladas" AS number_of_unregulated_complaints,
            --"Quantidade total de reclamações" AS total_number_of_complaints,
            --regexp_replace(`Quantidade total de clientes – CCS e SCR", ' ', '')  AS total_number_of_ccs_and_scr_customers,
            --regexp_replace(`Quantidade de clientes – CCS", ' ', '') AS number_of_ccs_customers,
            --regexp_replace(`Quantidade de clientes – SCR", ' ', '')  AS number_of_scr_customers,
            sk_financial_institution_name
        FROM claims_raw
    """).df()

    print(claims_temp)

    # Salvar o resultado final como arquivo Parquet
    con.execute(f"COPY claims_temp TO '{PATH_OUTPUT_TRUSTED}/claims.parquet' (FORMAT PARQUET)")

    # Fechar a conexão com o banco de dados
    con.close()