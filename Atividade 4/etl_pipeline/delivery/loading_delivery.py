import duckdb
import mysql.connector

def data_delivery(PATH_OUTPUT_TRUSTED):
    # Conectar ao banco de dados DuckDB
    con = duckdb.connect()

    # Ler os arquivos Parquet da camada Trusted e registrar como tabelas temporárias
    con.execute(f"CREATE TABLE banks AS SELECT * FROM read_parquet('{PATH_OUTPUT_TRUSTED}/banks.parquet')")
    con.execute(f"CREATE TABLE employees AS SELECT * FROM read_parquet('{PATH_OUTPUT_TRUSTED}/employees.parquet')")
    con.execute(f"CREATE TABLE claims AS SELECT * FROM read_parquet('{PATH_OUTPUT_TRUSTED}/claims.parquet')")

    # Fazer o JOIN entre as tabelas 'claims', 'banks' e 'employees'
    final_table = con.execute("""
        SELECT
            c.year_claim,
            c.quarter_claim,
            c.category,
            c.bank_type,
            c.cnpj,
            c.financial_institution_name,
            c.bank_index,
            c.sk_financial_institution_name,
            b.segment,
            e.employer_website,
            e.state_employer_headquarters,
            e.country_employer_headquarters,
            e.employer_founded,
            e.employer_industry,
            e.employer_revenue,
            e.general_score,
            e.culture_values_score,
            e.diversity_inclusion_score,
            e.life_quality_score,
            e.senior_leadership_score,
            e.compensation_benefits_score,
            e.career_opportunities_score,
            e.recommendation_score,
            e.company_positive_score,
            e.reviews_count,
            e.culture_count,
            e.salaries_count,
            e.benefits_count,
            e.match_percent
        FROM
            claims c
        INNER JOIN
            banks b ON c.cnpj = b.cnpj
        INNER JOIN
            employees e ON c.sk_financial_institution_name = e.sk_financial_institution_name
    """).df()

    # Exibir os dados resultantes
    print('Tabela Final:', final_table)

    import mysql
    # Conectar ao MySQL
    mysql_con = mysql.connector.connect(
        host="xx",
        user="xx",
        password="xx",
        database="xx"
    )
    cursor = mysql_con.cursor()

    # Criar a tabela no MySQL se ela não existir
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS delivery_deploy (
            year_claim INT,
            quarter_claim VARCHAR(255),
            category VARCHAR(255),
            bank_type VARCHAR(255),
            cnpj VARCHAR(255),
            financial_institution_name VARCHAR(255),
            bank_index INT,
            sk_financial_institution_name VARCHAR(255),
            segment VARCHAR(255),
            employer_website VARCHAR(255),
            state_employer_headquarters VARCHAR(255),
            country_employer_headquarters VARCHAR(255),
            employer_founded INT,
            employer_industry VARCHAR(255),
            employer_revenue VARCHAR(255),
            general_score DECIMAL(5,2),
            culture_values_score DECIMAL(5,2),
            diversity_inclusion_score DECIMAL(5,2),
            life_quality_score DECIMAL(5,2),
            senior_leadership_score DECIMAL(5,2),
            compensation_benefits_score DECIMAL(5,2),
            career_opportunities_score DECIMAL(5,2),
            recommendation_score DECIMAL(5,2),
            company_positive_score DECIMAL(5,2),
            reviews_count INT,
            culture_count INT,
            salaries_count INT,
            benefits_count INT,
            match_percent DECIMAL(5,2)
        );
    """)

    # Inserir os dados no MySQL
    for index, row in final_table.iterrows():
        cursor.execute("""
            INSERT INTO dlzd_bank_employment_satisfaction (
                year_claim, quarter_claim, category, bank_type, cnpj,
                financial_institution_name, bank_index, sk_financial_institution_name, 
                segment, employer_website, state_employer_headquarters, country_employer_headquarters,
                employer_founded, employer_industry, employer_revenue, general_score,
                culture_values_score, diversity_inclusion_score, life_quality_score,
                senior_leadership_score, compensation_benefits_score, career_opportunities_score,
                recommendation_score, company_positive_score, reviews_count, culture_count,
                salaries_count, benefits_count, match_percent
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, tuple(row))

    # Confirmar e fechar a conexão
    mysql_con.commit()
    cursor.close()
    mysql_con.close()

    print("Dados carregados no MySQL com sucesso!")