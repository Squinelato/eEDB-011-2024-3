import awswrangler as wr
import pandas as pd

def lambda_handler(event, context):
    try:
        print(event)
        
        messages = list()
        for record in event['Records']:
            body = record['body'].encode('latin1').decode('latin1').split(';')
            messages.append(body)
            
        print(messages)
        
        df = pd.DataFrame(messages, columns=["Ano", 
                                             "Trimestre", 
                                             "Categoria", 
                                             "Tipo", 
                                             "CNPJ IF", 
                                             "Instituição financeira", 
                                             "Índice", 
                                             "Quantidade de reclamações reguladas procedentes", 
                                             "Quantidade de reclamações reguladas - outras", 
                                             "Quantidade de reclamações não reguladas", 
                                             "Quantidade total de reclamações", 
                                             "Quantidade total de clientes - CCS e SCR", 
                                             "Quantidade de clientes - CCS", 
                                             "Quantidade de clientes - SCR", 
                                             "_14"])
        
        df.drop(columns=['_14'], inplace=True)
        df['CNPJ IF'] = df['CNPJ IF'].str.replace(" ", "")
        df["CNPJ IF"] = df["CNPJ IF"].map(lambda cnpj: cnpj.zfill(8) if len(cnpj) > 0 and len(cnpj) < 8 else cnpj)
        
        glue_database = "trzd"
        glue_table = "banks"
        df_glue = wr.athena.read_sql_table(table=glue_table, database=glue_database)

        df_merged = pd.merge(df, df_glue, left_on='CNPJ IF', right_on='cnpj', how='left')
        df_merged.drop(columns=['cnpj','sk_cnpj_segment','filename'], inplace=True)

        target_database = 'rwzd'
        target_table = 'enriched_claims'
        s3_output_path = f's3://eedb011-2024-g2-337171436930-raw/{target_database}/'

        wr.s3.to_parquet(
            df=df_merged,
            path=s3_output_path,
            database=target_database,
            table=target_table,
            dataset=True,
            mode='append',
            dtype={ column: 'string' for column in df_merged.columns },
            compression='snappy'
        )

        print('End')

    except Exception as e:
        print(f"Erro: {str(e)}")
