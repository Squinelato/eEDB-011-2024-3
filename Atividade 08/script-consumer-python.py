import os
import json
import pandas as pd
import mysql.connector
from kafka import KafkaConsumer

csv_file_path = './enriched_messages/batch_output.csv'

if os.path.exists(csv_file_path):
    os.remove(csv_file_path)

with open('credentials.json', 'r') as f:
    credentials = json.load(f)

url = credentials['url']
username = credentials['user']
password = credentials['pass']
kafka_server = credentials['kafka_server']
topic = credentials['topic']

host = url.split("//")[1].split(":")[0]
database = url.split("/")[-1]

db_connection = mysql.connector.connect(
    host=host,
    port=3306,
    user=username,
    password=password,
    database=database
)

query = "SELECT cnpj, segment, financial_institution_name FROM eedb011.trusted_banks"
df_banks = pd.read_sql(query, db_connection)
print(df_banks.head())

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=kafka_server,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='consumer-group',
    value_deserializer=lambda x: x.decode('ISO-8859-1')
)

def process_message(message):
    values = message.split(';')
    #print(f'values: {values}')

    def decode_unicode(value):
        return value.encode('utf-8').decode('unicode_escape')
    
    data = {
        'Ano': decode_unicode(values[0]),
        'Trimestre': decode_unicode(values[1]),
        'Categoria': decode_unicode(values[2]),
        'Tipo': decode_unicode(values[3]),
        'CNPJ': decode_unicode(values[4].zfill(8)),
        'Instituicao': decode_unicode(values[5]),
        'Indice': decode_unicode(values[6]),
        'Reclamacoes_Procedentes': decode_unicode(values[7]),
        'Reclamacoes_Outras': decode_unicode(values[8]),
        'Reclamacoes_Nao_Reguladas': decode_unicode(values[9]),
        'Total_Reclamacoes': decode_unicode(values[10]),
        'Clientes_CCS_SCR': decode_unicode(values[11]),
        'Clientes_CCS': decode_unicode(values[12]),
        'Clientes_SCR': decode_unicode(values[13])
    }

    #print(f'data: {data}')
    
    return data

messages_list = []

for message in consumer:
    csv_value = message.value[1:-1]
    #print(f'csv_value: {csv_value}')
    data_dict = process_message(csv_value)
    messages_list.append(data_dict)
    
    if len(messages_list) >= 100:
        df_messages = pd.DataFrame(messages_list)
        
        df_enriched = pd.merge(df_messages, df_banks, left_on='CNPJ', right_on='cnpj', how='left')

        print(df_enriched.head())
        
        df_enriched.to_csv(csv_file_path, mode='a', index=False, header=False)
        
        messages_list.clear()

db_connection.close()